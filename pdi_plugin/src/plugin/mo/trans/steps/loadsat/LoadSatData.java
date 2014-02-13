/*
 * Copyright (c) 2014 Martin Ouellet
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *  http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package plugin.mo.trans.steps.loadsat;

import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import plugin.mo.trans.steps.common.CompositeValues;

public class LoadSatData extends BaseStepData implements StepDataInterface {
	private static Class<?> PKG = CompositeValues.class;

	public Database db;

	// this is simply cloned from inputRowMeta
	public RowMetaInterface outputRowMeta;

	// hold the real schema name (after any var substitution)
	private String realSchemaName;
	// hold the name schema.table
	private String qualifiedSatTable;

	// position of surrogate FK in input row
	public int posFkInRow = -1;
	// position of FromDate attribute in input row
	public int posFromDateInRow = -1;

	//position index of fields stored in BINARY in row stream (= null if none)
	private int[] fieldsInBinary = null;
	
	// position of surrogate FK in fields mapping (same as satRowMeta)
	public int posFk = -1;
	// position of FromDate attribute in fields mapping (same as satRowMeta)
	public int posFromDate = -1;

	public Date toDateMaxFlag = null;

	// to limit historical sat rows query ResultSet
	// is using number of millisecond since epoch...
	public long minDateBuffer = Long.MAX_VALUE;

	// Values sorted as in the UI mapping entry
	private RowMetaInterface lookupRowMeta;
	// Same values as lookup + additional one appended
	private RowMetaInterface insertRowMeta;
	// optionally used by prepStmtUpdateSat
	private RowMetaInterface updateToDateRowMeta;

	// prepare during firstRow process
	private PreparedStatement prepStmtLookup;
	private PreparedStatement prepStmtInsertSat;
	private PreparedStatement prepStmtUpdateSat;

	// Buffer storing original input rows
	private List<Object[]> bufferRows;

	// index of sat attribute fields position in row stream
	private int[] satAttsRowIdx;

	// hold buffer Sat rows as returned from prepStmtLookup query
	private NavigableSet<CompositeValues> bufferSatHistRows;

	public boolean finishedAllRows = false;

	// TODO: all meta audit the sanme way (dedicate class "MetaValues" with
	// systdate, source, etc.. and fk-batchId..
	// For now, we just use creation-date
	private Date nowDate;

	private LogChannelInterface log;

	public LoadSatData(LogChannelInterface log) {
		super();
		this.log = log;
		db = null;
	}

	/*
	 * Useful to get load DTS values fixed or refresh according to own needs
	 */
	public Date getNowDate(boolean refresh) {
		if (nowDate == null || refresh) {
			nowDate = new Date(System.currentTimeMillis());
		}
		return nowDate;
	}

	/*
	 * Must be called prior to Prepared Stmt initialization and row processing
	 */
	public void initializeRowProcessing(LoadSatMeta meta) throws KettleStepException {
		if (bufferRows == null) {
			bufferRows = new ArrayList<Object[]>(meta.getBufferSize() + 10);
		}

		if (bufferSatHistRows == null) {
			bufferSatHistRows = new TreeSet<CompositeValues>();
		}

		// initialize all naming needing variable substitution (${var})
		realSchemaName = meta.getDatabaseMeta().environmentSubstitute(meta.getSchemaName());
		String realtable = meta.getDatabaseMeta().environmentSubstitute(meta.getTargetTable());
		qualifiedSatTable = meta.getDatabaseMeta().getQuotedSchemaTableCombination(realSchemaName, realtable);

		initSatAttsRowIdx(meta);
		minDateBuffer = Long.MAX_VALUE;
	}

	private void initSatAttsRowIdx(LoadSatMeta meta) throws KettleStepException {
		satAttsRowIdx = new int[meta.getFields().length];
		int nbBinary = 0;
		for (int i = 0; i < meta.getFields().length; i++) {
			satAttsRowIdx[i] = outputRowMeta.indexOfValue(meta.getFields()[i]);
			if (satAttsRowIdx[i] < 0) {
				// couldn't find field!
				throw new KettleStepException(BaseMessages.getString(PKG, "Load.Exception.FieldNotFound",
						meta.getFields()[i]));
			}
			if (meta.getTypes()[i].equals(LoadSatMeta.ATTRIBUTE_FK)) {
				posFkInRow = outputRowMeta.indexOfValue(meta.getFields()[i]);
			} else if (meta.getTypes()[i].equals(LoadSatMeta.ATTRIBUTE_TEMPORAL)) {
				posFromDateInRow = outputRowMeta.indexOfValue(meta.getFields()[i]);
			}
			ValueMetaInterface val = outputRowMeta.getValueMeta(satAttsRowIdx[i]);
			if (val.isStorageBinaryString()){
				nbBinary++;
			}
		}
		
		if (nbBinary > 0){
			fieldsInBinary = new int[nbBinary];
			nbBinary = 0;
			for (int i = 0; i < meta.getFields().length; i++) {
				int ix = outputRowMeta.indexOfValue(meta.getFields()[i]);
				ValueMetaInterface val = outputRowMeta.getValueMeta(ix);
				if (val.isStorageBinaryString()){
					val.setStorageType(ValueMetaInterface.STORAGE_TYPE_NORMAL);
					fieldsInBinary[nbBinary] = ix;
					nbBinary++;
				}
			}
		}
		log.logBasic("saaaaaaaaaaa fieldsBinary =" + Arrays.toString(fieldsInBinary));
		
		
	}

	public void emptyBuffersAndClearPrepStmts() {
		bufferRows.clear();
		bufferSatHistRows.clear();
		//reset minDatefor the next Buffer
		minDateBuffer = Long.MAX_VALUE;
		try {
			prepStmtLookup.clearParameters();
			prepStmtInsertSat.clearParameters();
			if (prepStmtUpdateSat != null) {
				prepStmtUpdateSat.clearParameters();
			}
		} catch (SQLException e) {
			new KettleException(e);
		}
	}

	public void initPrepStmtLookup(LoadSatMeta meta) throws KettleDatabaseException {
		DatabaseMeta dbMeta = meta.getDatabaseMeta();
		lookupRowMeta = new RowMeta();

		/*
		 * Get historical rows for a set of techKeys read in input rows. To
		 * reduce data transfer, only historical rows past a minimum time-point
		 * are considered: ---> rows previous to and following <minimum
		 * "FromDate"> read from input rows. This allows for non constant
		 * "FromDate" in input rows.
		 * 
		 * Notes: 
		 * 1) The second clause is simply ignored for non-temporal data.
		 * 2) Rely on ANSI SQL DATE literal value using Gregorian calendar: DATE
		 * 'YYYY-MM-DD' 3) All columns ([col1..]) ordering is based on the one
		 * defined in UI mapping
		 * 
		 * SELECT [col1], [col2] .. 
		 * FROM [sat_table] Sat 
		 * WHERE [surrFK] IN ( ?,* ?, ? ... ) 
		 * AND [fromDate] >= ( SELECT CASE WHEN max([fromDate]) IS
		 * NOT NULL THEN max([fromDate]) ELSE DATE '0001-01-01' END 
		 * FROM sat_table WHERE [surrFK] = Sat.[surrFK] AND [fromDate] < ? )
		 */
		String cols = "";

		for (int i = 0; i < meta.getCols().length; i++) {
			cols += dbMeta.quoteField(meta.getCols()[i]);
			if (i < meta.getCols().length - 1) {
				cols += ", ";
			}
			if (meta.getTypes()[i].equals(LoadSatMeta.ATTRIBUTE_FK)) {
				posFk = i;
			}
			if (meta.getTypes()[i].equals(LoadSatMeta.ATTRIBUTE_TEMPORAL)) {
				posFromDate = i;
			}
			int mtype = outputRowMeta.getValueMeta(satAttsRowIdx[i]).getType();
			lookupRowMeta.addValueMeta(i, new ValueMeta(meta.getCols()[i], mtype));
		}

		String sql = "SELECT " + cols + " FROM " + qualifiedSatTable + " Sat " + Const.CR;
		String whereIn = "WHERE " + dbMeta.quoteField(meta.getFkColumn()) + " IN ( ";
		String p = "";
		for (int j = 0; j < meta.getBufferSize(); j++) {
			if (j < meta.getBufferSize() - 1) {
				p += "?, ";
			} else {
				p += "? ) ";
			}
		}
		sql = sql + whereIn + p;

		if (meta.getFromDateColumn() != null) {
			String fromD = dbMeta.quoteField(meta.getFromDateColumn());
			String whereF = Const.CR + " AND " + fromD + " >= ";
			String whereS = " ( SELECT CASE WHEN max(" + fromD + ") IS NOT NULL THEN max(" + fromD
					+ ") ELSE DATE '0001-01-01' END " + Const.CR + " FROM " + qualifiedSatTable + " WHERE "
					+ meta.getFkColumn();
			whereS += " = Sat." + meta.getFkColumn() + " AND " + fromD + " < ? )";
			sql = sql + whereF + whereS;
		}

		try {
			meta.getLog().logBasic("Prepared statement for Lookup:" + Const.CR + sql);
			prepStmtLookup = db.getConnection().prepareStatement(dbMeta.stripCR(sql));
		} catch (SQLException ex) {
			throw new KettleDatabaseException(ex);
		}
	}

	public void initPrepStmtInsert(LoadSatMeta meta) throws KettleDatabaseException {
		DatabaseMeta dbMeta = meta.getDatabaseMeta();
		insertRowMeta = new RowMeta();

		/*
		 * INSERT INTO <sat_table> (<col1>, <col2> ..) VALUES ( ?, ?, ? ... )
		 * (<col1..> ordering same as defined in UI mapping)
		 */
		String ins = "INSERT INTO " + qualifiedSatTable;
		String cols = " ( ";
		String param = " ( ";

		// ***********************************************
		// 1- Handle sat columns defined in Mapping UI
		// ***********************************************
		for (int i = 0; i < meta.getCols().length; i++) {
			if (i < meta.getCols().length - 1) {
				cols += dbMeta.quoteField(meta.getCols()[i]) + ", ";
				param += "?, ";
			} else {
				cols += dbMeta.quoteField(meta.getCols()[i]);
				param += "?";
			}
			int t = outputRowMeta.getValueMeta(satAttsRowIdx[i]).getType();
			insertRowMeta.addValueMeta(new ValueMeta(meta.getCols()[i], t));
		}

		// ***********************************************
		// 2- Handle optional toDate
		// ***********************************************
		if (meta.isToDateColumnUsed()) {
			SimpleDateFormat format = new SimpleDateFormat(LoadSatMeta.DATE_FORMAT);
			try {
				toDateMaxFlag = format.parse(meta.getToDateMaxFlag());
			} catch (ParseException e) {
				throw new KettleDatabaseException("Date conversion error: " + meta.getToDateMaxFlag(), e);
			}
			cols += ", " + dbMeta.quoteField(meta.getToDateColumn());
			param += ", ?";
			int t = outputRowMeta.getValueMeta(posFromDateInRow).getType();
			insertRowMeta.addValueMeta(new ValueMeta(meta.getToDateColumn(), t));
		}

		// ***********************************************
		// 3- Handle audit columns
		// ***********************************************
		if (!Const.isEmpty(meta.getAuditDtsCol())) {
			cols += ", " + db.getDatabaseMeta().quoteField(meta.getAuditDtsCol());
			param += ", ?";
			insertRowMeta.addValueMeta(new ValueMetaDate(meta.getAuditDtsCol()));

		}
		if (!Const.isEmpty(meta.getAuditRecSourceCol())) {
			cols += ", " + db.getDatabaseMeta().quoteField(meta.getAuditRecSourceCol());
			param += ", ?";
			insertRowMeta.addValueMeta(new ValueMetaString(meta.getAuditRecSourceCol()));
		}

		cols += " ) ";
		param += " ) ";

		String sqlIns = ins + cols + " VALUES " + param;

		try {
			meta.getLog().logBasic("Prepared stmt for Satellite insert:" + Const.CR + sqlIns);
			prepStmtInsertSat = db.getConnection().prepareStatement(dbMeta.stripCR(sqlIns));
		} catch (SQLException ex) {
			throw new KettleDatabaseException(ex);
		}
	}

	public void initPrepStmtUpdate(LoadSatMeta meta) throws KettleDatabaseException {
		DatabaseMeta dbMeta = meta.getDatabaseMeta();
		// do we need to Update Satellite?
		if (meta.isToDateColumnUsed()) {
			updateToDateRowMeta = new RowMeta();
			updateToDateRowMeta.addValueMeta(lookupRowMeta.getValueMeta(posFromDate));
			updateToDateRowMeta.addValueMeta(lookupRowMeta.getValueMeta(posFk));
			updateToDateRowMeta.addValueMeta(lookupRowMeta.getValueMeta(posFromDate));

			/*
			 * UPDATE <sat_table> SET <toDate-col> = ? WHERE <surKey-col> = ?
			 * AND (<fromDate-col> = ?
			 */
			String u = "UPDATE " + qualifiedSatTable + " SET " + dbMeta.quoteField(meta.getToDateColumn()) + " = ? ";
			String w = " WHERE " + dbMeta.quoteField(meta.getFkColumn()) + " = ? " + " AND "
					+ dbMeta.quoteField(meta.getFromDateColumn()) + " = ? ";
			String sqlUpd = u + w;

			try {
				meta.getLog().logBasic("Prepared statement for update:" + Const.CR + sqlUpd);
				prepStmtUpdateSat = db.getConnection().prepareStatement(dbMeta.stripCR(sqlUpd));
			} catch (SQLException ex) {
				throw new KettleDatabaseException(ex);
			}
		}
	}

	public boolean addToBufferRows(Object[] r, int bufferSize) {
		if (bufferRows.size() < bufferSize) {
			bufferRows.add(r);

			// update the minimum date
			// TODO: use java.util.Date for now, NOT SURE IF OK in all cases?
			if (posFromDate != -1) {
				java.util.Date d = (java.util.Date) r[satAttsRowIdx[posFromDate]];
				if (minDateBuffer > d.getTime()) {
					minDateBuffer = d.getTime();
				}
			}
			return (bufferRows.size() < bufferSize);
		} else {
			return false;
		}
	}

	public int populateLookupMap(LoadSatMeta meta, int nbParamsClause) throws KettleDatabaseException {
		// Setting values for prepared Statement
		for (int i = 0; i < nbParamsClause; i++) {
			Object[] r;
			try {
				r = bufferRows.get(i);
			} catch (IndexOutOfBoundsException e) {
				r = null;
			}
			Object key = (r != null) ? r[satAttsRowIdx[posFk]] : null;
			db.setValue(prepStmtLookup, lookupRowMeta.getValueMeta(posFk), key, i + 1);
		}
		// final parameters (minDate) to limit historical sat rows
		if (posFromDate != -1) {
			// TODO: check about using Date: if column has finer-grained
			// Timestamp def?
			java.util.Date minDate = new Date(minDateBuffer);
			db.setValue(prepStmtLookup, lookupRowMeta.getValueMeta(posFromDate), minDate, nbParamsClause + 1);
		}

		// go fetch data in DB and populate satHistRows buffer
		ResultSet rs;
		try {
			rs = prepStmtLookup.executeQuery();
		} catch (SQLException e) {
			throw new KettleDatabaseException("Unable to execute Satellite Lookup query", e);
		}

		for (Object[] r : getLookupRows(rs, meta)) {
			CompositeValues v = new CompositeValues(r, 0, lookupRowMeta.size(), posFk, posFromDate);
			// flag records coming from DB
			v.setAsPersisted();
			// records from DB have integrity so no duplicates expected
			if (!bufferSatHistRows.add(v)) {
				meta.getLog().logError("Check DB state, satellite table has row duplicates: " + meta.getTargetTable());
			}
		}
		return bufferSatHistRows.size();
	}

	private List<Object[]> getLookupRows(ResultSet rs, LoadSatMeta meta) throws KettleDatabaseException {
		List<Object[]> result = new ArrayList<Object[]>(meta.getBufferSize() * 3);
		try {
			while (rs.next()) {
				Object[] row = new Object[lookupRowMeta.size()];
				for (int i = 0; i < lookupRowMeta.size(); i++) {
					ValueMetaInterface val = lookupRowMeta.getValueMeta(i);
					row[i] = db.getDatabaseMeta().getValueFromResultSet(rs, val, i);
				}
				result.add(row);
			}
			db.closeQuery(rs);
			return result;
		} catch (Exception e) {
			throw new KettleDatabaseException("Unable to get list of satellite rows from ResultSet : ", e);
		}
	}

	public void addBatchInsert(LoadSatMeta meta, Object[] satRow, Object optionalToDate) throws KettleDatabaseException {

		// ***********************************************
		// 1- Handle sat columns defined in Mapping UI
		// ***********************************************
		for (int i = 0; i < meta.getCols().length; i++) {
			db.setValue(prepStmtInsertSat, insertRowMeta.getValueMeta(i), satRow[i], i + 1);
		}

		int idx;
		// ***********************************************
		// 2- Handle optional toDate
		// ***********************************************
		if (meta.isToDateColumnUsed()) {
			idx = insertRowMeta.indexOfValue(meta.getToDateColumn());
			db.setValue(prepStmtInsertSat, insertRowMeta.getValueMeta(idx), optionalToDate, idx + 1);
		}

		// ***********************************************
		// 3- Handle audit columns
		// ***********************************************
		if (!Const.isEmpty(meta.getAuditDtsCol())) {
			idx = insertRowMeta.indexOfValue(meta.getAuditDtsCol());
			db.setValue(prepStmtInsertSat, insertRowMeta.getValueMeta(idx), getNowDate(false), idx + 1);
		}

		if (!Const.isEmpty(meta.getAuditRecSourceCol())) {
			idx = insertRowMeta.indexOfValue(meta.getAuditRecSourceCol());
			db.setValue(prepStmtInsertSat, insertRowMeta.getValueMeta(idx), meta.getAuditRecSourceValue(), idx + 1);
		}

		try {
			prepStmtInsertSat.addBatch();
			if (log.isRowLevel()) {
				log.logRowlevel("Adding batch values: " + Arrays.deepToString(satRow));
			}
		} catch (SQLException ex) {
			throw new KettleDatabaseException("Error adding batch to Load Sat, rowMeta: "
					+ insertRowMeta.toStringMeta(), ex);
		}
	}

	public void addBatchUpdateStmt(Object[] values) throws KettleDatabaseException {
		try {
			db.setValues(updateToDateRowMeta, values, prepStmtUpdateSat);
			prepStmtUpdateSat.addBatch();
			// logError("inserting int batch: " + Arrays.deepToString(values));
		} catch (SQLException ex) {
			throw new KettleDatabaseException("Error adding to Update Sat, for rowMeta: "
					+ updateToDateRowMeta.toStringMeta(), ex);
		}
	}

	public void executeBatch(PreparedStatement stmt, RowMetaInterface rowMeta, int updateExpected)
			throws KettleDatabaseException {
		int[] nb = null;
		try {
			nb = stmt.executeBatch();
			stmt.clearBatch();
			if (log.isDebug()) {
				log.logDebug("Successfully executed insert batch");
			}
		} catch (BatchUpdateException ex) {
			if (nb == null) {
				nb = ex.getUpdateCounts();
			}

			if (updateExpected == nb.length) {
				// jdbc driver continued processing all rows
				log.logError("Batch Exception but JDBC driver processed all rows");
				if (!Const.isEmpty(db.getConnectionGroup())) {
					// logError("Ignore when running in parrallel -unique constraint violation expected");
					throw new KettleDatabaseException(ex);
					// return;
				} else {
					// logError("Continue processing, but check database integrity");
					throw new KettleDatabaseException(ex);
					// return;
				}
			} else {
				log.logError("Batch Exception and not all rows prcessed", ex);
				throw new KettleDatabaseException(ex);
			}
		} catch (SQLException ex) {
			throw new KettleDatabaseException("Unexpected error with batch", ex);
		}
	}

	public String getRealSchemaName() {
		return realSchemaName;
	}

	public String getQualifiedSatTable() {
		return qualifiedSatTable;
	}

	public void setQualifiedSatTable(String qualifiedSatTable) {
		this.qualifiedSatTable = qualifiedSatTable;
	}

	public List<Object[]> getBufferRows() {
		return bufferRows;
	}

	public PreparedStatement getPrepStmtLookup() {
		return prepStmtLookup;
	}

	public RowMetaInterface getLookupRowMeta() {
		return lookupRowMeta;
	}

	public int[] getSatAttsRowIdx() {
		return satAttsRowIdx;
	}

	public int[] getFieldsInBinary() {
		return fieldsInBinary;
	}

	public NavigableSet<CompositeValues> getBufferSatHistRows() {
		return bufferSatHistRows;
	}

	public PreparedStatement getPrepStmtInsertSat() {
		return prepStmtInsertSat;
	}

	public PreparedStatement getPrepStmtUpdateSat() {
		return prepStmtUpdateSat;
	}

	public RowMetaInterface getUpdateToDateRowMeta() {
		return updateToDateRowMeta;
	}

	public RowMetaInterface getInsertRowMeta() {
		return insertRowMeta;
	}

}
