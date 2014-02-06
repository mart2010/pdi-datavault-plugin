package plugin.mo.trans.steps.loadsat;


import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeSet;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import plugin.mo.trans.steps.backup.loadanchor.LoadAnchorData;
import plugin.mo.trans.steps.backup.loadanchor.LoadAnchorMeta;
import plugin.mo.trans.steps.common.CompositeValues;
import plugin.mo.trans.steps.loadlink.LoadLinkMeta;
import sun.security.action.GetLongAction;

public class LoadSatData extends BaseStepData implements StepDataInterface {
	private static Class<?> PKG = CompositeValues.class;
	
	public Database db;

	//this is simply cloned from inputRowMeta   
	public RowMetaInterface outputRowMeta;

	// hold the real schema name (after any var substitution)
	private String realSchemaName;
	// hold the name schema.table
	private String qualifiedSatTable;
	
	//position of surrogate FK in input row
	public int posFkInRow = -1;
	//position of FromDate attribute in input row
	public int posFromDateInRow = -1;

	//position of surrogate FK in fields mapping (same as satRowMeta)
	public int posFk = -1;
	//position of FromDate attribute in fields mapping (same as satRowMeta)
	public int posFromDate = -1;
	
	public Date toDateMaxFlag = null;
	
	//to limit historical sat rows query ResultSet 
	//is using number of millisecond since epoch...
	public long minDateBuffer = Long.MAX_VALUE;

	//used for both query lookup and insert into sat table
	//Values sorted as in the UI mapping entry
	private RowMetaInterface satRowMeta;
	//optionally used by prepStmtUpdateSat
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
	
	// TODO: all meta audit the sanme way (dedicate class "MetaValues" with systdate, source, etc.. and fk-batchId..
	// For now, we just use creation-date
	private Date nowDate;
	
	
	public LoadSatData(){
		super();
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
			bufferRows = new ArrayList<Object[]>(meta.getBufferSize()+10);
		}

		if (bufferSatHistRows == null) {
			bufferSatHistRows = new TreeSet<CompositeValues>();
		}
		
		//initialize all naming needing variable substitution (${var})
		realSchemaName = meta.getDatabaseMeta().environmentSubstitute(meta.getSchemaName());
		String realtable = meta.getDatabaseMeta().environmentSubstitute(meta.getTargetTable());
		qualifiedSatTable = meta.getDatabaseMeta().getQuotedSchemaTableCombination(realSchemaName, realtable);
		
		initSatAttsRowIdx(meta);
		minDateBuffer = Long.MAX_VALUE;
	}

	private void initSatAttsRowIdx(LoadSatMeta meta) throws KettleStepException {
		satAttsRowIdx = new int[meta.getFields().length];
		for (int i = 0; i < meta.getFields().length; i++) {
			satAttsRowIdx[i] = outputRowMeta.indexOfValue(meta.getFields()[i]);
			if (satAttsRowIdx[i] < 0) {
				// couldn't find field!
				throw new KettleStepException(BaseMessages.getString(PKG,
						"Load.Exception.FieldNotFound", meta.getFields()[i]));
			} 
			if (meta.getTypes()[i].equals(LoadSatMeta.ATTRIBUTE_FK)) {
				posFkInRow = outputRowMeta.indexOfValue(meta.getFields()[i]);
			}
			if (meta.getTypes()[i].equals(LoadSatMeta.ATTRIBUTE_TEMPORAL)) {
				posFromDateInRow = outputRowMeta.indexOfValue(meta.getFields()[i]);
			}
		}
	}

	
	public void emptyBuffersAndClearPrepStmts() {
		bufferRows.clear();
		bufferSatHistRows.clear();
	    try {
			prepStmtLookup.clearParameters();
		    prepStmtInsertSat.clearParameters();
			if (prepStmtUpdateSat != null){
				prepStmtUpdateSat.clearParameters();	
			}
		} catch (SQLException e) {
			new KettleException(e);
		}
	}

	public void initPrepStmtLookup(LoadSatMeta meta) throws KettleDatabaseException {

		DatabaseMeta dbMeta = meta.getDatabaseMeta();
		satRowMeta = new RowMeta();

		/* 
		 * Get historical rows for a set of surrKeys read in input rows.  To reduce data transfer, 
		 * only historical rows past a minimum time-point are considered: 
		 *         ---> rows previous to and following <minimum "FromDate"> read from input rows.
		 * This allows for non constant "FromDate" in input rows.
		 * 
		 * Notes:
		 * 1) The second clause is simply ignored for non-temporal data.
		 * 2) Rely on ANSI SQL DATE literal value using Gregorian calendar:  DATE 'YYYY-MM-DD'
		 * 3) All columns ([col1..]) ordering is based on the one defined in UI mapping  
		 * 
		 * SELECT [col1], [col2] .. FROM [sat_table] Sat WHERE [surrFK] IN ( ?, ?, ? ... )
		 * AND  [fromDate] >=  ( SELECT CASE WHEN max([fromDate]) IS NOT NULL THEN max([fromDate])
		 * 								     ELSE DATE '0001-01-01' END
		 * 						  FROM sat_table WHERE [surrFK] = Sat.[surrFK] AND [fromDate] < ? )
		 * 
		 * 
		 */
		String cols = "";
	
		for (int i = 0; i < meta.getCols().length; i++){
			cols += dbMeta.quoteField(meta.getCols()[i]);
			if (i < meta.getCols().length-1){
				cols += ", ";
			}
			if (meta.getTypes()[i].equals(LoadSatMeta.ATTRIBUTE_FK) ){
				posFk = i;
			}
			if (meta.getTypes()[i].equals(LoadSatMeta.ATTRIBUTE_TEMPORAL) ){
				posFromDate = i;
			}
			int mtype = outputRowMeta.getValueMeta(satAttsRowIdx[i]).getType();
			satRowMeta.addValueMeta(i, new ValueMeta(meta.getCols()[i], mtype));
		}

		String sql = "SELECT " + cols + " FROM " + qualifiedSatTable + " Sat "  + Const.CR;
		String whereIn = "WHERE " + dbMeta.quoteField(meta.getFkColumn()) + " IN ( ";
		String p = "";
		for (int j = 0; j < meta.getBufferSize(); j++) {
			if (j < meta.getBufferSize()-1){
				p += "?, ";	
			} else {
				p += "? ) ";
			}
		}
		sql = sql + whereIn + p;
		
		if (meta.getFromDateColumn() != null){
			String fromD = dbMeta.quoteField(meta.getFromDateColumn());
			String whereF =  Const.CR + " AND " + fromD + " >= " ;
			String whereS = " ( SELECT CASE WHEN max(" + fromD + ") IS NOT NULL THEN max(" + fromD + ") ELSE DATE '0001-01-01' END " + Const.CR +
					" FROM " + qualifiedSatTable + " WHERE " + meta.getFkColumn();
			whereS += " = Sat." + meta.getFkColumn() + " AND " + fromD + " < ? )";
			sql = sql + whereF + whereS ;
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

		/* 
		 * INSERT INTO <sat_table> (<col1>, <col2> ..) VALUES ( ?, ?, ? ... )
		 * (<col1..> ordering same as defined in UI mapping)
		 */
		String ins = "INSERT INTO " + qualifiedSatTable ;
		
		String cols = " ( ";
		String param = " ( ";
		for (int i = 0; i < meta.getCols().length; i++){
			if (i < meta.getCols().length-1){
				cols += dbMeta.quoteField(meta.getCols()[i]) + ", ";
				param += "?, " ;
			} else {
				cols += dbMeta.quoteField(meta.getCols()[i]);
				param += "?";
			}
		}
		
		if (meta.isToDateColumnUsed()){
			SimpleDateFormat format = new SimpleDateFormat(LoadSatMeta.DATE_FORMAT);
			try {
				toDateMaxFlag = format.parse(meta.getToDateMaxFlag());
			} catch (ParseException e) {
				throw new KettleDatabaseException("Date conversion error: " + meta.getToDateMaxFlag(),e);
			}
			cols += ", " + dbMeta.quoteField(meta.getToDateColumn());
			param += ", ?"; 
		}
		cols += " ) ";
		param += " ) ";
		
		String sqlIns = ins + cols + " VALUES " + param;
		
	    try {
	    	meta.getLog().logBasic("Prepared statement for insert:" + Const.CR + sqlIns);
	    	prepStmtInsertSat = db.getConnection().prepareStatement( dbMeta.stripCR( sqlIns ) );
	      } catch ( SQLException ex ) {
	        throw new KettleDatabaseException( ex );
	      }
		
	}
	

	
	public void initPrepStmtUpdate(LoadSatMeta meta) throws KettleDatabaseException {
	    
		DatabaseMeta dbMeta = meta.getDatabaseMeta();
		//do we need to Update Satellite?
		if (meta.isToDateColumnUsed()){
			updateToDateRowMeta = new RowMeta();
			updateToDateRowMeta.addValueMeta(satRowMeta.getValueMeta(posFromDate));
			updateToDateRowMeta.addValueMeta(satRowMeta.getValueMeta(posFk));
			updateToDateRowMeta.addValueMeta(satRowMeta.getValueMeta(posFromDate));
			
			/* 
			 * UPDATE <sat_table> SET <toDate-col> = ?
			 * WHERE <surKey-col> = ? AND (<fromDate-col> = ? 
			 */
			String u = "UPDATE " + qualifiedSatTable + " SET " + dbMeta.quoteField(meta.getToDateColumn()) + " = ? ";
			String w = " WHERE " + dbMeta.quoteField(meta.getFkColumn()) + " = ? " + " AND " + 
								   dbMeta.quoteField(meta.getFromDateColumn()) + " = ? ";
			String sqlUpd = u + w;
			
		    try {
		    	meta.getLog().logBasic("Prepared statement for update:" + Const.CR + sqlUpd);
		    	prepStmtUpdateSat = db.getConnection().prepareStatement( dbMeta.stripCR( sqlUpd ) );
		      } catch ( SQLException ex ) {
		        throw new KettleDatabaseException( ex );
		      }
		}
	}
	
	
	public boolean addToBufferRows(Object[] r, int bufferSize) {
		if (bufferRows.size() < bufferSize) {
			bufferRows.add(r);
			
			//update the minimum date 
			//TODO: use java.util.Date for now, NOT SURE IF OK in all cases?
			if (posFromDate != -1){
				java.util.Date d = (java.util.Date) r[satAttsRowIdx[posFromDate]];
				if (minDateBuffer > d.getTime()){
					minDateBuffer = d.getTime();	
				}
			}
			return (bufferRows.size() < bufferSize);
		} else {
			return false;
		}
	}
	
	public int populateLookupMap(LoadSatMeta meta, int nbParamsClause) throws KettleDatabaseException{
		// Setting values for prepared Statement
		for (int i = 0; i < nbParamsClause; i++) {
			Object[] r;
			try {
				r = bufferRows.get(i);
			} catch (IndexOutOfBoundsException e) {
				r = null;
			}
			Object key = (r != null) ? r[satAttsRowIdx[posFk]] : null;
			db.setValue(prepStmtLookup, satRowMeta.getValueMeta(posFk),key,i+1);
		}
		//final parameters (minDate) to limit historical sat rows
		if (posFromDate != -1){
			//TODO: check about usding Date: if column has finer-grained Timestamp def?
			java.util.Date minDate = new Date(minDateBuffer);
			db.setValue(prepStmtLookup, satRowMeta.getValueMeta(posFromDate)
					,minDate,nbParamsClause+1);
		}

		// go fetch data in DB and populate satHistRows buffer
		ResultSet rs;
		try {
			rs = prepStmtLookup.executeQuery();
		} catch (SQLException e) {
			throw new KettleDatabaseException("Unable to execute Satellite Lookup query", e);
		}

		for (Object[] r : getLookupRows(rs,meta)) {
			CompositeValues v = new CompositeValues(r,0,satRowMeta.size(),posFk,posFromDate);
			//flag records coming from DB
			v.setAsPersisted();
			// records from DB have integrity so no duplicates expected
			if (!bufferSatHistRows.add(v) ){
				meta.getLog().logError("Check DB state, satellite table has row duplicates: " + meta.getTargetTable() );
			}
		}
		return bufferSatHistRows.size();
	}
	
	
	private List<Object[]> getLookupRows(ResultSet rs, LoadSatMeta meta) throws KettleDatabaseException {
		List<Object[]> result = new ArrayList<Object[]>(meta.getBufferSize()*3);
		try {
			while (rs.next())  {
				Object[] row = new Object[satRowMeta.size()];
				for (int i = 0; i < satRowMeta.size(); i++) {
					ValueMetaInterface val = satRowMeta.getValueMeta(i);
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


	public RowMetaInterface getSatRowMeta() {
		return satRowMeta;
	}


	public int[] getSatAttsRowIdx() {
		return satAttsRowIdx;
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
	
	
	
}
