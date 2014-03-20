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
package plugin.mo.trans.steps.backup;


import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import plugin.mo.trans.steps.common.CompositeKeys;

/**
 * 
 * 
 */
public class LoadAnchorData extends BaseStepData implements StepDataInterface {
	private static Class<?> PKG = LoadAnchor.class;
	
	// modified downstream rowMeta following "LoadHub" step
	public RowMetaInterface outputRowMeta;
	public Database db;

	// hold the real schema name (after potential var substitution)
	private String realSchemaName;
	// hold the name schema.table
	private String qualifiedHubTable;
	// left to null if not used
	private String qualifiedNatKeyTable;

	
	// index of natkey fields position in row stream
	private int[] natkeysRowIdx;
	// leave or remove nat-keys field from input stream
	private boolean[] removeField;

	// hold the lookup meta of nat-keys
	private RowMetaInterface lookupRowMeta;

	private RowMetaInterface insertHubRowMeta;
	private RowMetaInterface insertNatKeyRowMeta;

	// Buffer storing original input rows appended with new surrKey
	private List<Object[]> bufferRows;
	// hold the lookup record (n-key(s) + surrKey) returned from DB
	private Map<CompositeKeys, Long> bufferLookupMapping;

	public boolean finishedAllRows = false;

	// prepared only once during firstRow process
	private PreparedStatement prepStmtLookup;
	private PreparedStatement prepStmtInsertHub;
	// null if nat-keys stored inside hubTable
	private PreparedStatement prepStmtInsertNatKey;
	// Audit info, see how to leverage full auditing.. now simply use
	// creation-date
	private Date nowDate;

	// Useful to get consistent CreationDate throughout the lifetime of this
	// data Object, i.e. the complete Batch processing time
	public Date getNowDate() {
		if (nowDate == null) {
			nowDate = new Date(System.currentTimeMillis());
		}
		return nowDate;
	}

	// only needed when CREATION_METHOD_TABLEMAX is used
	private long curValueSurrKey;

	public LoadAnchorData() {
		super();
		db = null;

	}

	public List<Object[]> getBufferRows() {
		return bufferRows;
	}

	public Map<CompositeKeys, Long> getBufferLookupMapping() {
		return bufferLookupMapping;
	}

	/*
	 * When not full: Add row to Buffer and return BufferNotFull status after
	 * row insertion. When full: return false
	 */
	public boolean addToBufferRows(Object[] r, int bufferSize) {

		if (getBufferRows().size() < bufferSize) {
			bufferRows.add(r);
			return (getBufferRows().size() < bufferSize);
		} else {
			return false;
		}
	}

	

	/*
	 * Must be called prior to start any row processing and also after once
	 * the buffer is full
	 */
	public void initializeBuffers(int bsize) {
			
		if (this.bufferRows == null) {
			this.bufferRows = new ArrayList<Object[]>(bsize+10);
		}

		if (this.bufferLookupMapping == null) {
			int capacity = (int) ((bsize)/0.75+1);
			this.bufferLookupMapping = new HashMap<CompositeKeys, Long>(capacity);
		}

		this.bufferRows.clear();
		this.bufferLookupMapping.clear();
	}

	public String getRealSchemaName() {
		return realSchemaName;
	}

	public void setRealSchemaName(DatabaseMeta dbMeta, String uiEntrySchemaName) {
		this.realSchemaName = dbMeta.environmentSubstitute(uiEntrySchemaName);
		;
	}

	public String getQualifiedHubTable() {
		return qualifiedHubTable;
	}

	/*
	 * Must be called after setting schema: RealSchemaName
	 */
	public void setQualifiedHubTable(DatabaseMeta dbMeta, String uiEntryHubTable) {
		// replace potential ${var} by their subs env. values
		String realHubTable = dbMeta.environmentSubstitute(uiEntryHubTable);
		qualifiedHubTable = dbMeta.getQuotedSchemaTableCombination(realSchemaName, realHubTable);

	}

	public String getQualifiedNatKeyTable() {
		return qualifiedNatKeyTable;
	}

	/*
	 * Leave QualifiedNatKeyTable to Null when no proper ui entry is set
	 */
	public void setQualifiedNatKeyTable(DatabaseMeta dbMeta, String uiEntryNatKeyTable) {
		if (uiEntryNatKeyTable == null || uiEntryNatKeyTable.length() < 1) {
			qualifiedNatKeyTable = null;
		}
		// replace potential ${var} by their subs env. values
		String realNatKeyTable = dbMeta.environmentSubstitute(uiEntryNatKeyTable);
		qualifiedNatKeyTable = dbMeta.getQuotedSchemaTableCombination(realSchemaName, realNatKeyTable);
	}

	/*
	 * Assumption: all nat-key(s) column are NOT NULLABLE, reasonable for
	 * valid natural/business keys
	 */
	public void initPrepStmtLookup(LoadAnchorMeta meta, RowMetaInterface inputRowMeta)
			throws KettleDatabaseException {

		DatabaseMeta dbMeta = meta.getDatabaseMeta();
		this.lookupRowMeta = new RowMeta();

		/*
		 * SELECT <surrkey>, <natkey1> .. FROM <table> WHERE ( ( <natkey1> = ? )
		 * AND ( ..) OR ( ( <natkey1> = ? ) AND ( .. ) OR ..
		 * m-times-->bufferSize
		 */
		String targetTable = (qualifiedNatKeyTable != null) ? qualifiedNatKeyTable : qualifiedHubTable;
		String targetSurrkey = (qualifiedNatKeyTable != null) ? dbMeta.quoteField(meta.getSurrFKeyInNatkeyTable())
				: dbMeta.quoteField(meta.getSurrPKeyColumn());

		String sql = " SELECT " + targetSurrkey;
		String nkcols = "";
		String endClause = " WHERE ( (";

		for (int j = 0; j < meta.getBufferSize(); j++) {

			boolean comma = false;
			for (int i = 0; i < meta.getNatKeyCol().length; i++) {
				if (comma) {
					endClause += " AND ( ";
				} else {
					comma = true;
				}
				endClause += dbMeta.quoteField(meta.getNatKeyCol()[i]) + " = ? ) ";
				if (i == meta.getNatKeyCol().length - 1) {
					endClause += " ) ";
				}

				// add Meta of natkey(s) col name with corresponding input
				// stream type
				if (j == 0) {
					nkcols += ", " + dbMeta.quoteField(meta.getNatKeyCol()[i]);
					int tmpMetatype = inputRowMeta.getValueMeta(natkeysRowIdx[i]).getType();
					lookupRowMeta.addValueMeta(i, new ValueMeta(meta.getNatKeyCol()[i], tmpMetatype));
				}

			}
			if (j < meta.getBufferSize() - 1) {
				endClause += " OR " + Const.CR + " ( (";
			}

		}

		sql += nkcols;
		sql += " FROM " + targetTable;
		sql += endClause;

		try {
			meta.getLog().logBasic("Prepared statement for Hub natkey Lookup:" + Const.CR + sql);
			prepStmtLookup = db.getConnection().prepareStatement(dbMeta.stripCR(sql));
			if (dbMeta.supportsSetMaxRows()) {
				// lookup cannot return more than BufferSize
				prepStmtLookup.setMaxRows(meta.getBufferSize());
			}
		} catch (SQLException ex) {
			throw new KettleDatabaseException(ex);
		}

	}

	public void clearPrepStatementLookup() {
		if (this.prepStmtLookup != null) {
			try {
				this.prepStmtLookup.clearParameters();
			} catch (SQLException e) {
				new KettleException(e);
			}
		}

	}

	public PreparedStatement getPrepStmtLookup() {
		return prepStmtLookup;
	}

	public int[] getNatkeysRowIdx() {
		return natkeysRowIdx;
	}

	public void initNatkeysRowIdx(LoadAnchorMeta meta, RowMetaInterface inputRowMeta) throws KettleStepException {

		this.natkeysRowIdx = new int[meta.getNatKeyField().length];
		for (int i = 0; i < meta.getNatKeyField().length; i++) {
			this.natkeysRowIdx[i] = inputRowMeta.indexOfValue(meta.getNatKeyField()[i]);
			if (this.natkeysRowIdx[i] < 0) {
				// couldn't find field!
				throw new KettleStepException(BaseMessages.getString(PKG,
						"Load.Exception.FieldNotFound", meta.getNatKeyField()[i]));
			}
		}
	}

	public boolean[] getRemoveField() {
		return removeField;
	}

	/*
	 * Boolean Index used to help remove nat-key field(s) from input stream
	 */
	public void initRemoveFieldIndex(LoadAnchorMeta meta, RowMetaInterface inputRowMeta) {
		removeField = new boolean[inputRowMeta.size()];

		for (int i = 0; i < inputRowMeta.size(); i++) {
			ValueMetaInterface valueMeta = inputRowMeta.getValueMeta(i);
			// Is this one of the keys?
			int idx = Const.indexOfString(valueMeta.getName(), meta.getNatKeyField());
			removeField[i] = idx >= 0;
		}
	}

	/*
	 * Initialize Stmt for inserting into Hub and optionally into Natkey Table
	 * and initialize the corresponding RowMeta
	 */
	public void initPrepStmtInsert(LoadAnchorMeta meta, RowMetaInterface inputRowMeta)
			throws KettleDatabaseException {

		/*
		 * Construct the SQL statement... INSERT INTO d_test(keyfield, natkeys)
		 * VALUES(val_key, row values with keynrs[]) ;
		 */

		String sqlInsertHub = "INSERT INTO " + getQualifiedHubTable() + ("( ");
		String sqlInsertHubValues = ") VALUES (";
		String sqlInsertNatkey = null;
		String sqlInsertNatkeyValues = null;

		insertHubRowMeta = new RowMeta();
		insertNatKeyRowMeta = null;

		boolean comma = false;

		/******************** Start with fix columns in HubTable *****************/
		// NO AUTOINCREMENT
		if (!meta.isAutoIncrement()) {
			sqlInsertHub += db.getDatabaseMeta().quoteField(meta.getSurrPKeyColumn());
			insertHubRowMeta.addValueMeta(new ValueMetaInteger(meta.getSurrPKeyColumn()));
			sqlInsertHubValues += "?";
			comma = true;
		} else if (db.getDatabaseMeta().needsPlaceHolder()) {
			// Informix requires a dummy placeholder with AUTOINCREMENT
			sqlInsertHub += "0";
			//Not sure I need to put the meta here, as no values param will be set in the preparedStmt
			insertHubRowMeta.addValueMeta(new ValueMetaInteger(meta.getSurrPKeyColumn()));
			comma = true;
		}

		// Assume always have a CreationDate (to avoid empty INSERT statement
		// for Hub with separate Natkey...)
		// eventually we can either have Audit-FK col (AM) or Audit-cols
		// (creationDate & source)
		if (true) { // !Const.isEmpty( meta.getCreationDateCol() ) ) {
			if (comma) {
				sqlInsertHub += ", ";
				sqlInsertHubValues += ", ";
			}
			sqlInsertHub += db.getDatabaseMeta().quoteField(meta.getCreationDateCol());
			sqlInsertHubValues += "?";
			insertHubRowMeta.addValueMeta(new ValueMetaDate(meta.getCreationDateCol()));
			comma = true;
		}

		/******************** If applicable, continue with natkey fix columns in HubTable *****************/
		if (getQualifiedNatKeyTable() != null) {
			sqlInsertNatkey = "INSERT INTO " + getQualifiedNatKeyTable() + ("( ");
			sqlInsertNatkey += db.getDatabaseMeta().quoteField(meta.getSurrFKeyInNatkeyTable());
			sqlInsertNatkey += ", ";
			sqlInsertNatkeyValues = ") VALUES (?";
			// same assumption: always have CreationDate
			sqlInsertNatkey += db.getDatabaseMeta().quoteField(meta.getCreationDateCol());
			sqlInsertNatkeyValues += ", ?";
			insertNatKeyRowMeta = new RowMeta();
			//this ValueMetaDate is sort of hard-coding... can I do better?
			insertNatKeyRowMeta.addValueMeta(new ValueMetaInteger(meta.getSurrFKeyInNatkeyTable()));
			insertNatKeyRowMeta.addValueMeta(new ValueMetaDate(meta.getCreationDateCol()));

		}

		 //meta.getLog().logError("Here's my meta.batchsize: " +  meta.getBufferSize());
		 
		/******* Continue with natkey Columns in either HubTable or NatKeyColTable *******/
		for (int i = 0; i < meta.getNatKeyCol().length; i++) {
			ValueMetaInterface tmpValueMeta = inputRowMeta.getValueMeta(natkeysRowIdx[i]).clone();
			tmpValueMeta.setName(meta.getNatKeyCol()[i]);
			
			meta.getLog().logError("My meta is : " + tmpValueMeta.toStringMeta());

			// Only use a Hub
			if (getQualifiedNatKeyTable() == null) {
				sqlInsertHub += ", ";
				sqlInsertHub += db.getDatabaseMeta().quoteField(meta.getNatKeyCol()[i]);
				sqlInsertHubValues += ", ?";
				insertHubRowMeta.addValueMeta(tmpValueMeta);
			}
			// Use separate Natkey table
			else {
				sqlInsertNatkey += ", ";
				sqlInsertNatkey += db.getDatabaseMeta().quoteField(meta.getNatKeyCol()[i]);
				sqlInsertNatkeyValues += ", ?";
				insertNatKeyRowMeta.addValueMeta(tmpValueMeta);
			}
		}

		// append the "VALUES (..)" suffix
		sqlInsertHub = sqlInsertHub + sqlInsertHubValues + ") ";
		if (getQualifiedNatKeyTable() != null) {
			sqlInsertNatkey = sqlInsertNatkey + sqlInsertNatkeyValues + ") ";

		}

		meta.getLog().logError("My insertMeta is : " + insertNatKeyRowMeta.toStringMeta());

		// JDBC Prepare Statement
		String tmpS = "";
		try {
			tmpS = sqlInsertHub;
			if (meta.isAutoIncrement()) {
				meta.getLog().logBasic("Prepared statement for Hub insert with return key:" + Const.CR + tmpS);
				prepStmtInsertHub = db.getConnection().prepareStatement(db.getDatabaseMeta().stripCR(tmpS),
						Statement.RETURN_GENERATED_KEYS);
			} else {
				meta.getLog().logBasic("Prepared statement for Hub insert without return key:" + Const.CR + tmpS);
				prepStmtInsertHub = db.getConnection().prepareStatement(db.getDatabaseMeta().stripCR(tmpS));

				if (getQualifiedNatKeyTable() != null) {
					tmpS = sqlInsertNatkey;
					meta.getLog().logBasic("Prepared statement for ext Natkey Table insert:" + Const.CR + tmpS);
					prepStmtInsertNatKey = db.getConnection().prepareStatement(
							db.getDatabaseMeta().stripCR(tmpS));
				}
			}
		} catch (SQLException ex) {
			throw new KettleDatabaseException(ex);
		} catch (Exception ex) {
			throw new KettleDatabaseException(ex);
		}
	}

	public PreparedStatement getPrepStmtInsertHub() {
		return prepStmtInsertHub;
	}

	public PreparedStatement getPrepStmtInsertNatKey() {
		return prepStmtInsertNatKey;
	}

	public RowMetaInterface getLookupRowMeta() {
		return lookupRowMeta;
	}

	public RowMetaInterface getInsertHubRowMeta() {
		return insertHubRowMeta;
	}

	public RowMetaInterface getInsertNatKeyRowMeta() {
		return insertNatKeyRowMeta;
	}

	public long getCurValueSurrKey() {
		return curValueSurrKey;
	}

	public void setCurValueSurrKey(Long key) {
		if (key == null) {
			curValueSurrKey = 0l;
		} else {
			curValueSurrKey = key.longValue();
		}

	}

	
}
