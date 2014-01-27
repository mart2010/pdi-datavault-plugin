package plugin.mo.trans.steps.loadlink;

import java.lang.invoke.MethodHandles.Lookup;
import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;

import plugin.mo.trans.steps.common.CompositeValues;
import plugin.mo.trans.steps.loadhub.LoadHubData;
import plugin.mo.trans.steps.loadhub.LoadHubMeta;
import plugin.mo.trans.steps.loadsat.LoadSatMeta;

/**
 * 
 * Share Data object useable for both Hub and Link 
 * @author mouellet
 *
 */
public class LoadHubOrLinkData extends BaseStepData implements StepDataInterface {
	private static Class<?> PKG = CompositeValues.class;
	
	public Database db;
	//to add an extra pkey for Hub or Link 
	public RowMetaInterface outputRowMeta;
	
	// hold the real schema name (after any var substitution)
	private String realSchemaName;
	// hold the name schema.table
	private String qualifiedTable;

	// index of key fields position in row stream
	private int[] keysRowIdx;

	// hold the lookup meta of keys
	private RowMetaInterface lookupRowMeta;

	private RowMetaInterface insertRowMeta;

	// Buffer storing original input rows appended with new surrKey
	private List<Object[]> bufferRows;

	// hold the lookup record (key(s) --> PKey)
	private Map<CompositeValues, Long> lookupMapping;
	
	// used with CREATION_METHOD_TABLEMAX 
	private long curValueKey;

	
	public boolean finishedAllRows = false;

	private PreparedStatement prepStmtLookup;
	private PreparedStatement prepStmtInsert;
	
	
	public LoadHubOrLinkData() {
		super();
		db = null;
	}


	/*
	 * Must be called prior any row processing 
	 */
	public void initializeRowProcessing(int bsize) {		
		if (bufferRows == null) {
			bufferRows = new ArrayList<Object[]>(bsize+10);
		}
		if (lookupMapping == null) {
			int capacity = (int) ((bsize)/0.75+1);
			lookupMapping = new HashMap<CompositeValues, Long>(capacity);
		}
	}

	

	public Long getKeyfromMap(Object[] originalRow) {
		CompositeValues n = new CompositeValues(originalRow, keysRowIdx);
		return lookupMapping.get(n);
	}
	
	/**
	 * 
	 * Populate bufferLookupMapping from lookup Query result.  
	 * ValueMeta in LookupMeta MUST follow same order as parameters 
	 * found in rows using their position index: keysPosInRow
	 *  
	 * @param rows
	 * @param nbParamsClause
	 * @return number of row with successful lookup
	 * @throws KettleDatabaseException 
	 */
	public int populateMap(List<Object[]> rows, int nbParamsClause) throws KettleDatabaseException {
		//clean-up previous map
		lookupMapping.clear();
			
		// Setting values for prepared Statement
		for (int i = 0; i < nbParamsClause; i++) {
			Object[] p;
			// in case, we have less rows
			try {
				p = rows.get(i);
			} catch (IndexOutOfBoundsException e) {
				p = null;
			}
			for (int j = 0; j < keysRowIdx.length; j++) {
				int pIdx = (i * keysRowIdx.length) + (j + 1);
				db.setValue(prepStmtLookup, lookupRowMeta.getValueMeta(j),
						(p == null) ? null : p[keysRowIdx[j]], pIdx);
			}
		}

		ResultSet rs;
		try {
			rs = prepStmtLookup.executeQuery();
			//release prepared Stmt params  
			prepStmtLookup.clearParameters();
		} catch (SQLException e) {
			throw new KettleDatabaseException("Unable to execute Lookup query", e);
		}

		for (Object[] r : db.getRows(rs, nbParamsClause, null)) {
			CompositeValues v = new CompositeValues(r,1,keysRowIdx.length);
			lookupMapping.put(v, (Long) r[0]);
		}			
		return lookupMapping.size();
	}


	
	/*
	 * 
	 */
	public void initPrepStmtLookup(LoadLinkMeta meta, RowMetaInterface inputRowMeta) throws KettleDatabaseException {

		DatabaseMeta dbMeta = meta.getDatabaseMeta();
		lookupRowMeta = new RowMeta();
		
		/*
		 * SELECT <PK>, <key1>, <key2> .. 
		 * FROM <table> 
		 * WHERE 
		 * ( <key1> = ? AND <key2> = ?  .. ) 
		 * OR 
		 * ( <key1> = ? AND <key2> = ?  .. )
		 * 
		 * m-times-->bufferSize
		 */
		
		String sql = " SELECT " + dbMeta.quoteField(meta.getPrimaryKeyCol());
		String nkcols = "";
		String endClause = " WHERE " + Const.CR;

		for (int j = 0; j < meta.getBufferSize(); j++) {
			endClause  += " ( ";
			for (int i = 0; i < meta.getKeyCol().length; i++) {
				endClause += dbMeta.quoteField(meta.getKeyCol()[i]) + " = ? ";
				if (i < meta.getKeyCol().length-1){
					endClause  += " AND ";
				}
				// add Meta of key(s) col 
				if (j == 0) {
					nkcols += ", " + dbMeta.quoteField(meta.getKeyCol()[i]);
					int tmpMetatype = inputRowMeta.getValueMeta(keysRowIdx[i]).getType();
					lookupRowMeta.addValueMeta(i, new ValueMeta(meta.getKeyCol()[i], tmpMetatype));
				}
			}
			endClause  += " ) " + Const.CR;
			if (j < meta.getBufferSize() - 1) {
				endClause += " OR " + Const.CR;
			}
		}

		sql += nkcols;
		sql += " FROM " + qualifiedTable + Const.CR;
		sql += endClause;

		try {
			meta.getLog().logBasic("Prepared statement for Lookup:" + Const.CR + sql);
			prepStmtLookup = db.getConnection().prepareStatement(dbMeta.stripCR(sql));
			if (dbMeta.supportsSetMaxRows()) {
				// lookup cannot return more than BufferSize
				prepStmtLookup.setMaxRows(meta.getBufferSize());
			}
		} catch (SQLException ex) {
			throw new KettleDatabaseException(ex);
		}
		
	}


	public void initPrepStmtInsert(LoadLinkMeta meta, RowMetaInterface inputRowMeta)
			throws KettleDatabaseException {

		DatabaseMeta dbMeta = meta.getDatabaseMeta();
		//insert RowMeta is same as lookup (with Auto-Inc & Sequence), but add other fields like sys audit, etc. 
		insertRowMeta = lookupRowMeta.clone();

		/*
		 * INSERT INTO table(hkey1, hkey2, .., PKey)
		 * VALUES(?, ?, ? ..) ;
		 * 
		 * n.b. VALUES when used with Sequence: VALUES(?, ?, ? .., newVal.getValues()) ;
		 * TODO: this is supported by Oracle...check others and see proper usage
		 *  
		 */

		String sqlIns = "INSERT INTO " + qualifiedTable + "( ";
		String sqlValues = Const.CR + " VALUES (";
	
		//Handle all keys
		for (int i = 0; i < meta.getKeyCol().length; i++) {
			sqlIns += db.getDatabaseMeta().quoteField(meta.getKeyCol()[i]);
			sqlValues += "?";	
			if (i < meta.getKeyCol().length-1){
				sqlIns += ", ";
				sqlValues += ", ";
			}
		}
		
		//Handle Primary Key according to key creation
		if (meta.getKeyCreation().equals(LoadHubMeta.CREATION_METHOD_SEQUENCE)) {
			sqlIns += ", " + db.getDatabaseMeta().quoteField(meta.getPrimaryKeyCol());
			// Use Sequence, Oracle syntax: seqname.nextval (may need to remove Select, From dual clause..)
			String nextStr =  dbMeta.getSeqNextvalSQL(meta.getSequenceName()); // .replace("SELECT", "").replace("FROM","").replace("dual",""); 
			sqlValues += ", " + nextStr; 
		} 
		else if (meta.getKeyCreation().equals(LoadHubMeta.CREATION_METHOD_TABLEMAX)) {
			//use TABLE MAX
			String pk = meta.getPrimaryKeyCol();
			sqlIns += ", " + db.getDatabaseMeta().quoteField(pk);
			sqlValues += " ,?";
			insertRowMeta.addValueMeta(new ValueMetaInteger(pk));
		} 
		else if (meta.getKeyCreation().equals(LoadHubMeta.CREATION_METHOD_AUTOINC)) {
			// Here no need to add Col, but dummy placeholder my be required (Informix) ...To Test this...
			if (db.getDatabaseMeta().needsPlaceHolder()) {
				sqlIns += ", 0";
			} 
		}
	
		sqlIns += " )";
		sqlValues += ") ";
		
		String sqlInsert = sqlIns + sqlValues;
			
		//TODO: handle the audit stuff etc.. 
		//If I add stuff, should add new ValueMeta in insertLinkRowMeta accordingly ....
		try {
			meta.getLog().logBasic("Prepared statement for insert :" + Const.CR + sqlInsert);
			prepStmtInsert = db.getConnection().prepareStatement(dbMeta.stripCR(sqlInsert));
		} catch (SQLException ex) {
			throw new KettleDatabaseException(ex);
		}
		
	}
	
	//only used with method TABLEMAX
	//again, not designed for multi-thread here
	private void initSelectMax(LoadLinkMeta meta, RowMetaInterface inputRowMeta)
			throws KettleDatabaseException {
		
		if (!meta.getKeyCreation().equals(LoadHubMeta.CREATION_METHOD_TABLEMAX)) {
			return;
		}
		// Method "Database.getOneRow(string sql)" is screwed up as it changes the
		// metaRow instance variable in Database! This impacts later call done on Database.
		// Use direct call to PrepareStmt & ResultSet instead
		String sqlMax = "SELECT " + " MAX(" + db.getDatabaseMeta().quoteField(meta.getPrimaryKeyCol()) + ") "
					+ "FROM " + qualifiedTable;
			Statement stmtMax = null;
			try {
				stmtMax = db.getConnection().createStatement();
				ResultSet maxrs = stmtMax.executeQuery(sqlMax);
				if (maxrs.next()) {
					//return 0 when Null 
					curValueKey = maxrs.getLong(1);
				} else {
					throw new KettleDatabaseException("Unable to get max(key)");
				}
				if (stmtMax != null)
					stmtMax.close();
			} catch (SQLException e) {
				throw new KettleDatabaseException(e);
			}
		
	}
	
	public Long getNextKey(){
		curValueKey++;
		return Long.valueOf(curValueKey);
	}

	
	
	public void initKeysRowIdx(LoadLinkMeta meta, RowMetaInterface inputRowMeta) throws KettleStepException {
		keysRowIdx = new int[meta.getKeyField().length];
		for (int i = 0; i < meta.getKeyField().length; i++) {
			keysRowIdx[i] = inputRowMeta.indexOfValue(meta.getKeyField()[i]);
			if (keysRowIdx[i] < 0) {
				// couldn't find field!
				throw new KettleStepException(BaseMessages.getString(PKG,
						"Load??Hub.Exception.FieldNotFound", meta.getKeyField()[i]));
			}
		}
	}


	public boolean addToBufferRows(Object[] r, int bufferSize) {
		if (bufferRows.size() < bufferSize) {
			bufferRows.add(r);
			return (bufferRows.size() < bufferSize);
		} else {
			return false;
		}
	}
	
	
	
	public void addBatchInsert(LoadLinkMeta meta, Object[] values) throws KettleDatabaseException {		
		try {
		db.setValues(insertRowMeta, values, prepStmtInsert);
		prepStmtInsert.addBatch();
		if (meta.getLog().isRowLevel()){
			meta.getLog().logRowlevel("Adding batch values: " + Arrays.deepToString(values));
		}
		} catch ( SQLException ex ) {
		  throw new KettleDatabaseException( "Error adding batch for rowMeta: " + insertRowMeta.toStringMeta(), ex );
		} 
	}

	
	public void executeBatchInsert(LoadLinkMeta meta, int insertCtnExpected) throws KettleDatabaseException {
        try {
        	prepStmtInsert.executeBatch();
        	prepStmtInsert.clearBatch();
    		if (meta.getLog().isDebug()){
    			meta.getLog().logDebug("Successfully executed insert batch");
    		}
          } catch ( BatchUpdateException ex ) {
        	  int[] nbIns = ex.getUpdateCounts();
        	  if (insertCtnExpected == nbIns.length){
        		  meta.getLog().logError("BatchUpdateException raised but all rows processed", ex);
        		  //Continue processing.  Check for SQLIntegrityConstraintViolationException 
        		  // For Hub: business key(s) already exist (to be checked later)
        		  // Link: either FKs already exist (if confirmed, ignore later) or FK constraint violation (then fail process...)
        		  //To be confirmed by calling processRow()
        	  } else {
        		  meta.getLog().logError("BatchUpdateException raised and NOT all rows processed",ex);
        		  throw new KettleDatabaseException( ex ); 
        	  }
          } catch ( SQLException ex ) {
                  throw new KettleDatabaseException( ex );
          }  
	}

	
	


	public String getRealSchemaName() {
		return realSchemaName;
	}
	public void setRealSchemaName(DatabaseMeta dbMeta, String uiEntrySchemaName) {
		this.realSchemaName = dbMeta.environmentSubstitute(uiEntrySchemaName);
	}

	public String getQualifiedTable() {
		return qualifiedTable;
	}
	public void setQualifiedLinkTable(DatabaseMeta dbMeta, String uiEntryLinkTable) {
		// replace potential ${var} by their subs env. values
		String realLinkTable = dbMeta.environmentSubstitute(uiEntryLinkTable);
		qualifiedTable = dbMeta.getQuotedSchemaTableCombination(realSchemaName, realLinkTable);
	}



	public List<Object[]> getBufferRows() {
		return bufferRows;
	}		
		

	public int[] getKeysRowIdx() {
		return keysRowIdx;
	}



	public PreparedStatement getPrepStmtLookup() {
		return prepStmtLookup;
	}



	public PreparedStatement getPrepStmtInsert() {
		return prepStmtInsert;
	}


	public RowMetaInterface getLookupRowMeta() {
		return lookupRowMeta;
	}



	public Map<CompositeValues, Long> getLookupMapping() {
		return lookupMapping;
	}

	
	
	
	
	
}
