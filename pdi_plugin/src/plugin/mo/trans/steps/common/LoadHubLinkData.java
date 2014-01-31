package plugin.mo.trans.steps.common;

import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
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
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import plugin.mo.trans.steps.loadhub.LoadHubData;
import plugin.mo.trans.steps.loadhub.LoadHubMeta;
import plugin.mo.trans.steps.loadlink.LoadLinkMeta;
import plugin.mo.trans.steps.loadsat.LoadSatMeta;

/**
 * 
 * Data object useable for both Hub and Link
 *  
 * @author mouellet
 *
 */
public class LoadHubLinkData extends BaseStepData implements StepDataInterface {
	private static Class<?> PKG = CompositeValues.class;
	
	public Database db;
	//to add an extra pkey for Hub or Link 
	public RowMetaInterface outputRowMeta;
	
	// hold the real schema name (after any var substitution)
	private String realSchemaName;
	// hold the name schema.table (after any var substitution)
	private String qualifiedTable;

	// position index of composite key fields in row stream
	private int[] keysRowIdx;

	// position index of none-key fields in row stream (defined in UI)
	private int[] nonekeysRowIdx;

	private RowMetaInterface lookupRowMeta;
	private RowMetaInterface insertRowMeta;

	// Buffer storing original input rows appended with new surrKey
	private List<Object[]> bufferRows;

	// hold the lookup record (key(s) --> PKey)
	private Map<CompositeValues, Long> lookupMapping;
	
	
	public boolean finishedAllRows = false;
	
	private LogChannelInterface log;

	private PreparedStatement prepStmtLookup;
	private PreparedStatement prepStmtInsert;
	
	// Use to get/refresh the loadDTS
	private Date nowDate;

	
	public LoadHubLinkData(LogChannelInterface log) {
		super();
		this.log = log;
		db = null;
		
	}


	/*
	 * Must be called prior to Prepared Stmt init and any row processing 
	 */
	public void initializeRowProcessing(BaseLoadMeta meta, RowMetaInterface inputRowMeta) throws KettleStepException {		
		if (bufferRows == null) {
			bufferRows = new ArrayList<Object[]>(meta.getBufferSize()+10);
		}
		if (lookupMapping == null) {
			int capacity = (int) ((meta.getBufferSize())/0.75+1);
			lookupMapping = new HashMap<CompositeValues, Long>(capacity);
		}
		initRowIdx(meta,inputRowMeta);
		
		
		//initialize all naming needing variable substitution (${var})
		realSchemaName = meta.getDatabaseMeta().environmentSubstitute(meta.getSchemaName());
		String realtable = meta.getDatabaseMeta().environmentSubstitute(meta.getTargetTable());
		qualifiedTable = meta.getDatabaseMeta().getQuotedSchemaTableCombination(realSchemaName, realtable);
		
	}

	public Long getKeyfromMap(Object[] originalRow) {
		CompositeValues n = new CompositeValues(originalRow, keysRowIdx);
		return lookupMapping.get(n);
	}
	

	public boolean putKeyInMap(Object[] originalRow, Long valKey) {
		CompositeValues n = new CompositeValues(originalRow, keysRowIdx);
		if (lookupMapping.containsKey(n)){
			return false;
		} else {
			lookupMapping.put(n,valKey);
			return true;
		}
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
			log.logBasic("just before getRows with object r:" + Arrays.deepToString(r));
			CompositeValues v = new CompositeValues(r,1,keysRowIdx.length);
			lookupMapping.put(v, (Long) r[0]);
		}			
		return lookupMapping.size();
	}


	
	/*
	 * 
	 */
	public void initPrepStmtLookup(BaseLoadMeta meta, int bufferSize,
				RowMetaInterface inputRowMeta) throws KettleDatabaseException {

		lookupRowMeta = new RowMeta();
		
		/*
		 * SELECT <PK>, <compKey1>, <compKey2> .. 
		 * FROM <table> 
		 * WHERE 
		 * ( <key1> = ? AND <key2> = ?  .. ) 
		 * OR 
		 * ( <key1> = ? AND <key2> = ?  .. )
		 * 
		 * m-times (m=bufferSize)
		 */
		
		StringBuffer sql = new StringBuffer(meta.getBufferSize()*100);
		sql.append(" SELECT ").append(db.getDatabaseMeta().quoteField(meta.getTechKeyCol()));
		
		StringBuffer nkcols = new StringBuffer(100);
		StringBuffer endClause = new StringBuffer(meta.getBufferSize()*100);
		endClause.append(" WHERE ").append(Const.CR);

		for (int j = 0; j < bufferSize; j++) {
			endClause.append(" ( ");
			int keyCounter = 0;
			for (int i = 0; i < meta.getCols().length; i++) {
				String colType = meta.getTypes()[i];
				//TODO: put and share LoadLinkMeta.IDENTIFYING_KEY into Hub instead 
				if (colType.equals(LoadLinkMeta.IDENTIFYING_KEY)){
					if (keyCounter == 0){
						endClause.append(db.getDatabaseMeta().quoteField(meta.getCols()[i])).append("=?");
					} else {
						endClause.append(" AND ").append(db.getDatabaseMeta().quoteField(meta.getCols()[i])).append("=?");
					}
					// add Meta of key(s) col 
					if (j == 0) {
						nkcols.append(", ").append(db.getDatabaseMeta().quoteField(meta.getCols()[i]));
						int tmpMetatype = inputRowMeta.getValueMeta(keysRowIdx[keyCounter]).getType();
						lookupRowMeta.addValueMeta(new ValueMeta(meta.getCols()[i], tmpMetatype));
					}
					keyCounter++;
				}
			}
			endClause.append( " ) ");
			if (j < bufferSize - 1) {
				endClause.append(" OR ");
			}
		}
		sql.append(nkcols);
		sql.append(" FROM ").append(qualifiedTable).append(Const.CR);
		sql.append(endClause);

		try {
			log.logBasic("Prepared statement for Lookup:" + Const.CR + sql);
			prepStmtLookup = db.getConnection().prepareStatement(db.getDatabaseMeta().stripCR(sql));
			if (db.getDatabaseMeta().supportsSetMaxRows()) {
				// lookup cannot return more than BufferSize
				prepStmtLookup.setMaxRows(bufferSize);
			}
		} catch (SQLException ex) {
			throw new KettleDatabaseException(ex);
		}
	}


	public void initPrepStmtInsert(BaseLoadMeta meta, String keyGeneration, 
					String sequence, RowMetaInterface inputRowMeta) throws KettleDatabaseException {


		/*
		 * This applied for both Hub and Link:
		 * Column ordering rule: 1- cols composing natural/surr-key (same as for look-up)
		 * 						 2- other cols not part of composite keys (optional) 
		 * 						 3- technical audit columns
		 * 						 4- technical Primary key (not applicable for AUTO-INCREMENT)
		 * INSERT INTO table(key1, key2, .., nonekey1, nonekey2, sysAudits, .., PKey)
		 * VALUES(?, ?, ? ..)
		 * 
		 * n.b. VALUES when used with Sequence: VALUES(?, ?, ? .., newVal.getValues()) ;
		 * TODO: this is supported by Oracle...check others and see proper usage
		 *  
		 */

		insertRowMeta = new RowMeta();
		String sqlIns = "INSERT INTO " + qualifiedTable + "( ";
		String sqlValues = Const.CR + " VALUES (";
	
		// ***********************************************
		// 1- Handle composite keys 
		// ***********************************************
		int keyCounter = 0;
		for (int i = 0; i < meta.getCols().length; i++) {
			if (meta.getTypes()[i].equals(LoadLinkMeta.IDENTIFYING_KEY)){
				if (keyCounter == 0){
					sqlIns += db.getDatabaseMeta().quoteField(meta.getCols()[i]);
					sqlValues += "?";
				} else {
					sqlIns += ", " + db.getDatabaseMeta().quoteField(meta.getCols()[i]);
					sqlValues += ", ?";
				}
				int tmpMetatype = inputRowMeta.getValueMeta(keysRowIdx[keyCounter]).getType();
				insertRowMeta.addValueMeta(new ValueMeta(meta.getCols()[i], tmpMetatype));
				keyCounter++;
			}
		}
		
		// ***********************************************
		// 2- Handle other optional columns
		// ***********************************************
		int nonekeyCounter = 0;
		for (int i = 0; i < meta.getCols().length; i++) {
			if (meta.getTypes()[i].equals(LoadLinkMeta.OTHER_TYPE)){
				sqlIns += ", " + db.getDatabaseMeta().quoteField(meta.getCols()[i]);
				sqlValues += ", ?";
				
				int tmpMetatype = inputRowMeta.getValueMeta(nonekeysRowIdx[nonekeyCounter]).getType();
				insertRowMeta.addValueMeta(new ValueMeta(meta.getCols()[i], tmpMetatype));
				nonekeyCounter++;
			}
		}
	
		// ***********************************************
		// 3- Handle audit columns
		// ***********************************************
		if (!Const.isEmpty(meta.getAuditDtsCol())){
			sqlIns += ", " + db.getDatabaseMeta().quoteField(meta.getAuditDtsCol());
			sqlValues += ", ?";
			insertRowMeta.addValueMeta(new ValueMetaDate(meta.getAuditDtsCol()));
		}
		if (!Const.isEmpty(meta.getAuditRecSourceCol())){
			sqlIns += ", " + db.getDatabaseMeta().quoteField(meta.getAuditRecSourceCol());
			sqlValues += ", ?";
			insertRowMeta.addValueMeta(new ValueMetaString(meta.getAuditRecSourceCol()));
		}

		// ***********************************************
		// 4- Handle technical key (PK)
		// ***********************************************
		if (keyGeneration.equals(LoadHubMeta.CREATION_METHOD_SEQUENCE)) {
			sqlIns += ", " + db.getDatabaseMeta().quoteField(meta.getTechKeyCol());
			// Use Sequence, Oracle syntax: seqname.nextval (may need to remove Select, From dual clause..)
			String nextStr =  db.getDatabaseMeta().getSeqNextvalSQL(sequence); // .replace("SELECT", "").replace("FROM","").replace("dual",""); 
			sqlValues += ", " + nextStr; 
		} 
		else if (keyGeneration.equals(LoadHubMeta.CREATION_METHOD_TABLEMAX)) {
			sqlIns += ", " + db.getDatabaseMeta().quoteField(meta.getTechKeyCol());
			sqlValues += ", ?";
			insertRowMeta.addValueMeta(new ValueMetaInteger(meta.getTechKeyCol()));
		} 
		else if (keyGeneration.equals(LoadHubMeta.CREATION_METHOD_AUTOINC)) {
			// No need to refer to Column except for placeholder special requirement (ex. Informix) 
			//TODO: test this on Informix...
			if (db.getDatabaseMeta().needsPlaceHolder()) {
				sqlIns += ", 0";
			} 
		}
		sqlIns += " )";
		sqlValues += ") ";
		
		String sqlInsert = sqlIns + sqlValues;
		try {
			log.logBasic("Prepared statement for insert :" + Const.CR + sqlInsert);
			prepStmtInsert = db.getConnection().prepareStatement(db.getDatabaseMeta().stripCR(sqlInsert));
		} catch (SQLException ex) {
			throw new KettleDatabaseException(ex);
		}
	}

	/*
	//only used with method TABLEMAX
	//This should be called from a synchronized block in the Step class
	public void initSelectMax(String pKey, RowMetaInterface inputRowMeta, BaseStepMeta meta)
			throws KettleDatabaseException {
		
		// Method "Database.getOneRow(string sql)" is screwed up as it changes the
		// metaRow instance variable in Database! This impacts later call done on Database.
		// Use direct call to PrepareStmt & ResultSet instead
		String sqlMax = "SELECT " + " MAX(" + db.getDatabaseMeta().quoteField(pKey) + ") "
					+ "FROM " + qualifiedTable;
			Statement stmtMax = null;
			try {
				stmtMax = db.getConnection().createStatement();
				ResultSet maxrs = stmtMax.executeQuery(sqlMax);
				if (maxrs.next()) {
					//return 0 when Null 
					curSeqKey = maxrs.getLong(1);
					log.logBasic("Query returned max key: " + curSeqKey);
				} else {
					throw new KettleDatabaseException("Unable to get max key from Query: " + sqlMax);
				}
				if (stmtMax != null)
					stmtMax.close();
			} catch (SQLException e) {
				throw new KettleDatabaseException(e);
			}
	}
	*/
	
	
	private void initRowIdx(BaseLoadMeta meta, RowMetaInterface inputRowMeta) throws KettleStepException {
		int nbKey = 0;
		int nbNoneKey = 0;
		for (int i = 0; i < meta.getTypes().length; i++){
			if (meta.getTypes()[i].equals(LoadLinkMeta.IDENTIFYING_KEY)){
				nbKey++;
			} else if (meta.getTypes()[i].equals(LoadLinkMeta.OTHER_TYPE)){
				nbNoneKey++;
			}
		}
		
		keysRowIdx = new int[nbKey];
		nonekeysRowIdx = new int[nbNoneKey];
		nbKey = 0;
		nbNoneKey = 0;
		for (int i = 0; i < meta.getTypes().length; i++) {
			if (meta.getTypes()[i].equals(LoadLinkMeta.IDENTIFYING_KEY)){
				keysRowIdx[nbKey] = inputRowMeta.indexOfValue(meta.getFields()[i]);
				if (keysRowIdx[nbKey] < 0) {
					throw new KettleStepException(BaseMessages.getString(PKG,
							"Load.Exception.FieldNotFound", meta.getFields()[i]));
				}
				nbKey++;
			} else if (meta.getTypes()[i].equals(LoadLinkMeta.OTHER_TYPE)){
				nonekeysRowIdx[nbNoneKey] = inputRowMeta.indexOfValue(meta.getFields()[i]);
				if (nonekeysRowIdx[nbNoneKey] < 0) {
					throw new KettleStepException(BaseMessages.getString(PKG,
							"Load.Exception.FieldNotFound", meta.getFields()[i]));
				}
				nbNoneKey++;
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
	
	
	public void addBatchInsert(BaseLoadMeta meta, Object[] oriRow, Long newKey) throws KettleDatabaseException {		
		try {
		
			// ***********************************************
			// 1- Handle composite keys 
			// ***********************************************
			int pIdx = 0;
			for (int i = 0; i < keysRowIdx.length; i++) {
				pIdx = insertRowMeta.indexOfValue(meta.getCols()[keysRowIdx[i]]);
				db.setValue(prepStmtInsert,insertRowMeta.getValueMeta(pIdx),oriRow[keysRowIdx[i]],pIdx+1);
				log.logBasic("values keys:" + oriRow[keysRowIdx[i]] + " at pos =" + pIdx);
			}

			// ***********************************************
			// 2- Handle other optional columns
			// ***********************************************
			for (int i = 0; i < nonekeysRowIdx.length; i++) {
				pIdx = insertRowMeta.indexOfValue(meta.getCols()[nonekeysRowIdx[i]]);
				db.setValue(prepStmtInsert,insertRowMeta.getValueMeta(pIdx),oriRow[nonekeysRowIdx[i]],pIdx+1);
				log.logBasic("values other:" + oriRow[nonekeysRowIdx[i]] + " at pos =" + pIdx);
			}
			
			// ***********************************************
			// 3- Handle audit columns
			// ***********************************************
			if (!Const.isEmpty(meta.getAuditDtsCol())){
				pIdx = insertRowMeta.indexOfValue(meta.getAuditDtsCol());
				db.setValue(prepStmtInsert,insertRowMeta.getValueMeta(pIdx),getNowDate(true),pIdx+1);
				log.logBasic("audit :" + getNowDate(true));

			}
			if (!Const.isEmpty(meta.getAuditRecSourceCol())){
				pIdx = insertRowMeta.indexOfValue(meta.getAuditRecSourceCol());
				db.setValue(prepStmtInsert,insertRowMeta.getValueMeta(pIdx),meta.getAuditRecSourceValue(),pIdx+1);
				log.logBasic("audit src:" + meta.getAuditRecSourceValue());
			}
			
			// ***********************************************
			// 4- Handle technical key (PK)
			// ***********************************************
			if (meta.isTableMax()){
				pIdx = insertRowMeta.indexOfValue(meta.getTechKeyCol());
				db.setValue(prepStmtInsert,insertRowMeta.getValueMeta(pIdx),newKey,pIdx+1);
				log.logBasic("tech key:" + newKey + "  at pos=" + pIdx);
			}
			prepStmtInsert.addBatch();
		
		if (log.isRowLevel()){
			log.logRowlevel("Adding batch values: " + Arrays.deepToString(oriRow));
		}
		} catch ( SQLException ex ) {
		  throw new KettleDatabaseException( "Error adding batch for rowMeta: " + insertRowMeta.toStringMeta(), ex );
		} 
	}

	
	public void executeBatchInsert(BaseStepMeta meta, int insertCtnExpected) throws KettleDatabaseException {
        try {
        	int[] r = prepStmtInsert.executeBatch();
        	log.logError("FFFFFFFFSuccessfully executed insert batch with " + r.length);
        	prepStmtInsert.clearBatch();
    		if (log.isDebug()){
    			log.logDebug("Successfully executed insert batch");
    		}
          } catch ( BatchUpdateException ex ) {
        	  int[] nbIns = ex.getUpdateCounts();
        	  if (insertCtnExpected == nbIns.length){
        		  log.logError("BatchUpdateException raised but all rows processed", ex);
        		  //Continue processing.  Check for SQLIntegrityConstraintViolationException 
        		  // For Hub: business key(s) already exist (to be checked later)
        		  // Link: either FKs already exist (if confirmed, ignore later) or FK constraint violation (then fail process...)
        		  //To be confirmed by calling processRow()
        	  } else {
        		  log.logError("BatchUpdateException raised and NOT all rows processed",ex);
        		  throw new KettleDatabaseException( ex ); 
        	  }
          } catch ( SQLException ex ) {
                  throw new KettleDatabaseException( ex );
          }  
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


	public String getRealSchemaName() {
		return realSchemaName;
	}
	
	public String getQualifiedTable() {
		return qualifiedTable;
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
