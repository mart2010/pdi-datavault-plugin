package plugin.mo.trans.steps.loadlink;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import plugin.mo.trans.steps.common.BaseLoadMeta;
import plugin.mo.trans.steps.common.CompositeValues;
import plugin.mo.trans.steps.common.LoadHubLinkData;
import plugin.mo.trans.steps.loadsat.LoadSatData;
import plugin.mo.trans.steps.loadsat.LoadSatMeta;

/**
 * 
 * 
 * <b>Notes on multi-threading/concurrency:</b>
 * <p>
 * 
 * MAYBE ONLY TRUE WHEN USING TABLEMAX... WITH ALL MY ERROR HANDLING WITH BATCH COULD WORK!!!! TO BE RE-ANALYZED.
 * BUT STILL TRUE ABOUT BOTTLENECK AND SYNCHRONIZATION BYT MY CODE DOES NOT BLOCK BY ALLOWING DB ERRORS AND WORK AROUND THEM
 * These "Load*" plugins are not meant for "Run multiple copies..".   This is to avoid 
 * blocking long operations requiring synchronization.  Operations like DB look-up must be done 
 * serially to avoid multiple threads generating different keys for "missed" look-up on same natural-key.<b>
 * <p>
 * It turns out, these operations take large amount of time and synchronizing them is equivalent to
 * run them serially, while adding in code complexity and increasing likelihood of dead-lock.
 * <p>
 * DB Round-trip IS the single operation having the largest impact (by any factor) on Step 
 * total processing time.
 * <p> 
 * Design principle is thus aiming for more "bulk" processing to reduce these DB round-trip: 
 * <ul>
 * <li>1) batching JDBC insert/update 
 * <li>2) look-up Query on large number of keys at once)    
 * </ul>
 * <p>
 * This is controlled with the Buffer size defined in UI.
 * <p><p>
 * <b>Notes on Batch:</b>
 * <p>
 * Most JDBC support Batch mode, although some may just emulate its function. 
 * JDBC supporting batch:  Mysql 5.x+, PostgreSQL 8.x+,  Oracle 11.x+, DB2, SQL-server, even H2 and Derby.
 * (http://java-persistence-performance.blogspot.ch/2013/05/batch-writing-and-dynamic-vs.html)
 * 
 * Consequently better long term solution to code using Batch mode.
 *  
 *   
 * 
 * @author mouellet
 *
 */
public class LoadLink extends BaseStep implements StepInterface {
	private static Class<?> PKG = CompositeValues.class;
	
	private LoadHubLinkData data;
	private LoadLinkMeta meta;
	
	public LoadLink(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
		meta = (LoadLinkMeta) getStepMeta().getStepMetaInterface();
		data = (LoadHubLinkData) stepDataInterface;
		
	}

	
	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {

		// request new row (wait until available) & indicate busy!
		Object[] originalRow = getRow();
		Object[] rowNullAppended;
		boolean bufferNotFull = true;

		// initialize data on first row
		if (first) {
			first = false;
			initializeWithFirstRow();
		}

		// Add current row appended with null field to Buffer
		if (originalRow != null) {
			rowNullAppended = RowDataUtil.addValueData(originalRow, getInputRowMeta().size(), null);
			bufferNotFull = data.addToBufferRows(rowNullAppended, meta.getBufferSize());
		}
		// Done: no more rows to be expected...
		else {
			data.finishedAllRows = true;
			// Exceptionally buffer is empty, so we're done!
			// i.e. last processed row filled up buffer
			if (data.getBufferRows().size() == 0) {
				setOutputDone();
				return false;
			}

		}

		// Not done, return and to request more rows
		if (!data.finishedAllRows && bufferNotFull) {
			return true;
		}


		/*****
		 * From here: buffer is either full OR partially full but no more rows expected
		 *****/
		
		/***** step-1 --> Query DB and fill LookupMap  ******/

		int nbLookup = data.populateMap(data.getBufferRows(),meta.getBufferSize());
		
		log.logBasic("Frist lookup return no of ele:" + nbLookup);
		
		/***** step-2 --> Manage existing link: add key field, send downstream & remove from buffer *****/
		
		if (nbLookup > 0){
			processBufferAndSendRows(getInputRowMeta().size());	
			// Processing buffer finished when all key found!
			if (data.getBufferRows().size() == 0) {
				if (!data.finishedAllRows) {
					return true;
				} else {
					setOutputDone();
					return false;
				}
			}
		}
		
		/***** step-3 --> Add new rows to Batch while updating LookupMap ******/
		//needed for TABLEMAX only
		Long newKey = null;
		List<Object[]> queryParams = new ArrayList<Object[]>(meta.getBufferSize()+10);
		for (Object[] newRow : data.getBufferRows()){
			if (!data.putKeyInMap(newRow,null)){
				continue;
			}

			//Append Key when managed by TABLEMAX
			if (meta.isTableMax()){
				//fetch the newest Key (still to do...)
				newKey = data.db.getNextValue( getTrans().getCounters(), meta.getSchemaName(),
						 		meta.getTargetTable(), meta.getTechKeyCol());
				log.logBasic("Adding new row with TABLEMAX with key:" + newKey);
			} 
			
			data.addBatchInsert(meta, newRow, newKey);
			log.logBasic("Adding batch with newRow= " + Arrays.deepToString(newRow) + " abn bewKey=" + newKey);
			incrementLinesOutput();
			queryParams.add(newRow);
		}
		
 
		
		/***** step-4 --> Execute batch and fill Map with all new PKeys, if OK: commit ******/
		//WITH THIS CHECK, MAY BE GOOD FOR MULTI-THREADED SUPPORT!!!
		data.executeBatchInsert(meta, queryParams.size());
		
		int rowsAdded = data.populateMap(queryParams,meta.getBufferSize());		
		if (rowsAdded != queryParams.size()){
			data.db.rollback();
			throw new IllegalStateException("DB state error, nb of new keys loaded= " 
								+ rowsAdded + " but expecting= " + queryParams.size() );
		}
		//process remaining of Buffer with new Mapping
		processBufferAndSendRows(getInputRowMeta().size());
		//At this point all is safe, we commit
		data.db.commit();

		//watch for program logic issues
		if (data.getBufferRows().size() > 0 )
			throw new IllegalStateException("Buffer should be empty, check program logic");
			
	
	
		/***** step-6 --> Continue processing or Exit if no more rows expected *****/
		if (!data.finishedAllRows) {
			return true;
		} else {
			setOutputDone();
			return false;
		}
		
	}
	
	
	private void processBufferAndSendRows(int newKeyPos) throws KettleStepException{
		// using Iterator to remove safely existing rows
		Iterator<Object[]> iter = data.getBufferRows().iterator();
		while (iter.hasNext()) {
			Object[] r = iter.next();
			Long key = data.getKeyfromMap(r);
			if (key != null) {
				r[newKeyPos] = key;
				putRow(data.outputRowMeta, r);
				iter.remove();
			}
		}
	}
	
	
	private void initializeWithFirstRow() throws KettleStepException, KettleDatabaseException {
		
		data.outputRowMeta = getInputRowMeta().clone();
		meta.getFields( data.outputRowMeta, getStepname(), null, null, this, repository, metaStore );
	
		// Initialize the row indexes of keys and none-keys...
		if (meta.getCols() == null || meta.getCols().length < 2) {
			throw new KettleStepException(BaseMessages.getString(PKG, "LoadLinkMeta.CheckResult.KeyFieldsIssues"));
		}
		data.initializeRowProcessing((BaseLoadMeta) meta, getInputRowMeta());
		data.initPrepStmtLookup( (BaseLoadMeta) meta, meta.getBufferSize(), getInputRowMeta());
		data.initPrepStmtInsert( (BaseLoadMeta) meta, meta.getKeyGeneration(), meta.getSchemaName(), getInputRowMeta());
	}		
		
	
	
public boolean init(StepMetaInterface sii, StepDataInterface sdi) {
		
		if (super.init(sii, sdi)) {
			if (meta.getDatabaseMeta() == null) {
				logError(BaseMessages.getString(PKG, "Load.Init.ConnectionMissing", getStepname()));
				return false;
			}
			data.db = new Database(this, meta.getDatabaseMeta());
			//Is this to allow using "environmentSubstitute" to work inside Database class?
			data.db.shareVariablesWith(this);
			
			try {
				if (getTransMeta().isUsingUniqueConnections()) {
					synchronized (getTrans()) {
						data.db.connect(getTrans().getTransactionId(), getPartitionID());
					}
				} else {
					data.db.connect(getPartitionID());
				}
				
				if (log.isDetailed()) {
					logDetailed(BaseMessages.getString(PKG, "Load.Log.ConnectedToDB"));
				}
				//Commit is done explicitly
				data.db.setAutoCommit(false);

				return true;
			} catch (KettleDatabaseException dbe) {
				logError(BaseMessages.getString(PKG, "Load.Log.UnableToConnectDB") + dbe.getMessage());
			}
		}
		return false;
		
	}
	
public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
	meta = (LoadLinkMeta) smi;
	data = (LoadHubLinkData) sdi;

	if (data.db != null) {
		try {
			//TODO: not sure whether I do error handling correctly here to have the getErrors() work as designed!! to validate and align 
			if (getErrors() == 0) {
				data.db.commit();
			} else {
				//data.db.rollback();
			}
			data.db.closePreparedStatement(data.getPrepStmtLookup());
			data.db.closePreparedStatement(data.getPrepStmtInsert());
			//data.db.closePreparedStatement(data.getPrepStmtUpdateSat());	
		} catch (KettleDatabaseException e) {
			logError(BaseMessages.getString(PKG, "Load.Log.UnexpectedError") + " : " + e.toString());
		} finally {
			data.db.disconnect();
		}
	}
	super.dispose(smi, sdi);
}

	
	
	
}
