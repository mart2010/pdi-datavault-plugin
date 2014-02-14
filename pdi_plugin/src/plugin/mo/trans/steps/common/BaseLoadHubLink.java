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
package plugin.mo.trans.steps.common;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

/**
 * This base step is used for loading both Hub AND Link, and could be override in future
 * for more specialized functions.
 *  
 *  
 * TO BE RE-ANALYZED. 
 * 
 * <b>Notes on Concurrency :</b>
 * Concurrency is not managed by blocking operations with synchronization.  We rather left data flow and 
 * fail at DB level.....  
 * 
 * Operations like DB look-up must be done serially to avoid threads generating different tech keys 
 * on identical business keys ("missed" look-ups).
 * <p>
 * It turns out, these operations take the bulk of time and synchronizing them is equivalent to
 * running them serially, while adding in code complexity and increasing likelihood of dead-lock.
 * <p>
 * DB Round-trip IS the single operation having the largest impact (by any factor) on Step 
 * total processing time. Design principle is aiming to mitigate by favoring "bulk" processing: 
 * <ul>
 * <li>1) batching JDBC insert/update 
 * <li>2) look-up Query done on a number of business keys at once    
 * </ul>
 * <p>
 * This is controlled with the parameter "Buffer size" defined in UI.
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
public class BaseLoadHubLink extends BaseStep implements StepInterface {
	private static Class<?> PKG = CompositeValues.class;
	
	protected LoadHubLinkData data;
	protected BaseLoadMeta meta;
	
	
	public BaseLoadHubLink(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
		meta = (BaseLoadMeta) getStepMeta().getStepMetaInterface();
		data = (LoadHubLinkData) stepDataInterface;
	}

	
	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {

		Object[] originalRow = getRow();
		boolean bufferNotFull = true;

		if (first) {
			first = false;
			initializeWithFirstRow();
		}

		Object[] rowNullAppended;
		// Add current row appended with null field to Buffer
		if (originalRow != null) {
			//convert fields stored as BINARY (lazy conversion)
			//this is done implicitly during setValue(), but converting
			//here avoid converting many times downstream
			if (data.getFieldsInBinary() != null){
				for (int i=0; i < data.getFieldsInBinary().length; i++){
					int fi = data.getFieldsInBinary()[i];
					ValueMetaInterface valueMeta = getInputRowMeta().getValueMeta(fi);
					originalRow[fi] = valueMeta.convertToNormalStorageType( originalRow[fi] );
				}
			}
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
		 * From here: buffer is either full OR partially full with no more rows expected
		 *****/
		
		/***** step-1 --> Query DB and fill LookupMap  ******/

		int nbLookup = data.populateMap(data.getBufferRows(),meta.getBufferSize());
		if (log.isDetailed()){
			logDetailed("Buffer filled, number of fetched hub records from DB= " + nbLookup);	
		}

		
		/***** step-2 --> Manage existing: append key, send downstream & remove from buffer *****/
		if (nbLookup > 0){
			processBufferAndSendRows(getInputRowMeta().size());	
			// Processing finished when all keys were found!
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
		//needed for key generation: TABLEMAX
		Long newKey = null;
		
		List<Object[]> queryParams = new ArrayList<Object[]>(meta.getBufferSize()+10);
		for (Object[] newRow : data.getBufferRows()){
			if (!data.putKeyInMap(newRow,null)){
				continue;
			}
			//Append key for TABLEMAX
			if (meta.isMethodTableMax()){
				//fetch the next Key (this takes care of synchronization, but has a nasty bug: 
				//change rowMeta state in Database, so cannot call methods like getRows() subsequently..
				newKey = data.db.getNextValue( getTrans().getCounters(), meta.getSchemaName(),
						 		meta.getTargetTable(), meta.getTechKeyCol());
			} 
			
			data.addBatchInsert(meta, newRow, newKey);
			incrementLinesOutput();
			queryParams.add(newRow);
		}
		
 
		
		/***** step-4 --> Execute batch, fill Map with new keys, validate and if OK: commit ******/
		//TODO: verify that with this CHECK we are good for MULTI-THREADED SUPPORT!?
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

		if (data.getBufferRows().size() > 0 )
			throw new IllegalStateException("Buffer should be empty, check program logic");
		
	
		/***** step-5 --> Continue processing or Exit if no more rows *****/
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
			Long key = data.getKeyfromLookupMap(r);
			if (key != null) {
				r[newKeyPos] = key;
				putRow(data.outputRowMeta, r);
				iter.remove();
			}
		}
	}
	
	
	private void initializeWithFirstRow() throws KettleStepException, KettleDatabaseException {
		data.outputRowMeta = getInputRowMeta().clone();
		data.initializeRowProcessing((BaseLoadMeta) meta);
		data.initPrepStmtLookup( (BaseLoadMeta) meta, meta.getBufferSize());
		data.initPrepStmtInsert( (BaseLoadMeta) meta);
		meta.getFields( data.outputRowMeta, getStepname(), null, null, this, repository, metaStore );
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
				//Commit is taken care of explicitly
				data.db.setAutoCommit(false);

				return true;
			} catch (KettleDatabaseException dbe) {
				logError(BaseMessages.getString(PKG, "Load.Log.UnableToConnectDB") + dbe.getMessage());
			}
		}
		return false;
		
	}
	
public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
	meta = (BaseLoadMeta) smi;
	data = (LoadHubLinkData) sdi;

	if (data.db != null) {
		try {
			//TODO: not sure whether I do error handling correctly here to have the getErrors() work as designed!! to validate and align 
			if (getErrors() == 0) {
				data.db.commit();
			} else {
				data.db.rollback();
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
