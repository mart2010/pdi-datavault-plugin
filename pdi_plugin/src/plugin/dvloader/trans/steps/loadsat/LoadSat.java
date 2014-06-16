/*
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
 * Copyright (c) 2014 Martin Ouellet
 *
 */
package plugin.dvloader.trans.steps.loadsat;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import plugin.dvloader.trans.steps.common.BaseLoadMeta;
import plugin.dvloader.trans.steps.common.SatRecord;

/**
 * 
 * Load Attribute/Satellite table based on surrogate foreign key 
 * referencing to parent hub/anchor/link table.  This surrogate foreign key 
 * is mandatory in input field, so assume the hub/anchor/link have been 
 * loaded prior with Step: Load Hub/Anchor or Load Link/Tie. 
 * <p>
 * <p>
 * This step supports these features:
 * 1) More than one attribute is possible (a-la DV)
 * 2) Record can be either static (immutable) or temporal (using a 'FromDate' time point field) 
 * 3) With static, uses the first record read and ignores all subsequent records
 * 4) With temporal:
 * 		> must have an input field used as a 'ValidFrom' time point 
 *      > can either ignore identical consecutive records (Idempotent) or load them as-is
 * 5) Attribute/Satellite table can also include a closing 'toDate' expire column
 * 
 * 
 * Again, as with Load Hub and Sat, the strategy here is then to favor <b>batch</b> over multi-threading.  
 * So "# of copies to start.." should be 1 for these Steps. 
 *       
 * @author mouellet
 * 
 */
public class LoadSat extends BaseStep implements StepInterface {
	private static Class<?> PKG = BaseLoadMeta.class;
	
	private LoadSatData data;
	private LoadSatMeta meta;

	public LoadSat(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
		meta = (LoadSatMeta) getStepMeta().getStepMetaInterface();
		data = (LoadSatData) stepDataInterface;
	}

	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {

		// request and wait for new row & indicate busy!
		Object[] originalRow = getRow();
		boolean bufferNotFull = true;

		if (first) {
			first = false;
			if (originalRow != null){
				initializeWithFirstRow();	
			} else {
				setOutputDone();
				return false;
			}
		}

		// Add current row to Buffer 
		if (originalRow != null) {
			//proceed with fields conversion when stored in BINARY
			if (data.getFieldsInBinary() != null){
				for (int i=0; i < data.getFieldsInBinary().length; i++){
					int fi = data.getFieldsInBinary()[i];
					ValueMetaInterface valueMeta = getInputRowMeta().getValueMeta(fi);
					originalRow[fi] = valueMeta.convertToNormalStorageType( originalRow[fi] );
				}
			}

			bufferNotFull = data.addToBufferRows(originalRow, meta.getBufferSize());
		}
		// Done: no more rows to be expected...
		else {
			data.finishedAllRows = true;
			// Exceptionally the buffer is empty, so done
			// i.e. last processed row filled up buffer
			if (data.getBufferRows().size() == 0) {
				setOutputDone();
				return false;
			}
		}

		// Not done, return to request more rows
		if (!data.finishedAllRows && bufferNotFull) {
			return true;
		}


		/*****
		 * From here: buffer is either full OR partially full with no more rows expected
		 *****/
		
		/***** step-1 --> Query DB and fill bufferSatHistRows  ******/

		int nb = data.populateLookupMap(meta, meta.getBufferSize());
		if (log.isDetailed()){
			logDetailed("Buffer filled, number of fetched sat history records from DB= " + nb);	
		}
		
		/***** step-2 --> Add new records into bufferSatHistRows, ignore/send downstream duplicates ******
		 *******          (guarantees sorting needed for Idempotent & for updating "toDate")        ******/
		Iterator<Object[]> iter = data.getBufferRows().iterator();
		while (iter.hasNext()) {
			Object[] bufferRow = iter.next();
			SatRecord newRow = new SatRecord(bufferRow,data.getSatAttsRowIdx(),
											data.posFkInRow,data.posFromDateInRow);
			
			if (!data.getBufferSatHistRows().add(newRow)){
				//ignore duplicate (e.g. dups in-stream, immutable attr..)
				putRow(data.outputRowMeta, bufferRow);
				iter.remove();
			} else {
				//attach any meta-attributes for new sat record
				if (data.getSatMetaAttsRowIdx() != null){
					newRow.setMetaAtts(bufferRow,data.getSatMetaAttsRowIdx());
				}
			}
		}
		// Exit if all buffer rows were duplicates 
		if (data.getBufferRows().size() == 0) {
			data.emptyBuffersAndClearPrepStmts();
			if (!data.finishedAllRows) {
				return true;
			} else {
				setOutputDone();
				return false;
			}
		}

		
		
		/***** step-3 --> When Idempotent remove new duplicates record ******/
		if (meta.isIdempotent()) {
			Iterator<SatRecord> iterSat = data.getBufferSatHistRows().iterator();
			while (iterSat.hasNext()) {
			    SatRecord satRow = iterSat.next();
			    if (satRow.isPersisted()){
			    	continue;
			    }
			    SatRecord prevRow = data.getBufferSatHistRows().lower(satRow);
			    
				if ((prevRow != null) && !(prevRow.getTechkeyValue().equals(satRow.getTechkeyValue()))) {
					prevRow =  null;
				}

				if (prevRow != null){
					//remove when previous is identical
					if (prevRow.equalsNoCheckOnDate(satRow)){ 
						iterSat.remove();	
					} 
				} else {
				//satRow is very 1st record
					SatRecord nextRow = data.getBufferSatHistRows().higher(satRow); 
					//Exceptionally there was previously a first record with same value!!
					// At this point, cannot "rebuild the past" so keep the original 1st record 
					if (nextRow != null && nextRow.equalsNoCheckOnDate(satRow)) {
						iterSat.remove();
					}
				}
			}
		}
		

		/***** step-4 --> Prepare Batch insert & Set ToDate if needed ******/
		//JDBC may not support simultaneous addBatch() on different prepareStmt 
		//so we store params for sat updates
		List<Object[]> updateParams = null;
		if (meta.isToDateColumnUsed()) {
			updateParams = new ArrayList<Object[]>();
		}
		int insertCtn = 0;
		
		for (SatRecord rec : data.getBufferSatHistRows()){
			SatRecord nextRec = data.getBufferSatHistRows().higher(rec);
			if (nextRec != null && !nextRec.getTechkeyValue().equals(rec.getTechkeyValue())){
				nextRec = null;
			}
			Object optToDate = null;
			//batch insert new record
			if (!rec.isPersisted()){
				if (meta.isToDateColumnUsed()){
					optToDate = (nextRec == null) ? data.toDateMaxFlag : 
						nextRec.getValues()[data.posFromDate];	
				}
				data.addBatchInsert(meta, rec, optToDate);
				insertCtn++;
				incrementLinesOutput();
			} else {
			//for existing record followed by a new one, add param to update (when needed)
				if (meta.isToDateColumnUsed() && nextRec != null && !nextRec.isPersisted()){
					updateParams.add(new Object[]{nextRec.getValues()[data.posFromDate] 
							,rec.getTechkeyValue()
							,rec.getValues()[data.posFromDate] });
				}
			}
		}
		

		/***** step-5 --> Complete Stmt batch, commit and send rows ....   *****/
		if (insertCtn > 0){
			//sat rows have "toDate" and require updates
			boolean requireUpdate = (updateParams != null && updateParams.size() > 0); 
			
			//execute Batch insert for all new Rows
			data.executeBatch(data.getPrepStmtInsertSat(),insertCtn);

			if (requireUpdate ){
				for (Object[] p : updateParams){
					data.addBatchUpdateStmt(p);
					incrementLinesUpdated();
				}
				data.executeBatch(data.getPrepStmtUpdateSat(),updateParams.size());
			}
		} 

		//Reach the point where all rows are "in-synch" in DB
		data.db.commit();

		//finished inserting sat record, can flush buffer 
		for (Object[] r : data.getBufferRows()) {
			putRow(data.outputRowMeta, r);
		}
		data.emptyBuffersAndClearPrepStmts();
		
		
		/***** step-6 --> Continue processing or Exit if no more rows expected *****/
		if (!data.finishedAllRows) {
			return true;
		} else {
			setOutputDone();
			return false;
		}
	}
	
	
	private void initializeWithFirstRow() throws KettleStepException, KettleDatabaseException {
		//safer to clone rowMeta although no change is done 
		data.outputRowMeta = getInputRowMeta().clone();
 
		data.initializeRowProcessing(meta);
		data.initPrepStmtLookup(meta);
		data.initPrepStmtInsert(meta);
		if (meta.isToDateColumnUsed()) {
			data.initPrepStmtUpdate(meta);
		}
		meta.getFields( data.outputRowMeta, getStepname(), null, null, this, repository, metaStore );
		
	}

	
	public boolean init(StepMetaInterface sii, StepDataInterface sdi) {
		
		if (super.init(sii, sdi)) {
			if (meta.getDatabaseMeta() == null) {
				logError(BaseMessages.getString(PKG, "Load.Init.ConnectionMissing", getStepname()));
				return false;
			}

			data.db = new Database(this, meta.getDatabaseMeta());
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
		meta = (LoadSatMeta) smi;
		data = (LoadSatData) sdi;

		if (data.db != null) {
			try {
				if (getErrors() == 0) {
					data.db.commit();
				} else {
					data.db.rollback();
				}
				data.db.closePreparedStatement(data.getPrepStmtLookup());
				data.db.closePreparedStatement(data.getPrepStmtInsertSat());
				data.db.closePreparedStatement(data.getPrepStmtUpdateSat());	
			} catch (KettleDatabaseException e) {
				logError(BaseMessages.getString(PKG, "Load.Log.UnexpectedError") + " : " + e.toString());
			} finally {
				data.db.disconnect();
			}
		}
		super.dispose(smi, sdi);
	}

}
