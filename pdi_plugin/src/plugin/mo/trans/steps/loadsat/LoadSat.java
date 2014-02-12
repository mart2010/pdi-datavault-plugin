package plugin.mo.trans.steps.loadsat;

import java.sql.BatchUpdateException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.exception.KettleDatabaseBatchException;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import plugin.mo.trans.steps.common.CompositeValues;

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
 * 6)
 * 
 * -)* Support of Reference/Knot values not explicit, but possible by doing 
 * 	   a Lookup Step upstream and using ref/knot key in mapping
 *       
 * 
 */
public class LoadSat extends BaseStep implements StepInterface {
	private static Class<?> PKG = CompositeValues.class;
	
	private LoadSatData data;
	private LoadSatMeta meta;

	public LoadSat(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
		meta = (LoadSatMeta) getStepMeta().getStepMetaInterface();
		data = (LoadSatData) stepDataInterface;
	}

	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {

		// request new row (wait until available) & indicate busy!
		Object[] originalRow = getRow();
		boolean bufferNotFull = true;

		if (first) {
			first = false;
			initializeWithFirstRow();
		}

		// Add current row to Buffer 
		if (originalRow != null) {
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
		 * From here: buffer is either full OR partially full but no more rows expected
		 *****/
		
		/***** step-1 --> Query DB and fill bufferSatHistRows  ******/

		int nb = data.populateLookupMap(meta, meta.getBufferSize());
		if (log.isDetailed()){
			logDetailed("Buffer filled, number of fetched sat history records from DB= " + nb);	
		}
		
		/***** step-2 --> Add new records in bufferSatHistRows, ignore & send downstream duplicates ******
		 *******          This guarantees proper sorting needed for Idempotent & "toDate"          ******/
		
		// using Iterator to remove safely unneeded rows
		Iterator<Object[]> iter = data.getBufferRows().iterator();
		while (iter.hasNext()) {
			Object[] bufferRow = iter.next();
			CompositeValues newRow = new CompositeValues(bufferRow,data.getSatAttsRowIdx(),
											data.posFkInRow,data.posFromDateInRow);
			
			if (!data.getBufferSatHistRows().add(newRow)){
				//ignore duplicate (e.g. dups in stream, or immutable attr..)
				putRow(data.outputRowMeta, bufferRow);
				iter.remove();
				//incrementLinesSkipped();
			}
		}
		// Finished if all buffer rows are duplicates 
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
			Iterator<CompositeValues> iterSat = data.getBufferSatHistRows().iterator();
			while (iterSat.hasNext()) {
			    CompositeValues satRow = iterSat.next();
			    if (satRow.isPersisted()){
			    	continue;
			    }
				CompositeValues prevRow = data.getBufferSatHistRows().lower(satRow);
				//newRow is very 1st record in history
				if ((prevRow != null) && !(prevRow.getPkeyValue().equals(satRow.getPkeyValue()))) {
					prevRow =  null;
				}
				//remove when previous is identical
				if (prevRow != null && prevRow.equalsIgnoreFromDate(satRow)){
					iterSat.remove();
					//incrementLinesSkipped();
				}
			}
		}
		

		/***** step-4 --> Prepare Batch insert & Set ToDate if needed ******/
		
		//No simultaneous addBatch() possible on different prepareStmt 
		//so must preserve params for sat updates 
		List<Object[]> updateParams = null;
		if (meta.isToDateColumnUsed()) {
			updateParams = new ArrayList<Object[]>();
		}
		int insertCtn = 0;
		
		for (CompositeValues rec : data.getBufferSatHistRows()){
			CompositeValues nextRec = data.getBufferSatHistRows().higher(rec);
			if (nextRec != null && !nextRec.getPkeyValue().equals(rec.getPkeyValue())){
				nextRec = null;
			}
			Object optToDate = null;
			//batch insert new record
			if (!rec.isPersisted()){
				if (meta.isToDateColumnUsed()){
					optToDate = (nextRec == null) ? data.toDateMaxFlag : nextRec.getFromDateValue() ;	
				}
				//ready to batch the newRow
				data.addBatchInsert(meta, rec.getValues(), optToDate);
				insertCtn++;
				incrementLinesOutput();
			} else {
			//for existing record followed by a new one, add param to update (when)
				if (meta.isToDateColumnUsed() && nextRec != null && !nextRec.isPersisted()){
					updateParams.add(new Object[]{nextRec.getFromDateValue()
							,rec.getPkeyValue()
							,rec.getFromDateValue() });
				}
			}
		}
		

		/***** step-5 --> Complete Stmt batch, send remaining rows & re-init buffer *****/
		
		if (insertCtn > 0){
			//execute Batch insert for all new Rows
			data.executeBatch(data.getPrepStmtInsertSat(),data.getInsertRowMeta(),insertCtn);
			
			//sat rows have "toDate" and require update
			if (updateParams != null && updateParams.size() > 0 ){
				for (Object[] p : updateParams){
					data.addBatchUpdateStmt(p);
					incrementLinesUpdated();
				}
				data.executeBatch(data.getPrepStmtUpdateSat(),data.getUpdateToDateRowMeta(),updateParams.size());
			}
			
			//Reach the point where all rows have safely landed in DB
			data.db.commit();
			
			//Send remaining rows that got inserted into DB
			for (Object[] r : data.getBufferRows()) {
				putRow(data.outputRowMeta, r);
			}
		}  else {
			if (data.getBufferRows().size() > 0 )
				throw new IllegalStateException("Buffer should be empty, check program logic");		
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
		//safer to clone rowMeta for passing downstream
		data.outputRowMeta = getInputRowMeta().clone();
	    //although no change we still call getFields (ref. "InsertUpdate" step)
		meta.getFields( data.outputRowMeta, getStepname(), null, null, this, repository, metaStore );
		
		// Initialize the data state 
		data.initializeRowProcessing(meta);
		// initialize all PreparedStmt
		data.initPrepStmtLookup(meta);
		data.initPrepStmtInsert(meta);
		if (!meta.isToDateColumnUsed()) {
			data.initPrepStmtUpdate(meta);
		}
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
