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

		// Setting values to the prepared Statement
		// potentially move that one into "data.addToBufferRows" and step-1 would only set null when buffer not full.
		for (int i = 0; i < meta.getBufferSize(); i++) {
			Object[] r;

			try {
				r = data.getBufferRows().get(i);
			} catch (IndexOutOfBoundsException e) {
				// we have a partial filled buffer
				r = null;
			}
			Object key = (r != null) ? r[data.getSatAttsRowIdx()[data.posFk]] : null;
			data.db.setValue(data.getPrepStmtLookup(), data.getSatRowMeta().getValueMeta(data.posFk),key,i+1);
		}
		
		//final parameters (minDate) to limit historical sat rows
		if (data.posFromDate != -1){
			java.util.Date minDate = new Date(data.minDateBuffer);
			data.db.setValue(data.getPrepStmtLookup(), data.getSatRowMeta().getValueMeta(data.posFromDate)
					,minDate,meta.getBufferSize()+1);
		}
		
		// go fetch data in DB and populate satHistRows buffer
		ResultSet rs;
		try {
			rs = data.getPrepStmtLookup().executeQuery();
		} catch (SQLException e) {
			throw new KettleDatabaseException("Unable to execute Lookup query", e);
		}

		for (Object[] r : data.db.getRows(rs,0,null)) {
			CompositeValues v = new CompositeValues(r,0,meta.getAttField().length,data.posFk,data.posFromDate);
			//flag records coming from DB
			v.setAsPersisted();
			// records from DB have integrity so no duplicates expected
			if (! data.getBufferSatHistRows().add(v) ){
				logError("Check DB state, sat/att table has row duplicates: " + meta.getSatTable() );
			}
		}

		
		/***** step-2 --> Add new records in bufferSatHistRows, ignore & send downstream duplicates ******
		 *******          This guarantees sorting necessary for Idempotent & "toDate" handling ******/
		
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
				incrementLinesSkipped();
				//logError("attrappe un duplique !!!");
			}
		}
		
		
		/***** step-3 --> When Idempotent remove redundant record ******/
		
		if (meta.isIdempotent()) {
			Iterator<CompositeValues> iterSat = data.getBufferSatHistRows().iterator();
			while (iterSat.hasNext()) {
			    CompositeValues satRow = iterSat.next();
			    if (satRow.isPersisted()){
			    	continue;
			    }
				CompositeValues prevRow = data.getBufferSatHistRows().lower(satRow);
				//newRow is 1st sat record 
				if ((prevRow != null) && !(prevRow.getPkeyValue().equals(satRow.getPkeyValue()))) {
					prevRow =  null;
				}
				//Remove when previous is identical
				if (prevRow != null && prevRow.equalsIgnoreFromDate(satRow)){
					iter.remove();
					incrementLinesSkipped();
					logError("trouve idempotent row:" + satRow + " avec le meme prec=" + prevRow);
				}
			}
		}
		

		/***** step-4 --> Prepare Batch insert & Set ToDate if needed ******/
		
		//Cannot do simultaneous addBatch() on different prepareStmt 
		//need to preserve params for any Sat prevRow updates 
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
				addBatchToStmt(data.getPrepStmtInsertSat(), data.getSatRowMeta(), rec.getValues(), optToDate);
				insertCtn++;
				incrementLinesOutput();
			} else {
			//for existing, update toDate if needed and new record is found next
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
			executeBatch(data.getPrepStmtInsertSat(),data.getSatRowMeta(),insertCtn);
			
			//sat rows have "toDate" and require update
			if (updateParams != null && updateParams.size() > 0 ){
				for (Object[] p : updateParams){
					addBatchToStmt(data.getPrepStmtUpdateSat(),data.getUpToDateRowMeta(), p, null);
					incrementLinesUpdated();
				}
				executeBatch(data.getPrepStmtUpdateSat(),data.getUpToDateRowMeta(),updateParams.size());
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
		
		data.initializeBuffers(meta.getBufferSize());
		data.clearPrepStmts();

		
		
		/***** step-6 --> Continue processing or Exit if no more rows expected *****/
		if (!data.finishedAllRows) {
			return true;
		} else {
			setOutputDone();
			return false;
		}
	}

	/*
	 * TODO: Check out scenario where Batch would not be appropriate
	 * 
	 * ex. using unique Connection getTransMeta().isUsingUniqueConnections())?
	 * 
	 */
	private void addBatchToStmt(PreparedStatement stmt, RowMetaInterface rowMeta, 
						Object[] values, Object optionalToDate) throws KettleDatabaseException {
		try {
			data.db.setValues(rowMeta,values, stmt);
			
			//optionally set the toDate 
			if (optionalToDate != null){
				data.db.setValue(stmt, rowMeta.getValueMeta(data.posFromDate), optionalToDate ,values.length +1);
			}
			stmt.addBatch();
			//logError("inserting int batch: " + Arrays.deepToString(values));
		} catch ( SQLException ex ) {
		      throw new KettleDatabaseException( "Error adding \"batch\" for rowMeta: " + rowMeta.toStringMeta(), ex );
		} 
	}
	
	/*
	 * 
	 */
	private void executeBatch(PreparedStatement stmt, RowMetaInterface rowMeta, int updateCtnExpected) 
			throws KettleDatabaseException {
        try {
        	stmt. executeBatch();
	      	stmt.clearBatch();
          } catch ( BatchUpdateException ex ) {
        	  int[] nbUpd = ex.getUpdateCounts();
        	  if (updateCtnExpected == nbUpd.length){
        		  //jdbc driver continued processing all rows 
        		  logError("Batch Exception but JDBC driver processed all rows");
        		  if (!Const.isEmpty( data.db.getConnectionGroup())) {
        			 // logError("Ignore when running in parrallel -unique constraint violation expected");
        			  throw new KettleDatabaseException( ex );
        			  //return;
        		  } else {
        			 //logError("Continue processing, but check database integrity");
        			  throw new KettleDatabaseException( ex );
        			  //return;
        		  }
        	  } else {
        		  logError("Batch Exception and not all rows prcessed",ex);
        		  throw new KettleDatabaseException( ex ); 
        	  }
          } catch ( SQLException ex ) {
                  throw new KettleDatabaseException( "Unexpected error with batch", ex );
          }  
	}
		
	
	
	private void initializeWithFirstRow() throws KettleStepException, KettleDatabaseException {

		//probably safer to clone rowMeta to pass downstream
		data.outputRowMeta = getInputRowMeta().clone();
	    //although no change we still call getFields (ref. "InsertUpdate" step)
		meta.getFields( data.outputRowMeta, getStepname(), null, null, this, repository, metaStore );
		
		// Initialize the indexes of sat atts...
		data.initSatAttsRowIdx(meta);

		//initialize buffer and size
		data.initializeBuffers(meta.getBufferSize());

		// initialize all PreparedStmt
		data.initPrepStmtLookup(meta);
		data.initPrepStmtInsert(meta);
		if (!Const.isEmpty(meta.getToDateColumn())) {
			data.initPrepStmtUpdate(meta);
		}

		
	}

	public boolean init(StepMetaInterface sii, StepDataInterface sdi) {
		
		if (super.init(sii, sdi)) {
			if (meta.getDatabaseMeta() == null) {
				logError(BaseMessages.getString(PKG, "Load.Init.ConnectionMissing", getStepname()));
				return false;
			}

			data.setRealSchemaName(meta.getDatabaseMeta(), meta.getSchemaName());
			data.setQualifiedSatTable(meta.getDatabaseMeta(), meta.getSatTable());

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
