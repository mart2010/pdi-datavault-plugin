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
package plugin.mo.trans.steps.backup.loadanchor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;

import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowDataUtil;
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
 * Manages loading data in a Hub/Anchor by first looking up DB based on Natural
 * key(s) followed by loads of "new" rows (i.e. natural key(s) not found).
 * <p>
 * <p>
 * 1) Lookup composite natural key(s) in a "natkey" table
 * <p>
 * 2) If natural key(s) exist, append surrogate key and send downstream
 * <p>
 * 3) If not, insert new row, get/append surrogate key and send downstream
 * <p>
 * 4) Optionally replace natural key(s) fields from output.
 * <p>
 * 
 * TODO: 
 * -manage the lazy conversion case
 * -manage the multi-thread model.  In case, multiple instance runs asynchronously, what about all "new" 
 * -don't proceed with commit if running the transformation as a single database transaction (getTransMeta().isUsingUniqueConnections())
 * nat-key processed concurrently... may lead to insertion error (when unique constraint enabled) or worst to 
 * duplicates (when no constraint...).   Possible solution: 
 * 		1) running the transformation transactional, not sure but this could still cause issues when same natural keys are processed concurrently in two threads...  
 * 		2) handle the concurrency inside the Step logic by using some kind of thread-safe Collection to process stateful data concurently (instead of within a dedicated data class)
 * 		3) for now, don't support multi-threaded (ask for advice to pdi group.. not the 1st one: same issues with AnalyticQuery)
 * -create a set of test job with validation check based on the test files.
 *     4) split up the work differently and add synchronization in this class (refer to AggregateRows)
 *     5) IF THAT HAPPENS, WILL BE SIMPLY HAVE db insertion errors (violating unique constraint of NATKEYS), so we can simply ignore these!!!!
 *        however, this is still wrong for the case with Anchor + natkey tables !! 
 *  
 */
public class LoadAnchor extends BaseStep implements StepInterface {
	private static Class<?> PKG = LoadAnchor.class;

	private LoadAnchorData data;
	private LoadAnchorMeta meta;

	public LoadAnchor(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
		meta = (LoadAnchorMeta) getStepMeta().getStepMetaInterface();
		data = (LoadAnchorData) stepDataInterface;
	}

	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {

		// request new row (wait until available) & indicate busy!
		Object[] originalRow = getRow();
		Object[] rowNullAppended;
		boolean bufferNotFull = true;

		// initialize all stateful data on first row
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
		 * At this point: buffer is either full OR partially full but no more rows expected
		 *****/

		/***** step-1 --> Query DB and fill Lookup Map ******/

		
		
		// Setting values to the prepared Statement
		for (int i = 0; i < meta.getBufferSize(); i++) {
			Object[] p;
			// in case, we have a partial filled buffer
			try {
				p = data.getBufferRows().get(i);
			} catch (IndexOutOfBoundsException e) {
				p = null;
			}
			for (int j = 0; j < meta.getNatKeyCol().length; j++) {
				int cumIdx = (i * meta.getNatKeyCol().length) + (j + 1);
				data.db.setValue(data.getPrepStmtLookup(), data.getLookupRowMeta().getValueMeta(j),
						(p == null) ? null : p[data.getNatkeysRowIdx()[j]], cumIdx);
			}
		}

		// go fetch data in DB and populate the lookup Map
		ResultSet rs;
		try {
			rs = data.getPrepStmtLookup().executeQuery();
		} catch (SQLException e) {
			throw new KettleDatabaseException("Unable to execute Lookup query", e);
		}

		//populate buffer LookupMapping
		for (Object[] r : data.db.getRows(rs, meta.getBufferSize(), null)) {
			CompositeValues n = new CompositeValues(r,1,data.getNatkeysRowIdx().length);
			data.getBufferLookupMapping().put(n, (Long) r[0]);
		}

		
		/***** step-2 --> Handle existing: add Surr-key and remove from buffer *****/

		int surrKeyPos = getInputRowMeta().size();
		// using Iterator to remove safely existing natkey rows
		Iterator<Object[]> iter = data.getBufferRows().iterator();
		while (iter.hasNext()) {
			Object[] r = iter.next();
			CompositeValues n = new CompositeValues(r, data.getNatkeysRowIdx());
			Long sKey = data.getBufferLookupMapping().get(n);

			// Successful lookup, append surr-key
			// send downstream and remove from buffer
			if (sKey != null) {
				// 0-index based, so new appended surr-key
				r[surrKeyPos] = sKey;
				putRow(data.outputRowMeta, r);
				incrementLinesSkipped();
				iter.remove();
			}
		}

		// If all rows were found, job is finished
		if (data.getBufferRows().size() == 0) {
			data.initializeBuffers(meta.getBufferSize());
			data.clearPrepStatementLookup();
			if (!data.finishedAllRows) {
				return true;
			} else {
				setOutputDone();
				return false;
			}
		}

		/***** step-3 --> Handle new rows: Prepare stmt with added Surr-key and insert *****/

		Long surkey_val = null;
		if (meta.getSurrKeyCreation().equals(LoadAnchorMeta.CREATION_METHOD_TABLEMAX)) {
			surkey_val = data.getCurValueSurrKey();
		}

		for (int i = 0; i < data.getBufferRows().size(); i++) {
			Object[] curRow = data.getBufferRows().get(i);

			// There could be duplicates in new added rows as well
			CompositeValues newCompNatKeys = new CompositeValues(curRow, data.getNatkeysRowIdx());
			Long newKeysDup = data.getBufferLookupMapping().get(newCompNatKeys);
			
			// Duplicate found, skip this iteration 
			// DON't send downstream yet (done at end)
			if (newKeysDup != null) {
				curRow[surrKeyPos] = newKeysDup;
				continue;
			}

			// Set the surrogate-key
			if (meta.getSurrKeyCreation().equals(LoadAnchorMeta.CREATION_METHOD_TABLEMAX)) {
				surkey_val++;
			} else if (meta.getSurrKeyCreation().equals(LoadAnchorMeta.CREATION_METHOD_SEQUENCE)) {
				surkey_val = data.db.getNextSequenceValue(data.getRealSchemaName(), meta.getSequenceName(),
						meta.getSurrPKeyColumn());
			} else {
				// if using Auto-increment, do nothing and leave surkey_val=null
			}

			int tmpSurPos = -1;

			/******************** Start with fix columns in HubTable *****************/

			// Relying on knowledge from initialization of RowMetaValue
			// Surrogate-key (not needed when auto-inc)
			tmpSurPos = data.getInsertHubRowMeta().indexOfValue(meta.getSurrPKeyColumn());
			if (tmpSurPos != -1) {
				data.db.setValue(data.getPrepStmtInsertHub(),
						data.getInsertHubRowMeta().getValueMeta(tmpSurPos), surkey_val, tmpSurPos + 1);
			}
			
			// need CreationDate ?
			tmpSurPos = data.getInsertHubRowMeta().indexOfValue(meta.getCreationDateCol());
			if (tmpSurPos != -1) {
				data.db.setValue(data.getPrepStmtInsertHub(),
						data.getInsertHubRowMeta().getValueMeta(tmpSurPos), data.getNowDate(), tmpSurPos + 1);
			}

			/******************** If applicable, continue with natkey fix columns in HubTable *****************/
			if (meta.getNatkeyTable() != null) {
				// Foreign surr-key always needed
				tmpSurPos = data.getInsertNatKeyRowMeta().indexOfValue(meta.getSurrFKeyInNatkeyTable());
				data.db.setValue(data.getPrepStmtInsertNatKey(),
						data.getInsertNatKeyRowMeta().getValueMeta(tmpSurPos), surkey_val, tmpSurPos + 1);

				// need CreationDate ?
				tmpSurPos = data.getInsertNatKeyRowMeta().indexOfValue(meta.getCreationDateCol());
				if (tmpSurPos != -1) {
					data.db.setValue(data.getPrepStmtInsertNatKey(), data.getInsertNatKeyRowMeta()
							.getValueMeta(tmpSurPos), data.getNowDate(), tmpSurPos + 1);
				}
			}

			/*******
			 * Continue with natkey Columns in either HubTable or NatKeyColTable
			 *******/
			for (int j = 0; j < meta.getNatKeyCol().length; j++) {
				if (meta.getNatkeyTable() == null) {
					tmpSurPos = data.getInsertHubRowMeta().indexOfValue(meta.getNatKeyCol()[j]);
					data.db.setValue(data.getPrepStmtInsertHub(),
							data.getInsertHubRowMeta().getValueMeta(tmpSurPos), curRow[data.getNatkeysRowIdx()[j]],
							tmpSurPos + 1);
				}
				// Use separate Natkey table
				else {
					tmpSurPos = data.getInsertNatKeyRowMeta().indexOfValue(meta.getNatKeyCol()[j]);

					data.db.setValue(data.getPrepStmtInsertNatKey(), data.getInsertNatKeyRowMeta()
							.getValueMeta(tmpSurPos), curRow[data.getNatkeysRowIdx()[j]], tmpSurPos + 1);
				}
			}

			//JDBC call takes 30ms, whereas the remaining takes 0ms!!!!!..
			//logBasic("before time : " + System.currentTimeMillis() );
			//TODO: handle the insert execute myself, so could ignore the Unique constraint violation!!! (see Multi-thread comments on top)
			data.db.insertRow(data.getPrepStmtInsertHub(), false, false);
			
			//logBasic("after time : " + System.currentTimeMillis() );
			incrementLinesOutput();
			
			
			if (log.isRowLevel()) {
				logRowlevel("Written row: " + Arrays.deepToString(curRow));
			} 

			// Optionally insert Natkey no commit (wait after buffer)
			if (meta.getNatkeyTable() != null) {
				//TODO: handle the insert execute myself, so could ignore the Unique constraint violation!!! (see Multi-thread comments on top)
				data.db.insertRow(data.getPrepStmtInsertNatKey(), false, false);
			}

			
			// returned Auto-increment surrogate-key
			if (meta.isAutoIncrement()) {
				ResultSet keys = null;
				try {
					keys = data.getPrepStmtInsertHub().getGeneratedKeys();
					if (keys.next()) {
						surkey_val = Long.valueOf(keys.getLong(1));
					} else {
						throw new KettleDatabaseException("Unable to retrieve auto-increment Hub key : "
								+ meta.getSurrPKeyColumn() + ", no key in resultset");
					}
				} catch (SQLException ex) {
					throw new KettleDatabaseException("Unable to retrieve auto-increment Hub key : "
							+ meta.getSurrPKeyColumn(), ex);
				} finally {
					try {
						if (keys != null) {
							keys.close();
						}
					} catch (SQLException ex) {
						throw new KettleDatabaseException("Unable to close key ResultSet: " + meta.getSurrPKeyColumn(),
								ex);
					}
				}
			}
			
			// append the generated surr-key in row
			curRow[surrKeyPos] = surkey_val;

			// keep refreshing LookUpMap to manage duplicates among new rows
			data.getBufferLookupMapping().put(newCompNatKeys, surkey_val);

		}

		/***** step-4 --> Perform commit, send downstream & re-init buffer *****/
		//don't check db.isAutocommit (based on commitSize never set..)
		data.db.commit();

		for (Object[] r : data.getBufferRows()) {
			putRow(data.outputRowMeta, r);
		}

		data.initializeBuffers(meta.getBufferSize());
		data.clearPrepStatementLookup();

		// if needed, set surr-key to current
		if (meta.getSurrKeyCreation().equals(LoadAnchorMeta.CREATION_METHOD_TABLEMAX)) {
			data.setCurValueSurrKey(surkey_val);
		}

		/***** step-5 --> Continue processing or Exit if no more rows expected *****/
		if (!data.finishedAllRows) {
			return true;
		} else {
			setOutputDone();
			return false;
		}
	}


	
	private void initializeWithFirstRow() throws KettleStepException, KettleDatabaseException {

		data.outputRowMeta = getInputRowMeta().clone();
		meta.getFields(data.outputRowMeta, getStepname(), null, null, this, repository, metaStore);

		//must initialize buffer and size
		data.initializeBuffers(meta.getBufferSize());
		
		// Initialize the indexes of each nat-key values...
		data.initNatkeysRowIdx(meta, getInputRowMeta());

		// Initialize the boolean index to help remove nat-keys
		data.initRemoveFieldIndex(meta, getInputRowMeta());

		// initialize all PreparedStmt
		data.initPrepStmtLookup(meta, getInputRowMeta());
		data.initPrepStmtInsert(meta, getInputRowMeta());
		

		// Optional Initialization: max surrogate (when applicable)
		// this part should be synchronized?!!
		if (meta.getSurrKeyCreation().equals(LoadAnchorMeta.CREATION_METHOD_TABLEMAX)) {
			// Method "getOneRow(string sql)" is screwed up as it changes the
			// metaRow instance variable
			// in the database object !!! This impacts later call done on this
			// object.
			// Replaced by simple PrepareStmt & ResultSet
			String sqlMax = "SELECT " + " MAX(" + data.db.getDatabaseMeta().quoteField(meta.getSurrPKeyColumn()) + ") "
					+ "FROM " + data.getQualifiedHubTable();
			Statement stmtMax = null;
			try {
				stmtMax = data.db.getConnection().createStatement();
				ResultSet maxrs = stmtMax.executeQuery(sqlMax);
				if (maxrs.next()) {
					// Set the persistent surrKey
					data.setCurValueSurrKey(maxrs.getLong(1));
				} else {
					throw new KettleDatabaseException("Unable to get first of ResultSet Max(surrogate-key)");
				}
				if (stmtMax != null)
					stmtMax.close();
			} catch (SQLException e) {
				throw new KettleDatabaseException(e);
			}
		}

	}

	public boolean init(StepMetaInterface sii, StepDataInterface sdi) {
		if (super.init(sii, sdi)) {
			if (meta.getDatabaseMeta() == null) {
				logError(BaseMessages.getString(PKG, "Load.Init.ConnectionMissing", getStepname()));
				return false;
			}

			//REMOVE AND DO AS THE STEP LOADLINK AND LOADHUBLINKDATA
			data.setRealSchemaName(meta.getDatabaseMeta(), meta.getSchemaName());
			data.setQualifiedHubTable(meta.getDatabaseMeta(), meta.getHubTable());

			// manage Natkey Table if needed, or leave it to null
			// as a special flag to imply we only have HubTable
			if (meta.getNatkeyTable() != null) {
				data.setQualifiedNatKeyTable(meta.getDatabaseMeta(), meta.getNatkeyTable());
			}

			// should encapsulate also?
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
				//Commit is done explicitly, so we want it 
				//not set to auto-commit! (when supported)
				data.db.setAutoCommit(false);

				return true;
			} catch (KettleDatabaseException dbe) {
				logError(BaseMessages.getString(PKG, "Load.Log.UnableToConnectDB") + dbe.getMessage());
			}
		}

		return false;
	}

	public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
		meta = (LoadAnchorMeta) smi;
		data = (LoadAnchorData) sdi;

		if (data.db != null) {
			try {
				if (getErrors() == 0) {
					data.db.commit();
				} else {
					data.db.rollback();
				}
				data.db.closePreparedStatement(data.getPrepStmtLookup());
				data.db.closePreparedStatement(data.getPrepStmtInsertHub());
				data.db.closePreparedStatement(data.getPrepStmtInsertNatKey());
			} catch (KettleDatabaseException e) {
				logError(BaseMessages.getString(PKG, "Load.Log.UnexpectedError") + " : " + e.toString());
			} finally {
				data.db.disconnect();
			}
		}
		super.dispose(smi, sdi);
	}

}
