package plugin.mo.trans.steps.loadlink;

import java.util.List;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.shared.SharedObjectInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import plugin.mo.trans.steps.common.CompositeValues;
import plugin.mo.trans.steps.loadhub.LoadHubMeta;
import plugin.mo.trans.steps.loadsat.LoadSatMeta;
import plugin.mo.trans.steps.loadsat.ui.LoadSatDialog;

/**
 * 
 * TODO: 
 * -
 * 
 * 
 * @author mouellet
 *
 */


public class LoadLinkMeta extends BaseStepMeta implements StepMetaInterface {
	private static Class<?> PKG = CompositeValues.class;
	
	private DatabaseMeta databaseMeta;

	private String schemaName; 
	private String linkTable;
	
	// buffer & fetch size
	private int bufferSize;

	
	// key field(s) used for look up a link?
	private String[] keyField;

	// col(s) holding look-up key(s) in link table
	private String[] keyCol;

	// column holding the PKey of Link
	private String primaryKeyCol;
	//key added in output stream to avoid name conflict 
	private String newKeyFieldName;
	
	// type (identifying key or other)
	private String[] attType;
		
	private String creationDateCol;
	
	//method used to generate keys
	private String keyCreation;
	// if using sequence 
	private String sequenceName;
	
	
	public LoadLinkMeta() {
		super();
	}
		
	
	@Override
	public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
			TransMeta transMeta, Trans trans) {
		return new LoadLink(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	
	@Override
	public StepDataInterface getStepData() {
		return new LoadHubOrLinkData();
	}

	public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name) {
		return null; // new LoadSatDialog(shell, meta, transMeta, name);
	}
	
	
	@Override
	public void setDefault() {
		schemaName = "";
		linkTable = "";
		databaseMeta = null;
		bufferSize = LoadHubMeta.MIN_BUFFER_SIZE;
		
		int nrkeys = 2;

		allocateKeyArray(nrkeys);

		for (int i = 1; i < nrkeys; i++) {
			keyField[i-1] = "key" + i;
			keyCol[i-1] = "keylookup" + i;
		}
		
		
	}

	public void allocateKeyArray(int nrkeys) {
		keyField = new String[nrkeys];
		keyCol = new String[nrkeys];
	}

	
	/**
	 * Used to modify the stream fields meta to reflect this Step changes
	 */
	public void getFields(RowMetaInterface row, String origin, RowMetaInterface[] info, StepMeta nextStep,
			VariableSpace space, Repository repository, IMetaStore metaStore) throws KettleStepException {

		ValueMetaInterface v = new ValueMetaInteger(getNewKeyFieldName());
		v.setLength(10);
		v.setPrecision(0);
		v.setOrigin(origin);
		// append the surrogate key to the stream fields
		row.addValueMeta(v);
		
	}

	// return the XML holding all meta-setting
		public String getXML() throws KettleException {
			StringBuffer retval = new StringBuffer(512);
			retval.append("  ").append(
					XMLHandler.addTagValue("connection", databaseMeta == null ? "" : databaseMeta.getName()));
			retval.append("  ").append(XMLHandler.addTagValue("schemaName", schemaName));
			retval.append("  ").append(XMLHandler.addTagValue("linkTable", linkTable));
			retval.append("  ").append(XMLHandler.addTagValue("batchSize", bufferSize));

			//...
			retval.append("  ").append(XMLHandler.addTagValue("creation_method", keyCreation));
			retval.append("  ").append(XMLHandler.addTagValue("sequenceFrom", sequenceName));
			
			return retval.toString();
		}
	

		public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {
			readData(stepnode, databases);
		}
	
		private void readData(Node stepnode, List<? extends SharedObjectInterface> databases) throws KettleXMLException {
			try {
				String con = XMLHandler.getTagValue(stepnode, "connection");
				databaseMeta = DatabaseMeta.findDatabase(databases, con);
				schemaName = XMLHandler.getTagValue(stepnode, "schemaName");
				linkTable = XMLHandler.getTagValue(stepnode, "linkTable");

				String bSize;
				bSize = XMLHandler.getTagValue(stepnode, "batchSize");
				bufferSize = Const.toInt(bSize, 0);
				
				Node keys = XMLHandler.getSubNode(stepnode, "fields");
				int nrkeys = XMLHandler.countNodes(keys, "key");
				// allocateArray(nrkeys);

				keyCreation = XMLHandler.getTagValue(stepnode, "creation_method");
				sequenceName = XMLHandler.getTagValue(stepnode, "sequenceFrom");
			
			} catch (Exception e) {
				throw new KettleXMLException(BaseMessages.getString(PKG, "LoadHubMeta.Exception.UnableToLoadStepInfo"), e);
			}
		}

		public void readRep(Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases)
				throws KettleException {
			try {
				databaseMeta = rep.loadDatabaseMetaFromStepAttribute(id_step, "id_connection", databases);

				schemaName = rep.getStepAttributeString(id_step, "schemaName");
				linkTable = rep.getStepAttributeString(id_step, "hubTable");
				bufferSize = (int) rep.getStepAttributeInteger(id_step, "batchSize");
				
				// ...
				
				keyCreation = rep.getStepAttributeString(id_step, "creation_method");
				sequenceName = rep.getStepAttributeString(id_step, "sequenceFrom");
				
			} catch (Exception e) {
				throw new KettleException(BaseMessages.getString(PKG,
						"LoadHubMeta.Exception.UnexpectedErrorWhileReadingStepInfo"), e);
			}
		}
		
		public void saveRep(Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step)
				throws KettleException {
			try {
				rep.saveDatabaseMetaStepAttribute(id_transformation, id_step, "id_connection", databaseMeta);
				// this to save the step-database Id
				if (databaseMeta != null) {
					rep.insertStepDatabase(id_transformation, id_step, databaseMeta.getObjectId());
				}

				rep.saveStepAttribute(id_transformation, id_step, "schemaName", schemaName);
				rep.saveStepAttribute(id_transformation, id_step, "linkTable", linkTable);
				rep.saveStepAttribute(id_transformation, id_step, "batchSize", bufferSize);
				
				//..
				rep.saveStepAttribute(id_transformation, id_step, "creation_method", keyCreation);
				rep.saveStepAttribute(id_transformation, id_step, "sequenceFrom", sequenceName);
				
			} catch (Exception e) {
				throw new KettleException(BaseMessages.getString(PKG, "LoadHubMeta.Exception.UnableToSaveStepInfo")
						+ id_step, e);
			}
			
		}
		
		
		public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
				RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
				Repository repository, IMetaStore metaStore) {
			CheckResult cr;
			String error_message = "";

			if (databaseMeta != null) {
				Database db = new Database(loggingObject, databaseMeta);
				try {
					db.connect();

					if (!Const.isEmpty(linkTable)) {
						boolean first = true;
						boolean error_found = false;
						
						String schemaLinkTable = databaseMeta.getQuotedSchemaTableCombination(schemaName, linkTable);
						RowMetaInterface linkRowMeta = db.getTableFields(schemaLinkTable);
						//to complete ...
						
					}
					
					//....
					

					if (keyCreation != null) {
						if (!(LoadHubMeta.CREATION_METHOD_AUTOINC.equals(keyCreation)
								|| LoadHubMeta.CREATION_METHOD_SEQUENCE.equals(keyCreation) || LoadHubMeta.CREATION_METHOD_TABLEMAX
									.equals(keyCreation))) {
							error_message += BaseMessages.getString(PKG, "LoadHubMeta.CheckResult.ErrorSurrKeyCreation")
									+ ": " + keyCreation + "!";
							cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
							remarks.add(cr);
						}
					}
					
					
				}  catch (KettleException e) {
					error_message = BaseMessages.getString(PKG, "LoadDialog.CheckResult.ErrorOccurred") + e.getMessage();
					cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
					remarks.add(cr);
				} finally {
					db.disconnect();
				}
				
			} else {
				error_message = BaseMessages.getString(PKG, "LoadDialog.CheckResult.InvalidConnection");
				cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
				remarks.add(cr);
			}
		}	
		
		
		public DatabaseMeta[] getUsedDatabaseConnections() {
			if (databaseMeta != null) {
				return new DatabaseMeta[] { databaseMeta };
			} else {
				return super.getUsedDatabaseConnections();
			}
		}
		
		
		public Object clone() {
			LoadLinkMeta retval = (LoadLinkMeta) super.clone();

			int nrkeys = keyField.length;
			retval.allocateKeyArray(nrkeys);

			// Deep copy for Array
			for (int i = 0; i < nrkeys; i++) {
				retval.keyField[i] = keyField[i];
				retval.keyCol[i] = keyCol[i];
			}		
			return retval;
		}
		
		public boolean equals(Object other) {
			if (other == this) {
				return true;
			}
			if (other == null || getClass() != other.getClass()) {
				return false;
			}

			LoadLinkMeta o = (LoadLinkMeta) other;

			if ((getSchemaName() == null && o.getSchemaName() != null)
					|| (getSchemaName() != null && o.getSchemaName() == null)
					|| (getSchemaName() != null && o.getSchemaName() != null && !getSchemaName().equals(o.getSchemaName()))) {
				return false;
			}

			if ((getLinkTable() == null && o.getLinkTable() != null) || (getLinkTable() != null && o.getLinkTable() == null)
					|| (getLinkTable() != null && o.getLinkTable() != null && !getLinkTable().equals(o.getLinkTable()))) {
				return false;
			}

			return true;
		}
		
		
		public int getBufferSize() {
			return bufferSize;
		}
		
		/*
		 * Validate to avoid senseless BufferSize
		 */
		public void setBufferSize(int bSize) {
			if (bSize < LoadHubMeta.MIN_BUFFER_SIZE) {
				bufferSize = LoadHubMeta.MIN_BUFFER_SIZE;
			}
			bufferSize = bSize ;
		}
		
		
		public boolean isAutoIncrement() {
			return keyCreation.equals(LoadHubMeta.CREATION_METHOD_AUTOINC);
		}
		
		public boolean isTableMax() {
			return keyCreation.equals(LoadHubMeta.CREATION_METHOD_TABLEMAX);
		}
		

		public String getKeyCreation() {
			return keyCreation;
		}

		public void setKeyCreation(String techKeyCreation) {
			keyCreation = techKeyCreation;
		}

		
		
		public String getSchemaName() {
			return schemaName;
		}

		public void setSchemaName(String schemaName) {
			this.schemaName = schemaName;
		}

		public String getLinkTable() {
			return linkTable;
		}

		public void setLinkTable(String table) {
			this.linkTable = table;
		}


		public DatabaseMeta getDatabaseMeta() {
			return databaseMeta;
		}

		public void setDatabaseMeta(DatabaseMeta databaseMeta) {
			this.databaseMeta = databaseMeta;
		}


		public String[] getKeyField() {
			return keyField;
		}


		public void setKeyField(String[] keyField) {
			this.keyField = keyField;
		}


		public String[] getKeyCol() {
			return keyCol;
		}


		public void setKeyCol(String[] keyCol) {
			this.keyCol = keyCol;
		}


		public String getPrimaryKeyCol() {
			return primaryKeyCol;
		}

		//set as well newKeyFieldName with prefix table-name
		public void setPrimaryKeyCol(String primaryKeyCol) {
			this.primaryKeyCol = primaryKeyCol;
			newKeyFieldName = linkTable + "." + primaryKeyCol;
		}


		public String getNewKeyFieldName() {
			return newKeyFieldName;
		}


		public String getSequenceName() {
			return sequenceName;
		}


		public String getCreationDateCol() {
			return creationDateCol;
		}


		public void setCreationDateCol(String creationDateCol) {
			this.creationDateCol = creationDateCol;
		}


		public void setSequenceName(String sequenceName) {
			this.sequenceName = sequenceName;
		}
	
		
}

			