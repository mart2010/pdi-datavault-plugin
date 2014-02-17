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

import java.util.List;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
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
import plugin.mo.trans.steps.loadhub.ui.LoadHubDialog;

/**
 * 
 * TODO: 
 * -in Check() add vallidation and sql stuff for the optional NatKey table
 * - 
 * 
 * eventually move to annotated (but now both do not mix well)
 * 
 * @Step(id = "HL7Input", )
 * 
 * 
 * 
 * @Step(id="LoadHubAnchorPlugin",name="Hub/AnchorLoader",image="ui/images/DPL.png",
 *		description="LoadHubMeta.TypeLongDesc.HubLoader",categoryDescription="Experimental")
 *	
 *  
 *	
 */
public class LoadAnchorMeta extends BaseStepMeta implements StepMetaInterface {
	private static Class<?> PKG = LoadAnchor.class;
	
	public static int MAX_SUGG_BUFFER_SIZE = 5000;
	public static int MIN_BUFFER_SIZE = 100;
	
	public static String CREATION_METHOD_AUTOINC = "autoinc";
	public static String CREATION_METHOD_SEQUENCE = "sequence";
	public static String CREATION_METHOD_TABLEMAX = "tablemax";

	private String schemaName;
	private String hubTable;
	// equal to null if nat-keys stored inside hubTable
	private String natkeyTable;

	private DatabaseMeta databaseMeta;
	// determines the buffer & max commit size
	private int bufferSize;

	private boolean removeNatkeyFields;

	// which field(s) to use for look up an entity?
	private String[] natKeyField;

	// which col(s) hold the look-up key(s) in natkeyTable
	private String[] natKeyCol;

	/* optional Creation Date Audit col */
	private String creationDateCol;

	// column holding the surrogateKey in Hub
	private String surrPKeyColumn;

	// column holding the surrogateFK pointing to surrogateKey in Hub
	// equal to null if nat-keys stored inside hubTable
	private String surrFKeyInNatkeyTable;

	// When using sequence to generate the surrogateKey
	private String sequenceName;

	// Which method to use for the creation of surrogate key
	private String surrKeyCreation = null;

	public LoadAnchorMeta() {
		super();
	}

	// return the XML holding all meta-setting
	public String getXML() throws KettleException {
		StringBuffer retval = new StringBuffer(512);
		retval.append("  ").append(
				XMLHandler.addTagValue("connection", databaseMeta == null ? "" : databaseMeta.getName()));
		retval.append("  ").append(XMLHandler.addTagValue("schemaName", schemaName));
		retval.append("  ").append(XMLHandler.addTagValue("hubTable", hubTable));
		retval.append("  ").append(XMLHandler.addTagValue("natkeyTable", natkeyTable));
		retval.append("  ").append(XMLHandler.addTagValue("batchSize", bufferSize));

		retval.append("  <fields>").append(Const.CR);
		for (int i = 0; i < natKeyField.length; i++) {
			retval.append("   <key>").append(Const.CR);
			retval.append("   ").append(XMLHandler.addTagValue("field_key", natKeyField[i]));
			retval.append("   ").append(XMLHandler.addTagValue("col_lookup", natKeyCol[i]));
			retval.append("   </key>").append(Const.CR);
		}
		retval.append("  </fields>").append(Const.CR);

		retval.append("  ").append(XMLHandler.addTagValue("surrPKey", surrPKeyColumn));
		retval.append("  ").append(XMLHandler.addTagValue("surrFKey", surrFKeyInNatkeyTable));
		retval.append("  ").append(XMLHandler.addTagValue("creation_method", surrKeyCreation));
		retval.append("  ").append(XMLHandler.addTagValue("sequenceFrom", sequenceName));
		retval.append("  ").append(XMLHandler.addTagValue("removeNatkeyFields", removeNatkeyFields));
		retval.append("  ").append(XMLHandler.addTagValue("creationDate", creationDateCol));

		return retval.toString();
	}

	private void readData(Node stepnode, List<? extends SharedObjectInterface> databases) throws KettleXMLException {
		try {
			String con = XMLHandler.getTagValue(stepnode, "connection");
			databaseMeta = DatabaseMeta.findDatabase(databases, con);
			schemaName = XMLHandler.getTagValue(stepnode, "schemaName");
			hubTable = XMLHandler.getTagValue(stepnode, "hubTable");
			natkeyTable = XMLHandler.getTagValue(stepnode, "natkeyTable");
			String bSize;
			bSize = XMLHandler.getTagValue(stepnode, "batchSize");
			bufferSize = Const.toInt(bSize, 0);

			Node keys = XMLHandler.getSubNode(stepnode, "fields");
			int nrkeys = XMLHandler.countNodes(keys, "key");
			allocateKeyArray(nrkeys);
			// Read key mapping
			for (int i = 0; i < nrkeys; i++) {
				Node knode = XMLHandler.getSubNodeByNr(keys, "key", i);
				natKeyField[i] = XMLHandler.getTagValue(knode, "field_key");
				natKeyCol[i] = XMLHandler.getTagValue(knode, "col_lookup");
			}

			surrPKeyColumn = XMLHandler.getTagValue(stepnode, "surrPKey");
			surrFKeyInNatkeyTable = XMLHandler.getTagValue(stepnode, "surrFKey");
			surrKeyCreation = XMLHandler.getTagValue(stepnode, "creation_method");

			sequenceName = XMLHandler.getTagValue(stepnode, "sequenceFrom");
			removeNatkeyFields = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "removeNatkeyFields"));
			creationDateCol = XMLHandler.getTagValue(stepnode, "creationDate");

		} catch (Exception e) {
			throw new KettleXMLException(BaseMessages.getString(PKG, "LoadMeta.Exception.LoadCommomStepInfo"), e);
		}
	}

	public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {
		readData(stepnode, databases);
	}

	public void allocateKeyArray(int nrkeys) {
		natKeyField = new String[nrkeys];
		natKeyCol = new String[nrkeys];
	}

	/**
	 * Used to modify the stream fields meta to reflect this Step changes
	 */
	public void getFields(RowMetaInterface row, String origin, RowMetaInterface[] info, StepMeta nextStep,
			VariableSpace space, Repository repository, IMetaStore metaStore) throws KettleStepException {

		ValueMetaInterface v = new ValueMetaInteger(surrPKeyColumn);
		v.setLength(10);
		v.setPrecision(0);
		v.setOrigin(origin);
		// append the surrogate key to the stream fields
		row.addValueMeta(v);
		
		/*
		if (removeNatkeyFields) {
			for (int i = 0; i < natKeyField.length; i++) {
				int idx = row.indexOfValue(natKeyField[i]);
				if (idx >= 0) {
					row.removeValueMeta(idx);
				}
			}
		}
		*/
	}

	public Object clone() {
		LoadAnchorMeta retval = (LoadAnchorMeta) super.clone();
		int nrkeys = natKeyField.length;
		retval.allocateKeyArray(nrkeys);

		// Deep copy for Array
		for (int i = 0; i < nrkeys; i++) {
			retval.natKeyField[i] = natKeyField[i];
			retval.natKeyCol[i] = natKeyCol[i];
		}
		return retval;
	}

	public void setDefault() {
		schemaName = "";
		hubTable = BaseMessages.getString(PKG, "LoadHubMeta.HubTableName.Label");
		databaseMeta = null;
		bufferSize = MIN_BUFFER_SIZE;
		natkeyTable = null;
		removeNatkeyFields = false;
		int nrkeys = 0;

		allocateKeyArray(nrkeys);

		for (int i = 1; i < nrkeys; i++) {
			natKeyField[i-1] = "key" + i;
			natKeyCol[i-1] = "keylookup" + i;
		}
		surrPKeyColumn = "column in Anchor/Hub table";
		surrFKeyInNatkeyTable = "column in table holding natural key(s)";

	}

	public void readRep(Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases)
			throws KettleException {
		try {
			databaseMeta = rep.loadDatabaseMetaFromStepAttribute(id_step, "id_connection", databases);

			schemaName = rep.getStepAttributeString(id_step, "schemaName");
			hubTable = rep.getStepAttributeString(id_step, "hubTable");
			natkeyTable = rep.getStepAttributeString(id_step, "natkeyTable");
			bufferSize = (int) rep.getStepAttributeInteger(id_step, "batchSize");

			int nrkeys = rep.countNrStepAttributes(id_step, "field_key");
			allocateKeyArray(nrkeys);
			for (int i = 0; i < nrkeys; i++) {
				natKeyField[i] = rep.getStepAttributeString(id_step, i, "field_key");
				natKeyCol[i] = rep.getStepAttributeString(id_step, i, "col_lookup");
			}

			surrPKeyColumn = rep.getStepAttributeString(id_step, "surrPKey");
			surrFKeyInNatkeyTable = rep.getStepAttributeString(id_step, "surrFKey");
			surrKeyCreation = rep.getStepAttributeString(id_step, "creation_method");
			sequenceName = rep.getStepAttributeString(id_step, "sequenceFrom");
			removeNatkeyFields = rep.getStepAttributeBoolean(id_step, "removeNatkeyFields");
			creationDateCol = rep.getStepAttributeString(id_step, "creationDateCol");
		} catch (Exception e) {
			throw new KettleException(BaseMessages.getString(PKG,
					"LoadMeta.Exception.ErrorReadingCommonStepInfo"), e);
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
			rep.saveStepAttribute(id_transformation, id_step, "hubTable", hubTable);
			rep.saveStepAttribute(id_transformation, id_step, "natkeyTable", natkeyTable);
			rep.saveStepAttribute(id_transformation, id_step, "batchSize", bufferSize);

			for (int i = 0; i < natKeyField.length; i++) {
				rep.saveStepAttribute(id_transformation, id_step, i, "field_key", natKeyField[i]);
				rep.saveStepAttribute(id_transformation, id_step, i, "col_lookup", natKeyCol[i]);
			}

			rep.saveStepAttribute(id_transformation, id_step, "surrPKey", surrPKeyColumn);
			rep.saveStepAttribute(id_transformation, id_step, "surrFKey", surrFKeyInNatkeyTable);
			rep.saveStepAttribute(id_transformation, id_step, "creation_method", surrKeyCreation);
			rep.saveStepAttribute(id_transformation, id_step, "sequenceFrom", sequenceName);
			rep.saveStepAttribute(id_transformation, id_step, "removeNatkeyFields", removeNatkeyFields);
			rep.saveStepAttribute(id_transformation, id_step, "creationDate", creationDateCol);
		} catch (Exception e) {
			throw new KettleException(BaseMessages.getString(PKG, "LoadMeta.Exception.UnableToSaveCommonStepInfo")
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

				if (!Const.isEmpty(hubTable)) {
					boolean first = true;
					boolean error_found = false;
					
					String schemaHubTable = databaseMeta.getQuotedSchemaTableCombination(schemaName, hubTable);
					RowMetaInterface hubRowMeta = db.getTableFields(schemaHubTable);

					RowMetaInterface hubOrNatkeyRowMeta = hubRowMeta ;
					
					
					if (!Const.isEmpty(natkeyTable)) {
						String schemaNatkeyTable = databaseMeta.getQuotedSchemaTableCombination(schemaName, natkeyTable);
						hubOrNatkeyRowMeta = db.getTableFields(schemaNatkeyTable);
					}
						
					if ((hubRowMeta != null) || (hubOrNatkeyRowMeta != null)) {
						for (int i = 0; i < natKeyCol.length; i++) {
							String lufield = natKeyCol[i];

							ValueMetaInterface v = hubOrNatkeyRowMeta.searchValueMeta(lufield);
							if (v == null) {
								if (first) {
									first = false;
									error_message += BaseMessages.getString(PKG,
											"LoadDialog.CheckResult.MissingCompareColumns") + Const.CR;
								}
								error_found = true;
								error_message += "\t\t" + lufield + Const.CR;
							}
						}
						if (error_found) {
							cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
						} else {
							cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG,
									"LoadHubMeta.CheckResult.AllFieldsFound"), stepMeta);
						}
						remarks.add(cr);

						if (hubRowMeta.indexOfValue(surrPKeyColumn) < 0) {
							error_message = BaseMessages.getString(PKG, "LoadHubMeta.CheckResult.SurrogateKeyNotFound",
									surrPKeyColumn) + Const.CR;
							cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
							
						} else if (!Const.isEmpty(natkeyTable) && (hubOrNatkeyRowMeta.indexOfValue(surrFKeyInNatkeyTable) < 0)) {
							error_message = BaseMessages.getString(PKG, "LoadHubMeta.CheckResult.SurrogateKeyNotFound",
									surrFKeyInNatkeyTable) + Const.CR;
							cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
						} else {
							error_message = BaseMessages.getString(PKG, "LoadHubMeta.CheckResult.SurrogateKeyFound") + Const.CR;
							cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, error_message, stepMeta);
						}
						remarks.add(cr);
						
						if (!Const.isEmpty(creationDateCol) && hubRowMeta.indexOfValue(creationDateCol) < 0) {
							error_message = BaseMessages.getString(PKG, "LoadHubMeta.CheckResult.CreationDateColNotFound") + Const.CR;
							cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
							
						} else if (!Const.isEmpty(creationDateCol) && hubOrNatkeyRowMeta.indexOfValue(creationDateCol) < 0) {
							error_message = BaseMessages.getString(PKG, "LoadHubMeta.CheckResult.CreationDateColNotFound") + Const.CR;
							cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
						} else {
							error_message = BaseMessages.getString(PKG, "LoadHubMeta.CheckResult.CreationDateFound") + Const.CR;
							cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, error_message, stepMeta);
						}
						remarks.add(cr);
					} else {
						error_message = BaseMessages.getString(PKG, "LoadDialog.CheckResult.CouldNotReadTableInfo");
						cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
						remarks.add(cr);
					}
				}

				if (bufferSize > MAX_SUGG_BUFFER_SIZE){
					error_message = BaseMessages.getString(PKG, "LoadDialog.CheckResult.BufferSize") + Const.CR;
					cr = new CheckResult(CheckResultInterface.TYPE_RESULT_WARNING, error_message, stepMeta);
					remarks.add(cr);
				}
				
				// Look up fields in the input stream <prev>
				if (prev != null && prev.size() > 0) {
					boolean first = true;
					error_message = "";
					boolean error_found = false;

					for (int i = 0; i < natKeyField.length; i++) {
						ValueMetaInterface v = prev.searchValueMeta(natKeyField[i]);
						if (v == null) {
							if (first) {
								first = false;
								error_message += BaseMessages.getString(PKG, "LoadDialog.CheckResult.MissingFields")
										+ Const.CR;
							}
							error_found = true;
							error_message += "\t\t" + natKeyField[i] + Const.CR;
						}
					}
					if (error_found) {
						cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
					} else {
						cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG,
								"LoadDialog.CheckResult.AllFieldsFoundInInputStream"), stepMeta);
					}
					remarks.add(cr);
				} else {
					error_message = BaseMessages.getString(PKG, "LoadDialog.CheckResult.CouldNotReadFields")
							+ Const.CR;
					cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
					remarks.add(cr);
				}

				// Check sequence
				if (databaseMeta.supportsSequences() && CREATION_METHOD_SEQUENCE.equals(getSurrKeyCreation())) {
					if (Const.isEmpty(sequenceName)) {
						error_message += BaseMessages.getString(PKG, "LoadHubMeta.CheckResult.ErrorNoSequenceName")
								+ "!";
						cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
						remarks.add(cr);
					} else {
						// No check on sequence name, if it's not filled in.
						// check out that... seems only work with Oracle?!?
						if (db.checkSequenceExists(sequenceName)) {
							error_message = BaseMessages.getString(PKG, "LoadHubMeta.CheckResult.ReadingSequenceOK",
									sequenceName);
							cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, error_message, stepMeta);
							remarks.add(cr);
						} else {
							error_message += BaseMessages
									.getString(PKG, "LoadHubMeta.CheckResult.ErrorReadingSequence")
									+ sequenceName
									+ "!";
							cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
							remarks.add(cr);
						}
					}
				}

				
				if (surrKeyCreation != null) {
					if (!(CREATION_METHOD_AUTOINC.equals(surrKeyCreation)
							|| CREATION_METHOD_SEQUENCE.equals(surrKeyCreation) || CREATION_METHOD_TABLEMAX
								.equals(surrKeyCreation))) {
						error_message += BaseMessages.getString(PKG, "LoadHubMeta.CheckResult.ErrorSurrKeyCreation")
								+ ": " + surrKeyCreation + "!";
						cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
						remarks.add(cr);
					}
				}
				
				
			} catch (KettleException e) {
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

		// See if we have input streams leading to this step
		if (input.length > 0) {
			cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG,
					"LoadDialog.CheckResult.ReceivingInfoFromOtherSteps"), stepMeta);
			remarks.add(cr);
		} else {
			cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(PKG,
					"LoadDialog.CheckResult.NoInputReceived"), stepMeta);
			remarks.add(cr);
		}
	}

	
	public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
			Trans trans) {
		return new LoadAnchor(stepMeta, stepDataInterface, cnr, transMeta, trans);
	}

	public StepDataInterface getStepData() {
		return new LoadAnchorData();
	}

	public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name) {
		return new LoadHubDialog(shell, meta, transMeta, name);
	}

	public DatabaseMeta[] getUsedDatabaseConnections() {
		if (databaseMeta != null) {
			return new DatabaseMeta[] { databaseMeta };
		} else {
			return super.getUsedDatabaseConnections();
		}
	}

	public boolean equals(Object other) {
		if (other == this) {
			return true;
		}
		if (other == null || getClass() != other.getClass()) {
			return false;
		}

		LoadAnchorMeta o = (LoadAnchorMeta) other;

		if (getBufferSize() != o.getBufferSize()) {
			return false;
		}
		if (!getSurrKeyCreation().equals(o.getSurrKeyCreation())) {
			return false;
		}
		if (isRemoveNatkeyFields() != o.isRemoveNatkeyFields()) {
			return false;
		}

		if ((getSequenceName() == null && o.getSequenceName() != null)
				|| (getSequenceName() != null && o.getSequenceName() == null)
				|| (getSequenceName() != null && o.getSequenceName() != null && !getSequenceName().equals(
						o.getSequenceName()))) {
			return false;
		}

		if ((getSchemaName() == null && o.getSchemaName() != null)
				|| (getSchemaName() != null && o.getSchemaName() == null)
				|| (getSchemaName() != null && o.getSchemaName() != null && !getSchemaName().equals(o.getSchemaName()))) {
			return false;
		}

		if ((getHubTable() == null && o.getHubTable() != null) || (getHubTable() != null && o.getHubTable() == null)
				|| (getHubTable() != null && o.getHubTable() != null && !getHubTable().equals(o.getHubTable()))) {
			return false;
		}

		if ((getSurrPKeyColumn() == null && o.getSurrPKeyColumn() != null)
				|| (getSurrPKeyColumn() != null && o.getSurrPKeyColumn() == null)
				|| (getSurrPKeyColumn() != null && o.getSurrPKeyColumn() != null && !getSurrPKeyColumn().equals(
						o.getSurrPKeyColumn()))) {
			return false;
		}

		// comparison missing but can be added later if required, like:
		// getNatKeyField()
		// getNatKeyCol()

		return true;
	}

	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	public String getHubTable() {
		return hubTable;
	}

	public void setHubTable(String anchorTable) {
		this.hubTable = anchorTable;
	}

	public String getNatkeyTable() {
		return natkeyTable;
	}

	public void setNatkeyTable(String natkeyTable) {
		this.natkeyTable = natkeyTable;
	}

	public DatabaseMeta getDatabaseMeta() {
		return databaseMeta;
	}

	public void setDatabaseMeta(DatabaseMeta databaseMeta) {
		this.databaseMeta = databaseMeta;
	}

	public int getBufferSize() {
		return bufferSize;
	}

	/*
	 * Validate to avoid senseless BufferSize
	 * 
	 */
	public void setBufferSize(int bSize) {
		if (bSize < MIN_BUFFER_SIZE) {
			bufferSize = MIN_BUFFER_SIZE;
		}
		bufferSize = bSize ;
	}

	public boolean isRemoveNatkeyFields() {
		return removeNatkeyFields;
	}

	public void setRemoveNatkeyFields(boolean replaceNatkeyFields) {
		this.removeNatkeyFields = replaceNatkeyFields;
	}

	public String[] getNatKeyField() {
		return natKeyField;
	}

	public String[] getNatKeyCol() {
		return natKeyCol;
	}

	public String getSurrPKeyColumn() {
		return surrPKeyColumn;
	}

	public void setSurrPKeyColumn(String surrogateKeyField) {
		this.surrPKeyColumn = surrogateKeyField;
	}

	public String getSequenceName() {
		return sequenceName;
	}

	public void setSequenceName(String sequence) {
		this.sequenceName = sequence;
	}

	public boolean isAutoIncrement() {
		return surrKeyCreation.equals(CREATION_METHOD_AUTOINC);
	}

	public String getSurrKeyCreation() {
		return surrKeyCreation;
	}

	public void setSurrKeyCreation(String techKeyCreation) {
		this.surrKeyCreation = techKeyCreation;
	}

	public String getSurrFKeyInNatkeyTable() {
		return surrFKeyInNatkeyTable;
	}

	public void setSurrFKeyInNatkeyTable(String surrFKeyInNatkeyTable) {
		this.surrFKeyInNatkeyTable = surrFKeyInNatkeyTable;
	}

	public String getCreationDateCol() {
		return creationDateCol;
	}

	public void setCreationDateCol(String creationDateCol) {
		this.creationDateCol = creationDateCol;
	}

}
