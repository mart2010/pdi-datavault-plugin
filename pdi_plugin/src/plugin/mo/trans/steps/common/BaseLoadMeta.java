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
package plugin.mo.trans.steps.common;

import java.util.List;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.shared.SharedObjectInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

/**
 * This meta base class contains attributes common to all Steps.
 * 
 * 
 * Subclass must implement: clone(), loadXML(), check(), getStep(), 
 * getStepData(), getDialog(), and complete the partial implementation of
 * reading/writing from/to meta-store. 
 * 
 * Subclass can also provide override and specialized attributes.
 * 
 * @author mouellet
 *
 */
public abstract class BaseLoadMeta extends BaseStepMeta implements StepMetaInterface {
	protected static Class<?> PKG = BaseLoadMeta.class;

	public static int MAX_SUGG_BUFFER_SIZE = 5000;	
	public static int MIN_BUFFER_SIZE = 50;
	
	public static String CREATION_METHOD_AUTOINC = "autoinc";
	public static String CREATION_METHOD_SEQUENCE = "sequence";
	public static String CREATION_METHOD_TABLEMAX = "tablemax";

	protected DatabaseMeta databaseMeta;
	protected String schemaName;
	protected String targetTable;
	protected int bufferSize;
	protected String[] fields;
	protected String[] cols;
	protected String[] types;
	protected String techKeyCol;
	protected String keyGeneration;
	protected String sequenceName;
	
	protected String auditDtsCol;
	protected String auditRecSourceCol;
	protected String auditRecSourceValue;
	
	//name of techKeyCol added to output stream for Hub & Link
	//For now, get() simply return 'table.techKeyCol' (could be changed)
	protected String newKeyFieldName;		

	
	public BaseLoadMeta(){
		super();
	}
	
	//for subclass to provide Col-type name used in UI
	public abstract String getIdKeyTypeString();
	public abstract String getOtherTypeString();
	
	
	
	public void setDefault() {
		schemaName = "";
		targetTable = "";
		databaseMeta = null;
		bufferSize = MIN_BUFFER_SIZE*10;
		//rest to be implemented by subclass 	
	}

	
	public void allocateKeyArray(int nrkeys) {
		fields = new String[nrkeys];
		cols = new String[nrkeys];
		types = new String[nrkeys];
	}

	/*
	 * This handles common info, specific is implemented by subclass
	 * picking up returned String... 
	 */
	public String getXML() throws KettleException {
		StringBuffer retval = new StringBuffer(512);
		retval.append("  ").append(
				XMLHandler.addTagValue("connection", databaseMeta == null ? "" : databaseMeta.getName()));
		retval.append("  ").append(XMLHandler.addTagValue("schemaName", schemaName));
		retval.append("  ").append(XMLHandler.addTagValue("targetTable", targetTable));
		retval.append("  ").append(XMLHandler.addTagValue("batchSize", bufferSize));

		retval.append("  <fields>").append(Const.CR);
		for (int i = 0; i < fields.length; i++) {
			retval.append("   <key>").append(Const.CR);
			retval.append("   ").append(XMLHandler.addTagValue("field", fields[i]));
			retval.append("   ").append(XMLHandler.addTagValue("col", cols[i]));
			retval.append("   ").append(XMLHandler.addTagValue("type", types[i]));
			retval.append("   </key>").append(Const.CR);
		}
		retval.append("  </fields>").append(Const.CR);

		retval.append("  ").append(XMLHandler.addTagValue("auditDts", auditDtsCol));
		retval.append("  ").append(XMLHandler.addTagValue("auditRecSrcCol", auditRecSourceCol));
		retval.append("  ").append(XMLHandler.addTagValue("auditRecSrcVal", auditRecSourceValue));
		return retval.toString();
	}

	/*
	 * This handles common info, specific is implemented by subclass 
	 */
	public void saveRep(Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step)
			throws KettleException {
		try {
			rep.saveDatabaseMetaStepAttribute(id_transformation, id_step, "id_connection", databaseMeta);
			// this saves the step-database Id
			if (databaseMeta != null) {
				rep.insertStepDatabase(id_transformation, id_step, databaseMeta.getObjectId());
			}
			rep.saveStepAttribute(id_transformation, id_step, "schemaName", schemaName);
			rep.saveStepAttribute(id_transformation, id_step, "targetTable", targetTable);
			rep.saveStepAttribute(id_transformation, id_step, "batchSize", bufferSize);
			
			for (int i = 0; i < fields.length; i++) {
				rep.saveStepAttribute(id_transformation, id_step, i, "field", fields[i]);
				rep.saveStepAttribute(id_transformation, id_step, i, "col", cols[i]);
				rep.saveStepAttribute(id_transformation, id_step, i, "type", types[i]);
			}
			rep.saveStepAttribute(id_transformation, id_step, "auditDts", auditDtsCol);
			rep.saveStepAttribute(id_transformation, id_step, "auditRecSrcCol", auditRecSourceCol);
			rep.saveStepAttribute(id_transformation, id_step, "auditRecSrcVal", auditRecSourceValue);
		} catch (Exception e) {
			throw new KettleException(BaseMessages.getString(PKG, "LoadMeta.Exception.UnableToSaveCommonStepInfo")
					+ id_step, e);
		}
	}

	/*
	 * This handles common info, specific is implemented by subclass 
	 */
	protected void readData(Node stepnode, List<? extends SharedObjectInterface> databases) throws KettleXMLException {
		try {
			String con = XMLHandler.getTagValue(stepnode, "connection");
			databaseMeta = DatabaseMeta.findDatabase(databases, con);
			schemaName = XMLHandler.getTagValue(stepnode, "schemaName");
			targetTable = XMLHandler.getTagValue(stepnode, "targetTable");
			String bSize;
			bSize = XMLHandler.getTagValue(stepnode, "batchSize");
			bufferSize = Const.toInt(bSize, 0);
			
			Node keys = XMLHandler.getSubNode(stepnode, "fields");
			int nrkeys = XMLHandler.countNodes(keys, "key");
			allocateKeyArray(nrkeys);
			// Read key mapping
			for (int i = 0; i < nrkeys; i++) {
				Node knode = XMLHandler.getSubNodeByNr(keys, "key", i);
				fields[i] = XMLHandler.getTagValue(knode, "field");
				cols[i] = XMLHandler.getTagValue(knode, "col");
				types[i] = XMLHandler.getTagValue(knode, "type");
			}
			auditDtsCol = XMLHandler.getTagValue(stepnode, "auditDts");
			auditRecSourceCol = XMLHandler.getTagValue(stepnode, "auditRecSrcCol");
			auditRecSourceValue = XMLHandler.getTagValue(stepnode, "auditRecSrcVal");
		} catch (Exception e) {
			throw new KettleXMLException(BaseMessages.getString(PKG, "LoadMeta.Exception.LoadCommomStepInfo"), e);
		}
	}

	/*
	 * This handles common info, specific is implemented by subclass 
	 */
	public void readRep(Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases)
			throws KettleException {
		try {
			databaseMeta = rep.loadDatabaseMetaFromStepAttribute(id_step, "id_connection", databases);

			schemaName = rep.getStepAttributeString(id_step, "schemaName");
			targetTable = rep.getStepAttributeString(id_step, "hubTable");
			bufferSize = (int) rep.getStepAttributeInteger(id_step, "batchSize");
			
			int nrkeys = rep.countNrStepAttributes(id_step, "key");
			allocateKeyArray(nrkeys);
			for (int i = 0; i < nrkeys; i++) {
				fields[i] = rep.getStepAttributeString(id_step, i, "field");
				cols[i] = rep.getStepAttributeString(id_step, i, "col");
				types[i] = rep.getStepAttributeString(id_step, i, "type");
			}
			auditDtsCol = rep.getStepAttributeString(id_step, "auditDts");
			auditRecSourceCol = rep.getStepAttributeString(id_step, "auditRecSrcCol");
			auditRecSourceValue = rep.getStepAttributeString(id_step, "auditRecSrcVal");
		} catch (Exception e) {
			throw new KettleException(BaseMessages.getString(PKG,
					"LoadMeta.Exception.ErrorReadingCommonStepInfo"), e);
		}
	}

	
	public DatabaseMeta[] getUsedDatabaseConnections() {
		if (databaseMeta != null) {
			return new DatabaseMeta[] { databaseMeta };
		} else {
			return super.getUsedDatabaseConnections();
		}
	}

	public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
			RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
			Repository repository, IMetaStore metaStore) {
		CheckResult cr;
		String error_message = "";
		
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
		
		// Look up fields in the input stream <prev>
		if (prev != null && prev.size() > 0) {
			boolean first = true;
			error_message = "";
			boolean error_found = false;
			for (int i = 0; i < fields.length; i++) {
				ValueMetaInterface v = prev.searchValueMeta(fields[i]);
				if (v == null) {
					if (first) {
						first = false;
						error_message += BaseMessages.getString(PKG, "LoadDialog.CheckResult.MissingFields") + Const.CR;
					}
					error_found = true;
					error_message += "\t\t" + fields[i] + Const.CR;
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

		//DB check on target table and columns
		if (databaseMeta == null) {
			error_message = BaseMessages.getString(PKG, "LoadDialog.CheckResult.InvalidConnection") + Const.CR;
			cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
			remarks.add(cr);
		} else {
			Database db = new Database(loggingObject, databaseMeta);
			try {
				db.connect();
				if (!Const.isEmpty(targetTable)) {
					boolean first = true;
					boolean error_found = false;
					
					String schemaTargetTable = databaseMeta.getQuotedSchemaTableCombination(schemaName, targetTable);
					RowMetaInterface targetRowMeta = db.getTableFields(schemaTargetTable);
					error_message = "";
					if (targetRowMeta != null){
						for (int i = 0; i < cols.length; i++) {
							String lucol = cols[i];
							ValueMetaInterface v = targetRowMeta.searchValueMeta(lucol);
							if (v == null) {
								if (first) {
									first = false;
									error_message += BaseMessages.getString(PKG,
											"LoadDialog.CheckResult.MissingCompareColumns") + Const.CR;
								}
								error_found = true;
								error_message += "\t\t" + lucol + Const.CR;
							}
						}
						if (error_found) {
							cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
						} else {
							cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG,
									"LoadMeta.CheckResult.AllFieldsFound"), stepMeta);
						}
						remarks.add(cr);
						
						if (!Const.isEmpty(techKeyCol)){
							if (targetRowMeta.searchValueMeta(techKeyCol) == null) {
								error_message = BaseMessages.getString(PKG,
										"LoadMeta.CheckResult.SurrogateKeyNotFound") + Const.CR;
								cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
								remarks.add(cr);
							}
						}
						
					} else {
						error_message = BaseMessages.getString(PKG, "LoadDialog.CheckResult.CouldNotReadTableInfo");
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
			
		}
		
		if (bufferSize > BaseLoadMeta.MAX_SUGG_BUFFER_SIZE){
			error_message = BaseMessages.getString(PKG, "LoadDialog.CheckResult.BufferSize") + Const.CR;
			cr = new CheckResult(CheckResultInterface.TYPE_RESULT_WARNING, error_message, stepMeta);
			remarks.add(cr);
		}

	}	
	
	/*
	 * TODO: verify if we need overriding equals() 
	 * most Steps do not do this!
	 * 
	 */
	public boolean equals(Object other) {
		if (other == this) {
			return true;
		}
		if (other == null || getClass() != other.getClass()) {
			return false;
		}

		BaseLoadMeta o = (BaseLoadMeta) other;
		if ((getSchemaName() == null && o.getSchemaName() != null)
				|| (getSchemaName() != null && o.getSchemaName() == null)
				|| (getSchemaName() != null && o.getSchemaName() != null && !getSchemaName().equals(o.getSchemaName()))) {
			return false;
		}
		if ((getTargetTable() == null && o.getTargetTable() != null) || (getTargetTable() != null && o.getTargetTable() == null)
				|| (getTargetTable() != null && o.getTargetTable() != null && !getTargetTable().equals(o.getTargetTable()))) {
			return false;
		}
		return true;
	}


	public DatabaseMeta getDatabaseMeta() {
		return databaseMeta;
	}


	public void setDatabaseMeta(DatabaseMeta databaseMeta) {
		this.databaseMeta = databaseMeta;
	}


	public String getSchemaName() {
		return schemaName;
	}


	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}


	public String getTargetTable() {
		return targetTable;
	}


	public void setTargetTable(String targetTable) {
		this.targetTable = targetTable;
	}


	public int getBufferSize() {
		return bufferSize;
	}

	public void setBufferSize(int bSize) {
		if (bSize < BaseLoadMeta.MIN_BUFFER_SIZE) {
			bufferSize = BaseLoadMeta.MIN_BUFFER_SIZE;
		}
		bufferSize = bSize ;
	}
	


	public String[] getFields() {
		return fields;
	}


	public void setFields(String[] fields) {
		this.fields = fields;
	}


	public String[] getCols() {
		return cols;
	}


	public void setCols(String[] cols) {
		this.cols = cols;
	}


	public String[] getTypes() {
		return types;
	}


	public void setTypes(String[] types) {
		this.types = types;
	}


	public String getTechKeyCol() {
		return techKeyCol;
	}


	public void setTechKeyCol(String techKeyCol) {
		this.techKeyCol = techKeyCol;
	}


	public String getKeyGeneration() {
		return keyGeneration;
	}


	public void setKeyGeneration(String keyGeneration) {
		this.keyGeneration = keyGeneration;
	}


	public String getSequenceName() {
		return sequenceName;
	}


	public void setSequenceName(String sequenceName) {
		this.sequenceName = sequenceName;
	}


	public String getAuditDtsCol() {
		return auditDtsCol;
	}


	public void setAuditDtsCol(String auditDtsCol) {
		this.auditDtsCol = auditDtsCol;
	}


	public String getAuditRecSourceCol() {
		return auditRecSourceCol;
	}


	public void setAuditRecSourceCol(String auditRecSourceCol) {
		this.auditRecSourceCol = auditRecSourceCol;
	}


	public String getAuditRecSourceValue() {
		return auditRecSourceValue;
	}


	public void setAuditRecSourceValue(String auditRecSourceValue) {
		this.auditRecSourceValue = auditRecSourceValue;
	}


	public String getNewKeyFieldName() {
		//returned default value
		if (newKeyFieldName == null){
			return targetTable + "." + techKeyCol;	
		} else {
			return newKeyFieldName;
		}
		
	}


	public void setNewKeyFieldName(String newKeyFieldName) {
		this.newKeyFieldName = newKeyFieldName;
	}
	

	public boolean isMethodAutoIncrement() {
		return CREATION_METHOD_AUTOINC.equals(keyGeneration);
	}

	public boolean isMethodTableMax() {
		return CREATION_METHOD_TABLEMAX.equals(keyGeneration);
	}

	
	
	
}
