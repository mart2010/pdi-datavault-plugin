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
package plugin.mo.trans.steps.loadsat;

import java.text.ParseException;
import java.text.SimpleDateFormat;
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
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.shared.SharedObjectInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import plugin.mo.trans.steps.common.BaseLoadMeta;
import plugin.mo.trans.steps.common.CompositeValues;
import plugin.mo.trans.steps.loadsat.ui.LoadSatDialog;

/**
 * 
 * TODO: 
 *
 */
@Step(id = "LoadSatAttPlugin", name = "LoadSatDialog.Shell.Title", description="LoadSatDialog.Shell.Desc", 
image = "sat.png", 	i18nPackageName="plugin.mo.trans.steps.common", 
categoryDescription="i18n:org.pentaho.di.trans.step:BaseStep.Category.Experimental")
public class LoadSatMeta extends BaseLoadMeta implements StepMetaInterface {
	private static Class<?> PKG = CompositeValues.class;
		
	public static String ATTRIBUTE_NORMAL = "Normal Attribute";
	public static String ATTRIBUTE_TEMPORAL = "From-Date Temporal";
	public static String ATTRIBUTE_FK = "Foreign-Key to Hub";
		
	public static String NA = "n.a.";
	public static String DEFAULT_MAX_DATE = "01-01-4000";
	public static String DATE_FORMAT = "dd-MM-yyyy";
	
		
	// column holding "FromDate" (mandatory, if temporal)
	// flag to null for immutable sat
	// map to the attCol[i] with type= ATTRIBUTE_TEMPORAL
	private String fromDateColumn;
	// map to the attCol[i] with type= ATTRIBUTE_SURR_FK
	private String fkColumn;
	
	// column holding the "ToDate" (null when not used)
	private String toDateColumn;

	// Max Date to flag active record (null when "ToDate" not used)
	private String toDateMaxFlag;

	private boolean isIdempotent;
	
	
	public LoadSatMeta() {
		super();
	}
	
	@Override
	public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
			Trans trans) {
		return new LoadSat(stepMeta, stepDataInterface, cnr, transMeta, trans);
	}
	@Override
	public StepDataInterface getStepData() {
		return new LoadSatData(getLog());
	}

	public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name) {
		return new LoadSatDialog(shell, meta, transMeta, name);
	}


	public void setDefault() {
		super.setDefault();
		
		int nrkeys = 2;
		allocateKeyArray(nrkeys);
		for (int i = 1; i < nrkeys; i++) {
			fields[i-1] = "field-" + i;
			cols[i-1] = "foreignKey-" + i;
			types[i-1] = ATTRIBUTE_FK;
		}

		toDateMaxFlag = DEFAULT_MAX_DATE;
		toDateColumn = NA;
		isIdempotent = true;
	}

	/**
	 * No modification here.
	 */
	public void getFields(RowMetaInterface row, String origin, RowMetaInterface[] info, StepMeta nextStep,
			VariableSpace space, Repository repository, IMetaStore metaStore) throws KettleStepException {
	}

	// return the XML holding all meta-setting
	public String getXML() throws KettleException {
		String base = super.getXML();
		
		StringBuffer retval = new StringBuffer(100);
		retval.append("  ").append(XMLHandler.addTagValue("idempotent", isIdempotent));
		retval.append("  ").append(XMLHandler.addTagValue("toDateColumn", toDateColumn));
		retval.append("  ").append(XMLHandler.addTagValue("toDateMaxFlag", toDateMaxFlag));
		return base + retval.toString();
	}
	
	public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {
		this.readData(stepnode, databases);
	}

	protected void readData(Node stepnode, List<? extends SharedObjectInterface> databases) throws KettleXMLException {
		try {
			super.readData(stepnode, databases);
			isIdempotent = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "idempotent"));
			toDateColumn = XMLHandler.getTagValue(stepnode, "toDateColumn");
			toDateMaxFlag = XMLHandler.getTagValue(stepnode, "toDateMaxFlag");
			updateFkAndFromDate();
		} catch (Exception e) {
			throw new KettleXMLException(BaseMessages.getString(PKG, "LoadSatMeta.Exception.LoadStepInfo"), e);
		}
	}

	
	public void readRep(Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases)
			throws KettleException {
		try {
			super.readRep(rep, metaStore, id_step, databases);
			isIdempotent = rep.getStepAttributeBoolean(id_step, "idempotent");
			toDateColumn = rep.getStepAttributeString(id_step, "toDateColumn");
			toDateMaxFlag = rep.getStepAttributeString(id_step, "toDateMaxFlag");
			updateFkAndFromDate();
		} catch (Exception e) {
			throw new KettleException(BaseMessages.getString(PKG,
					"LoadSatMeta.Exception.ErrorReadingLinkStepInfo"), e);
		}
	}

	public void saveRep(Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step)
			throws KettleException {
		try {
			super.saveRep(rep, metaStore, id_transformation, id_step);
			rep.saveStepAttribute(id_transformation, id_step, "idempotent", isIdempotent);
			rep.saveStepAttribute(id_transformation, id_step, "toDateColumn", toDateColumn);
			rep.saveStepAttribute(id_transformation, id_step, "toDateMaxFlag", toDateMaxFlag);
		} catch (Exception e) {
			throw new KettleException(BaseMessages.getString(PKG, "LoadSatMeta.Exception.UnableToSaveLinkStepInfo")
					+ id_step, e);
		}
	}
	
	private void updateFkAndFromDate(){
		for(int i = types.length -1; i >= 0 ; i--){
			if (types[i] != null && types[i].equals(ATTRIBUTE_FK)){
				fkColumn = cols[i]; 
			}
			if (types[i] != null &&  types[i].equals(ATTRIBUTE_TEMPORAL)){
				fromDateColumn = cols[i]; 
			}
		}
	}
	
	
	
	public Object clone() {
		LoadSatMeta retval = (LoadSatMeta) super.clone();
		int nr = fields.length;
		retval.allocateKeyArray(nr);

		// Deep copy for Array
		for (int i = 0; i < nr; i++) {
			retval.fields[i] = fields[i];
			retval.cols[i] = cols[i];
			retval.types[i] = types[i];
		}		
		return retval;
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

				if (!Const.isEmpty(targetTable)) {
					boolean first = true;
					boolean error_found = false;
					
					String schemaSatTable = databaseMeta.getQuotedSchemaTableCombination(schemaName, targetTable);
					RowMetaInterface satRowMeta = db.getTableFields(schemaSatTable);
					
					int fkfound = 0;
					int temporalfound = 0;
					int normalfound = 0;
					int unknownfound = 0;
					
					if (satRowMeta != null) {
						for (int i = 0; i < cols.length; i++) {
							String lufield = cols[i];
							if (types[i].equals(LoadSatMeta.ATTRIBUTE_FK)) {
								fkfound++;
							} else if (types[i].equals(LoadSatMeta.ATTRIBUTE_TEMPORAL)){
								temporalfound++;
							} else if (types[i].equals(LoadSatMeta.ATTRIBUTE_NORMAL)){
								normalfound++;
							} else {
								unknownfound++;
							}
							ValueMetaInterface v = satRowMeta.searchValueMeta(lufield);
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
									"LoadSatMeta.CheckResult.AllFieldsFound"), stepMeta);
						}
						remarks.add(cr);

						if (fkfound == 0){
							error_message += BaseMessages.getString(PKG,
									"LoadSatMeta.CheckResult.NoFKFieldsFound",LoadSatMeta.ATTRIBUTE_FK) + Const.CR;
							cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
							remarks.add(cr);
						} else if (fkfound > 1) {
							error_message += BaseMessages.getString(PKG,
									"LoadSatMeta.CheckResult.ManyFKFieldsFound",LoadSatMeta.ATTRIBUTE_FK) + Const.CR;
							cr = new CheckResult(CheckResultInterface.TYPE_RESULT_WARNING, error_message, stepMeta);
							remarks.add(cr);
						}
						
						if (temporalfound > 0) {
							if (temporalfound > 1) {
								error_message += BaseMessages.getString(PKG,
										"LoadSatMeta.CheckResult.ManyTempoFieldsFound",LoadSatMeta.ATTRIBUTE_TEMPORAL) + Const.CR;
								cr = new CheckResult(CheckResultInterface.TYPE_RESULT_WARNING, error_message, stepMeta);
								remarks.add(cr);
							}
							if (!Const.isEmpty(toDateColumn) ){
								SimpleDateFormat f = new SimpleDateFormat(LoadSatMeta.DATE_FORMAT);
								try {
									f.parse(toDateMaxFlag);
								} catch (ParseException e) {
									error_message += BaseMessages.getString(PKG,
											"LoadSatMeta.CheckResult.WrongDateFormat") + Const.CR;
									cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
									remarks.add(cr);
								}
							}
						}
						if (normalfound == 0) {
							error_message += BaseMessages.getString(PKG,
									"LoadSatMeta.CheckResult.NoNormalFieldFound") + Const.CR;
							cr = new CheckResult(CheckResultInterface.TYPE_RESULT_WARNING, error_message, stepMeta);
							remarks.add(cr);
						}
						if (unknownfound > 0) {
							error_message += BaseMessages.getString(PKG,
									"LoadSatMeta.CheckResult.UnknownFieldFound") + Const.CR;
							cr = new CheckResult(CheckResultInterface.TYPE_RESULT_WARNING, error_message, stepMeta);
							remarks.add(cr);
						}
					} else {
						error_message = BaseMessages.getString(PKG, "LoadDialog.CheckResult.CouldNotReadTableInfo");
						cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
						remarks.add(cr);
					}
				}
				
				if (bufferSize > BaseLoadMeta.MAX_SUGG_BUFFER_SIZE){
					error_message = BaseMessages.getString(PKG, "LoadDialog.CheckResult.BufferSize") + Const.CR;
					cr = new CheckResult(CheckResultInterface.TYPE_RESULT_WARNING, error_message, stepMeta);
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
								error_message += BaseMessages.getString(PKG, "LoadDialog.CheckResult.MissingFields")
										+ Const.CR;
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

	
	//not used with LoadLinkMeta
	public String getIdKeyTypeString() {
		return null;
	}
	//not used with LoadLinkMeta
	public String getOtherTypeString() {
		return null;
	}


	public String getFromDateColumn() {
		return fromDateColumn;
	}

	public void setFromDateColumn(String fromDateColumn) {
		this.fromDateColumn = fromDateColumn;
	}

	public boolean isToDateColumnUsed() {
		return ( !Const.isEmpty(fromDateColumn) && !Const.isEmpty(toDateColumn) 
						&& !toDateColumn.equals(LoadSatMeta.NA) );
	}

	
	public String getToDateColumn() {
		return toDateColumn;
	}

	public void setToDateColumn(String toDateColumn) {
		this.toDateColumn = toDateColumn;
	}


	public String getToDateMaxFlag() {
		return toDateMaxFlag;
	}

	public void setToDateMaxFlag(String toDateMaxFlag) {
		this.toDateMaxFlag = toDateMaxFlag;
	}

	
	public boolean isIdempotent() {
		return isIdempotent;
	}

	public void setIdempotent(boolean isIdempotent) {
		this.isIdempotent = isIdempotent;
	}
	
	public String getFkColumn() {
		return fkColumn;
	}

}
