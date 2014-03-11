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
package plugin.mo.trans.steps.loadsat;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.SQLStatement;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
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
import plugin.mo.trans.steps.ui.LoadSatDialog;

/**
 * Meta class used for Satellite.
 * 
 * @author mouellet
 */
@Step(id = "LoadSatAttPlugin", name = "LoadSatDialog.Shell.Title", description="LoadSatDialog.Shell.Desc", 
image = "sat.png", 	i18nPackageName="plugin.mo.trans.steps.common", 
categoryDescription="i18n:org.pentaho.di.trans.step:BaseStep.Category.Experimental")
public class LoadSatMeta extends BaseLoadMeta implements StepMetaInterface {
	
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
		super.check(remarks, transMeta, stepMeta, prev, input, output, info, space, repository, metaStore);

		CheckResult cr;
		String error_message = "";

		int fkfound = 0;
		int temporalfound = 0;
		int normalfound = 0;
		int unknownfound = 0;
	
		for (int i = 0; i < cols.length; i++) {
			if (types[i].equals(LoadSatMeta.ATTRIBUTE_FK)) {
				fkfound++;
			} else if (types[i].equals(LoadSatMeta.ATTRIBUTE_TEMPORAL)){
				temporalfound++;
			} else if (types[i].equals(LoadSatMeta.ATTRIBUTE_NORMAL)){
				normalfound++;
			} else {
				unknownfound++;
			}
		}
	
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
	}
				
	@Override
	public SQLStatement getSQLStatements(TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
			Repository repository, IMetaStore metaStore) throws KettleStepException {
		
		SQLStatement retval = super.getSQLStatements(transMeta, stepMeta, prev, repository, metaStore);
		if (retval.getError() != null){
			return retval;
		}
		
		Database db = new Database(loggingObject, databaseMeta);
		db.shareVariablesWith( transMeta );
		LoadSatData data = (LoadSatData) getStepData();
		data.db = db;
		try {
			db.connect();
			data.outputRowMeta = transMeta.getPrevStepFields( stepMeta.getName()).clone();
			data.initSatAttsRowIdx(this);
			data.initPrepStmtInsert(this);
			
			if (data.getInsertRowMeta() == null || data.getInsertRowMeta().size() < 1 ){
				retval.setError( BaseMessages.getString( PKG, "LoadDialog.CheckResult.NoMapping" ) );
				return retval;
			}
			
			String schemaTable = databaseMeta.getQuotedSchemaTableCombination( schemaName, targetTable);
            String cr_table = db.getDDL( schemaTable, data.getInsertRowMeta(), null, false, null );

            if ( cr_table == null || cr_table.length() == 0 ) {
              cr_table = null;
            }
            retval.setSQL( cr_table );
		} catch ( KettleDatabaseException dbe ) {
            retval.setError( BaseMessages.getString( PKG, "LoadDialog.Error.ErrorConnecting", dbe.getMessage() ) );
        } finally {
            db.disconnect();
        }
		return retval;
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
