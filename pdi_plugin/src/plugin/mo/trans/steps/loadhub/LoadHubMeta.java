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
package plugin.mo.trans.steps.loadhub;

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
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import plugin.mo.trans.steps.common.BaseLoadHubLink;
import plugin.mo.trans.steps.common.BaseLoadMeta;
import plugin.mo.trans.steps.common.LoadHubLinkData;
import plugin.mo.trans.steps.loadhub.ui.LoadHubDialog;
import plugin.mo.trans.steps.loadsat.LoadSatMeta;

/**
 * 
 * Meta class used for the Hub.
 * 
 * @author mouellet
 *
 */
@Step(id = "LoadHubAnchorPlugin", name = "LoadHubDialog.Shell.Title", description="LoadHubDialog.Shell.Desc", 
		image = "hub.png", 	i18nPackageName="plugin.mo.trans.steps.common", 
		categoryDescription="i18n:org.pentaho.di.trans.step:BaseStep.Category.Experimental")
public class LoadHubMeta extends BaseLoadMeta implements StepMetaInterface {
	public static String IDENTIFYING_KEY = "Business/Natural Key";
	public static String OTHER_TYPE = "Other Attribute";
	

	public LoadHubMeta() {
		super();
	}
		

	@Override
	public String getIdKeyTypeString() {
		return IDENTIFYING_KEY;
	}


	@Override
	public String getOtherTypeString() {
		return OTHER_TYPE;
	}

	
	@Override
	public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
			TransMeta transMeta, Trans trans) {
		return new BaseLoadHubLink(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	
	@Override
	public StepDataInterface getStepData() {
		return new LoadHubLinkData(this.getLog());
	}

	public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name) {
		return new LoadHubDialog(shell, meta, transMeta, name);
	}

	
	@Override
	public void setDefault() {
		super.setDefault();
		allocateKeyArray(1);
		fields[0] = "key field-1";
		cols[0] = "key column-1";
		types[0] = IDENTIFYING_KEY;
	}

	
	/*
	 * Appending new field onto output stream
	 */
	public void getFields(RowMetaInterface row, String origin, RowMetaInterface[] info, StepMeta nextStep,
			VariableSpace space, Repository repository, IMetaStore metaStore) throws KettleStepException {

		ValueMetaInterface v = new ValueMetaInteger(getNewKeyFieldName());
		v.setName(getNewKeyFieldName());
		v.setLength(10);
		v.setPrecision(0);
		v.setOrigin(origin);
		row.addValueMeta(v);
	}

	public String getXML() throws KettleException {
		String base = super.getXML();
		
		StringBuffer retval = new StringBuffer(100);
		retval.append("  ").append(XMLHandler.addTagValue("techKeyCol", techKeyCol));
		retval.append("  ").append(XMLHandler.addTagValue("keyGeneration", keyGeneration));
		retval.append("  ").append(XMLHandler.addTagValue("sequenceName", sequenceName));
		return base + retval.toString();
	}
	

	public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) 
			throws KettleXMLException {
		this.readData(stepnode, databases);
	}
	
	protected void readData(Node stepnode, List<? extends SharedObjectInterface> databases) 
			throws KettleXMLException {
		try {
			super.readData(stepnode, databases);
			techKeyCol = XMLHandler.getTagValue(stepnode, "techKeyCol");
			keyGeneration = XMLHandler.getTagValue(stepnode, "keyGeneration");
			sequenceName = XMLHandler.getTagValue(stepnode, "sequenceName");
		} catch (Exception e) {
			throw new KettleXMLException(BaseMessages.getString(PKG, "LoadHubMeta.Exception.LoadStepInfo"), e);
		}
	}

	
	public void readRep(Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases)
			throws KettleException {
		try {
			super.readRep(rep, metaStore, id_step, databases);
			techKeyCol = rep.getStepAttributeString(id_step, "techKeyCol");
			keyGeneration = rep.getStepAttributeString(id_step, "keyGeneration");
			sequenceName = rep.getStepAttributeString(id_step, "sequenceName");
		} catch (Exception e) {
			throw new KettleException(BaseMessages.getString(PKG,
					"LoadHubMeta.Exception.ErrorReadingHubStepInfo"), e);
		}
	}
	
	public void saveRep(Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step)
			throws KettleException {
		try {
			super.saveRep(rep, metaStore, id_transformation, id_step);
			rep.saveStepAttribute(id_transformation, id_step, "techKeyCol", techKeyCol);
			rep.saveStepAttribute(id_transformation, id_step, "keyGeneration", keyGeneration);
			rep.saveStepAttribute(id_transformation, id_step, "sequenceName", sequenceName);
			
		} catch (Exception e) {
			throw new KettleException(BaseMessages.getString(PKG, "LoadHubMeta.Exception.UnableToSaveHubStepInfo")
					+ id_step, e);
		}
	}
	
	
	public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
			RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
			Repository repository, IMetaStore metaStore) {
		super.check(remarks, transMeta, stepMeta, prev, input, output, info, space, repository, metaStore);
		
		CheckResult cr;
		String error_message = "";

		if (Const.isEmpty(techKeyCol)){
			error_message = BaseMessages.getString(PKG,
					"LoadMeta.CheckResult.SurrogateKeyNotFound") + Const.CR;
			cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
			remarks.add(cr);
		}
		
		int keyfound = 0;
		for (int i = 0; i < cols.length; i++) {
			if (types[i].equals(LoadHubMeta.IDENTIFYING_KEY)) {
				keyfound++;
			} 
		}
		if (keyfound == 0) {
			error_message += BaseMessages.getString(PKG,
					"LoadHubMeta.CheckResult.KeyFieldsIssues",LoadSatMeta.ATTRIBUTE_FK) + Const.CR;
			cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
			remarks.add(cr);
		}
		if (keyGeneration != null) {
				if (!(BaseLoadMeta.CREATION_METHOD_AUTOINC.equals(keyGeneration)
						|| BaseLoadMeta.CREATION_METHOD_SEQUENCE.equals(keyGeneration) 
						|| BaseLoadMeta.CREATION_METHOD_TABLEMAX.equals(keyGeneration))) {
					error_message += BaseMessages.getString(PKG, "LoadMeta.CheckResult.ErrorSurrKeyCreation")
							+ ": " + keyGeneration + "!";
					cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
					remarks.add(cr);
				}
		}
	}	
	
	
	
	//useful to construct DDL reflecting UI configuration settings
	@Override
	public SQLStatement getSQLStatements(TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
			Repository repository, IMetaStore metaStore) throws KettleStepException {
		
		SQLStatement retval = super.getSQLStatements(transMeta, stepMeta, prev, repository, metaStore);
		if (retval.getError() != null){
			return retval;
		}
		
		Database db = new Database(loggingObject, databaseMeta);
		db.shareVariablesWith( transMeta );
		LoadHubLinkData data = (LoadHubLinkData) getStepData();
		data.db = db;
		try {
			db.connect();
			
			data.outputRowMeta = transMeta.getPrevStepFields( stepMeta.getName()).clone();
			data.initRowIdx(this);
			data.initPrepStmtInsert(this);
			
			
			if (data.getInsertRowMeta() == null || data.getInsertRowMeta().size() < 1 ){
				retval.setError( BaseMessages.getString( PKG, "LoadDialog.CheckResult.NoMapping" ) );
				return retval;
			}
			
			//Add explicit PK when using Sequence or Table-max generation
			if (isMethodAutoIncrement() || keyGeneration.equals(CREATION_METHOD_SEQUENCE)){
				data.getInsertRowMeta().addValueMeta(new ValueMetaInteger(techKeyCol));
			}
			String schemaTable = databaseMeta.getQuotedSchemaTableCombination( schemaName, targetTable);
            String cr_table = db.getDDL( schemaTable, data.getInsertRowMeta(), techKeyCol, isMethodAutoIncrement(), null);

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


	public Object clone() {
		LoadHubMeta retval = (LoadHubMeta) super.clone();
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

	
}
