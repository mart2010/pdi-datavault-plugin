package plugin.mo.trans.steps.loadlink;

import java.util.ArrayList;
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

import plugin.mo.trans.steps.backup.loadanchor.LoadAnchorMeta;
import plugin.mo.trans.steps.common.BaseLoadMeta;
import plugin.mo.trans.steps.common.CompositeValues;
import plugin.mo.trans.steps.common.LoadHubLinkData;
import plugin.mo.trans.steps.loadlink.ui.LoadLinkDialog;
import plugin.mo.trans.steps.loadsat.LoadSatMeta;
import plugin.mo.trans.steps.loadsat.ui.LoadSatDialog;

/**
 * 
 * TODO: 
 * -
 * 
 * @author mouellet
 *
 */
public class LoadLinkMeta extends BaseLoadMeta implements StepMetaInterface {
	public static String IDENTIFYING_KEY = "Relationship Key";
	public static String OTHER_TYPE = "Other Attribute";
	
	
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
		return new LoadHubLinkData(this.getLog());
	}

	public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name) {
		return new LoadLinkDialog(shell, meta, transMeta, name);
	}

	
	@Override
	public void setDefault() {
		super.setDefault();
		int nrkeys = 2;
		allocateKeyArray(nrkeys);
		for (int i = 1; i < nrkeys; i++) {
			fields[i-1] = "key field-" + i;
			cols[i-1] = "key column-" + i;
			types[i-1] = LoadLinkMeta.IDENTIFYING_KEY;
		}
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
			throw new KettleXMLException(BaseMessages.getString(PKG, "LoadLinkMeta.Exception.LoadStepInfo"), e);
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
					"LoadLinkMeta.Exception.ErrorReadingLinkStepInfo"), e);
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
			throw new KettleException(BaseMessages.getString(PKG, "LoadLinkMeta.Exception.UnableToSaveLinkStepInfo")
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
/*
				if (!Const.isEmpty(linkTable)) {
					boolean first = true;
					boolean error_found = false;
					
					String schemaLinkTable = databaseMeta.getQuotedSchemaTableCombination(schemaName, linkTable);
					RowMetaInterface linkRowMeta = db.getTableFields(schemaLinkTable);
					//to complete ...
					
				}
				
				//....
				/* to add to check :
				if (keyCols == null || keyCols.length < 2) {
					throw new KettleStepException(BaseMessages.getString(PKG, "LoadLinkMeta.CheckResult.KeyFieldsIssues"));
				}

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
				*/
				
				
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
	
	
	public Object clone() {
		LoadLinkMeta retval = (LoadLinkMeta) super.clone();
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
