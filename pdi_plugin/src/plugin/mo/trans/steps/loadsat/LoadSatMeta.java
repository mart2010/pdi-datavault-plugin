package plugin.mo.trans.steps.loadsat;

import java.text.ParseException;
import java.text.SimpleDateFormat;
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
import plugin.mo.trans.steps.loadhub.LoadHub;
import plugin.mo.trans.steps.loadhub.LoadHubData;
import plugin.mo.trans.steps.loadhub.LoadHubMeta;
import plugin.mo.trans.steps.loadhub.ui.LoadHubDialog;
import plugin.mo.trans.steps.loadsat.ui.LoadSatDialog;

/*
 * 
 * TODO: 
 *
 */
public class LoadSatMeta extends BaseStepMeta implements StepMetaInterface {
	private static Class<?> PKG = CompositeValues.class;
		
	public static String ATTRIBUTE_NORMAL = "Normal";
	public static String ATTRIBUTE_TEMPORAL = "FromDate Temporal";
	public static String ATTRIBUTE_SURR_FK = "Surrogate ForeignKey";
		
	public static String NA = "n.a.";
	public static String DEFAULT_MAX_DATE = "01-01-4000";
	public static String DATE_FORMAT = "dd-MM-yyyy";
	
	private DatabaseMeta databaseMeta;

	private String schemaName; 
	private String satTable;

	// buffer & fetch size
	private int bufferSize;

	// fields used for attributes
	private String[] attField;

	// table columns matching fields
	private String[] attCol;
	
	// attribute type (normal,temporal,surrFk)
	private String[] attType;
		
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

	private boolean isIdempotent = false;
	
	
	public LoadSatMeta() {
		super();
	}
	
	
	public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
			Trans trans) {
		return new LoadSat(stepMeta, stepDataInterface, cnr, transMeta, trans);
	}

	public StepDataInterface getStepData() {
		return new LoadSatData();
	}
	

	public void setDefault() {
		schemaName = "";
		satTable = "";
		databaseMeta = null;
		bufferSize = LoadHubMeta.MIN_BUFFER_SIZE;
		
		allocateArray(2);
		attField[0] = "fkey field";
		attCol[0] = "fkey col";
		attType[0] = LoadSatMeta.ATTRIBUTE_SURR_FK;
		attField[1] = "att field";
		attCol[1] = "att col";
		attType[1] = LoadSatMeta.ATTRIBUTE_NORMAL;
		toDateMaxFlag = LoadSatMeta.DEFAULT_MAX_DATE;
		toDateColumn = LoadSatMeta.NA;
	}

	

	// return the XML holding all meta-setting
	public String getXML() throws KettleException {
		StringBuffer retval = new StringBuffer(512);
		retval.append("  ").append(
				XMLHandler.addTagValue("connection", databaseMeta == null ? "" : databaseMeta.getName()));
		retval.append("  ").append(XMLHandler.addTagValue("schemaName", schemaName));
		retval.append("  ").append(XMLHandler.addTagValue("satTable", satTable));
		retval.append("  ").append(XMLHandler.addTagValue("batchSize", bufferSize));

		retval.append("  <fields>").append(Const.CR);
		for (int i = 0; i < attField.length; i++) {
			retval.append("   <key>").append(Const.CR);
			retval.append("   ").append(XMLHandler.addTagValue("field_key", attField[i]));
			retval.append("   ").append(XMLHandler.addTagValue("col_lookup", attCol[i]));
			retval.append("   ").append(XMLHandler.addTagValue("col_type", attType[i]));
			retval.append("   </key>").append(Const.CR);
		}
		retval.append("  </fields>").append(Const.CR);

		retval.append("  ").append(XMLHandler.addTagValue("idempotent", isIdempotent));
		retval.append("  ").append(XMLHandler.addTagValue("toDateColumn", toDateColumn));
		retval.append("  ").append(XMLHandler.addTagValue("toDateMaxFlag", toDateMaxFlag));

		return retval.toString();
	}

	private void readData(Node stepnode, List<? extends SharedObjectInterface> databases) throws KettleXMLException {
		try {
			String con = XMLHandler.getTagValue(stepnode, "connection");
			databaseMeta = DatabaseMeta.findDatabase(databases, con);
			schemaName = XMLHandler.getTagValue(stepnode, "schemaName");
			satTable = XMLHandler.getTagValue(stepnode, "satTable");

			String bSize;
			bSize = XMLHandler.getTagValue(stepnode, "batchSize");
			bufferSize = Const.toInt(bSize, 0);
			
			Node keys = XMLHandler.getSubNode(stepnode, "fields");
			int nrkeys = XMLHandler.countNodes(keys, "key");
			allocateArray(nrkeys);
			// Read key mapping
			for (int i = 0; i < nrkeys; i++) {
				Node knode = XMLHandler.getSubNodeByNr(keys, "key", i);
				attField[i] = XMLHandler.getTagValue(knode, "field_key");
				attCol[i] = XMLHandler.getTagValue(knode, "col_lookup");
				attType[i] = XMLHandler.getTagValue(knode, "col_type");	
			}
			updateFkAndFromDate();
			isIdempotent = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "idempotent"));
			toDateColumn = XMLHandler.getTagValue(stepnode, "toDateColumn");
			toDateMaxFlag = XMLHandler.getTagValue(stepnode, "toDateMaxFlag");
		} catch (Exception e) {
			throw new KettleXMLException(BaseMessages.getString(PKG, "LoadHubMeta.Exception.UnableToLoadStepInfo"), e);
		}
	}

	
	private void updateFkAndFromDate(){
		for(int i = 0; i < attType.length ; i++){
			if (attType[i] != null && attType[i].equals(LoadSatMeta.ATTRIBUTE_SURR_FK)){
				fkColumn = attCol[i]; 
			}
			if (attType[i] != null &&  attType[i].equals(LoadSatMeta.ATTRIBUTE_TEMPORAL)){
				fromDateColumn = attCol[i]; 
			}
		}
	}
	
	
	public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {
		readData(stepnode, databases);
	}

	public void allocateArray(int nrFields) {
		attField = new String[nrFields];
		attCol = new String[nrFields];
		attType = new String[nrFields];
	}

	/**
	 * No modification here.
	 */
	public void getFields(RowMetaInterface row, String origin, RowMetaInterface[] info, StepMeta nextStep,
			VariableSpace space, Repository repository, IMetaStore metaStore) throws KettleStepException {
		
	}

	
	public Object clone() {
		LoadSatMeta retval = (LoadSatMeta) super.clone();

		int nrkeys = attField.length;
		retval.allocateArray(nrkeys);

		// Deep copy for Array
		for (int i = 0; i < nrkeys; i++) {
			retval.attField[i] = attField[i];
			retval.attCol[i] = attCol[i];
		}
		return retval;
	}

	public void readRep(Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases)
			throws KettleException {
		try {
			databaseMeta = rep.loadDatabaseMetaFromStepAttribute(id_step, "id_connection", databases);

			schemaName = rep.getStepAttributeString(id_step, "schemaName");
			satTable = rep.getStepAttributeString(id_step, "hubTable");
			bufferSize = (int) rep.getStepAttributeInteger(id_step, "batchSize");
			
			int nrkeys = rep.countNrStepAttributes(id_step, "field_key");
			allocateArray(nrkeys);
			for (int i = 0; i < nrkeys; i++) {
				attField[i] = rep.getStepAttributeString(id_step, i, "field_key");
				attCol[i]  = rep.getStepAttributeString(id_step, i, "col_lookup");
				attType[i] = rep.getStepAttributeString(id_step, i, "col_type");
			}

			updateFkAndFromDate();
			isIdempotent = rep.getStepAttributeBoolean(id_step, "idempotent");
			toDateColumn = rep.getStepAttributeString(id_step, "toDateColumn");
			toDateMaxFlag = rep.getStepAttributeString(id_step, "toDateMaxFlag");
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
			rep.saveStepAttribute(id_transformation, id_step, "hubTable", satTable);
			rep.saveStepAttribute(id_transformation, id_step, "batchSize", bufferSize);
			
			for (int i = 0; i < attField.length; i++) {
				rep.saveStepAttribute(id_transformation, id_step, i, "field_key", attField[i]);
				rep.saveStepAttribute(id_transformation, id_step, i, "col_lookup", attCol[i]);
			}
			
			rep.saveStepAttribute(id_transformation, id_step, "idempotent", isIdempotent);
			rep.saveStepAttribute(id_transformation, id_step, "toDateColumn", toDateColumn);
			rep.saveStepAttribute(id_transformation, id_step, "toDateMaxFlag", toDateMaxFlag);
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

				if (!Const.isEmpty(satTable)) {
					boolean first = true;
					boolean error_found = false;
					
					String schemaSatTable = databaseMeta.getQuotedSchemaTableCombination(schemaName, satTable);
					RowMetaInterface satRowMeta = db.getTableFields(schemaSatTable);
					
					int fkfound = 0;
					int temporalfound = 0;
					int normalfound = 0;
					int unknownfound = 0;
					
					if (satRowMeta != null) {
						for (int i = 0; i < attCol.length; i++) {
							String lufield = attCol[i];
							if (attType[i].equals(LoadSatMeta.ATTRIBUTE_SURR_FK)) {
								fkfound++;
							} else if (attType[i].equals(LoadSatMeta.ATTRIBUTE_TEMPORAL)){
								temporalfound++;
							} else if (attType[i].equals(LoadSatMeta.ATTRIBUTE_NORMAL)){
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
									"LoadSatMeta.CheckResult.NoFKFieldsFound") + Const.CR;
							cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
							remarks.add(cr);
						} else if (fkfound > 1) {
							error_message += BaseMessages.getString(PKG,
									"LoadSatMeta.CheckResult.ManyFKFieldsFound") + Const.CR;
							cr = new CheckResult(CheckResultInterface.TYPE_RESULT_WARNING, error_message, stepMeta);
							remarks.add(cr);
						}
						
						if (temporalfound > 0) {
							if (temporalfound > 1) {
								error_message += BaseMessages.getString(PKG,
										"LoadSatMeta.CheckResult.ManyTempoFieldsFound") + Const.CR;
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

				
				if (bufferSize > LoadHubMeta.MAX_SUGG_BUFFER_SIZE){
					error_message = BaseMessages.getString(PKG, "LoadDialog.CheckResult.BufferSize") + Const.CR;
					cr = new CheckResult(CheckResultInterface.TYPE_RESULT_WARNING, error_message, stepMeta);
					remarks.add(cr);
				}
				
				// Look up fields in the input stream <prev>
				if (prev != null && prev.size() > 0) {
					boolean first = true;
					error_message = "";
					boolean error_found = false;

					for (int i = 0; i < attField.length; i++) {
						ValueMetaInterface v = prev.searchValueMeta(attField[i]);
						if (v == null) {
							if (first) {
								first = false;
								error_message += BaseMessages.getString(PKG, "LoadDialog.CheckResult.MissingFields")
										+ Const.CR;
							}
							error_found = true;
							error_message += "\t\t" + attField[i] + Const.CR;
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

	public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name) {
		return new LoadSatDialog(shell, meta, transMeta, name);
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

		LoadSatMeta o = (LoadSatMeta) other;

		if ((getSchemaName() == null && o.getSchemaName() != null)
				|| (getSchemaName() != null && o.getSchemaName() == null)
				|| (getSchemaName() != null && o.getSchemaName() != null && !getSchemaName().equals(o.getSchemaName()))) {
			return false;
		}

		if ((getSatTable() == null && o.getSatTable() != null) || (getSatTable() != null && o.getSatTable() == null)
				|| (getSatTable() != null && o.getSatTable() != null && !getSatTable().equals(o.getSatTable()))) {
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
	
	
	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	public String getSatTable() {
		return satTable;
	}

	public void setSatTable(String anchorTable) {
		this.satTable = anchorTable;
	}


	public DatabaseMeta getDatabaseMeta() {
		return databaseMeta;
	}

	public void setDatabaseMeta(DatabaseMeta databaseMeta) {
		this.databaseMeta = databaseMeta;
	}

	
	public String[] getAttField() {
		return attField;
	}

	public String[] getAttCol() {
		return attCol;
	}

	public String getFromDateColumn() {
		return fromDateColumn;
	}

	public void setFromDateColumn(String fromDateColumn) {
		this.fromDateColumn = fromDateColumn;
	}

	public boolean isToDateColumnUsed() {
		return !(Const.isEmpty(toDateColumn)) && !(toDateColumn.equals(LoadSatMeta.NA));
	}

	
	public String getToDateColumn() {
		return toDateColumn;
	}

	public void setToDateColumn(String toDateColumn) {
		this.toDateColumn = toDateColumn;
	}

	public String[] getAttType() {
		return attType;
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
