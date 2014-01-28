package plugin.mo.trans.steps.common;

import java.util.List;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.shared.SharedObjectInterface;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

import plugin.mo.trans.steps.loadhub.LoadHubMeta;
import plugin.mo.trans.steps.loadlink.LoadLinkMeta;

/**
 * Contains attributes exchanged between UI and Meta common to the plugins Steps.
 * 
 * Subclass must implement: clone(), loadXML(), check(), getStep(), 
 * getStepData(), getDialog(), complete the partial impl of reading/writing
 * and any overriden and specialized properties.
 * @author mouellet
 *
 */
public abstract class LoadBaseMeta extends BaseStepMeta implements StepMetaInterface {
	//TODO: remove the ones in subclass
	protected static Class<?> PKG = CompositeValues.class;

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
	
	//name of techKeyCol added to output stream (default = table.techKeyCol)
	protected String newKeyFieldName;		

	
	public LoadBaseMeta(){
		super();
	}
	
	
	public void setDefault() {
		schemaName = "";
		targetTable = "";
		databaseMeta = null;
		bufferSize = MIN_BUFFER_SIZE*4;
	
		//rest to be implemented by subclass 	
	}

	
	public void allocateKeyArray(int nrkeys) {
		fields = new String[nrkeys];
		cols = new String[nrkeys];
		types = new String[nrkeys];
	}

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
		//rest to be implemented by subclass (picking up the returned String)
		
	}

	//subclass must still imple the LoadXML..
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
			auditRecSourceValue = XMLHandler.getTagValue(stepnode, "auditRecSrcValue");
				
		
		} catch (Exception e) {
			throw new KettleXMLException(BaseMessages.getString(PKG, "LoadHubMeta.Exception.UnableToLoadStepInfo"), e);
		}
	}

	//DO The other reading...writing..
	
	
	
	public DatabaseMeta[] getUsedDatabaseConnections() {
		if (databaseMeta != null) {
			return new DatabaseMeta[] { databaseMeta };
		} else {
			return super.getUsedDatabaseConnections();
		}
	}

	
	//maybe must be done by subclass... to get proper class type...
	public boolean equals(Object other) {
		if (other == this) {
			return true;
		}
		if (other == null || getClass() != other.getClass()) {
			return false;
		}

		LoadBaseMeta o = (LoadBaseMeta) other;
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
		if (bSize < LoadHubMeta.MIN_BUFFER_SIZE) {
			bufferSize = LoadHubMeta.MIN_BUFFER_SIZE;
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
		return newKeyFieldName;
	}


	public void setNewKeyFieldName(String newKeyFieldName) {
		this.newKeyFieldName = newKeyFieldName;
	}
	

	
	
	
}
