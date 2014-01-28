package plugin.mo.trans.steps.common;

public interface HubLinkCommonMeta {
	
	//Column name of Link or Hub Primary Key (i.e. technical unique key)
	public String getPrimaryKey();
	
	//Column names of composite keys (Link: technical foreign keys, Hub: natural keys)
	public String[] getColKeys();
	
	//Any other columns name included in the mapping
	public String[] getColOthers();

	//Column for Load date/time audit
	public String getColLoadDTS();
	
	//Column for Source audit
	public String getColRecSource();
	
	

}
