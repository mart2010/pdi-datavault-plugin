package plugin.mo.trans.steps.loadsat.ui;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.combinationlookup.CombinationLookupMeta;
import org.pentaho.di.ui.core.database.dialog.DatabaseExplorerDialog;
import org.pentaho.di.ui.core.dialog.EnterSelectionDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.ComboValuesSelectionListener;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.trans.step.TableItemInsertListener;

import plugin.mo.trans.steps.backup.loadanchor.LoadAnchorMeta;
import plugin.mo.trans.steps.common.CompositeValues;
import plugin.mo.trans.steps.loadsat.LoadSatMeta;

/*
 * 
 * Responsible for the creation/setup of all widgets. 
 * Dialog elements created in order of appearance on dialog (top to bottom) 
 * Each widget consists of 1- its label and 2- the entry widget itself
 * FormData objects define the anchor points of the widgets
 * 
 */
public class LoadSatDialog extends BaseStepDialog implements StepDialogInterface {
	private static Class<?> PKG = CompositeValues.class;
	
	private CCombo wConnection;

	private Label wlSchema;
	private TextVar wSchema;
	private Button wbSchema;
	private FormData fdbSchema;

	private Label wlSatTable;
	private Button wbSatTable;
	private TextVar wSatTable;

	private Label wlBatchSize;
	private Text wBatchSize;

	private Label wlKey;
	private TableView wKey;
	private boolean hasOneTemporalField;
	
	private Label wlIsIdempotentSat;
	private Button wbIsIdempotentSat;

	private Group wAdditionalFields;
	private Label wlToDateCol;
	private CCombo wcbToDateCol;
	private Label wlToDateMax;
	private Text wToDateMax;


	private Button wGet;
	private Listener lsGet;

	private ColumnInfo[] ciKey;
	private LoadSatMeta inputMeta;
	private DatabaseMeta dbMeta;

	private Map<String, Integer> inputFields;
	// used to cache columns any tables for connection.schema
	private Map<String, RowMetaInterface> cacheColumnMap;

	/**
	 * List of ColumnInfo that should have the field names of the selected
	 * database table
	 */
	private List<ColumnInfo> tableFieldColumns = new ArrayList<ColumnInfo>();

	public LoadSatDialog(Shell parent, Object in, TransMeta transMeta, String sname) {
		super(parent, (BaseStepMeta) in, transMeta, sname);
		inputMeta = (LoadSatMeta) in;
		inputFields = new HashMap<String, Integer>();
		cacheColumnMap = new HashMap<String, RowMetaInterface>();
	}

	/*
	 * Constructing all Dialog widgets Return the (possibly new) name of the
	 * step. If it returns null, Kettle assumes that the dialog was cancelled
	 * (done by Cancel handler).
	 */
	public String open() {
		Shell parent = getParent();
		Display display = parent.getDisplay();

		shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
		props.setLook(shell);
		setShellImage(shell, inputMeta);

		FormLayout formLayout = new FormLayout();
		formLayout.marginWidth = Const.FORM_MARGIN;
		formLayout.marginHeight = Const.FORM_MARGIN;

		shell.setLayout(formLayout);
		shell.setText(BaseMessages.getString(PKG, "LoadSatDialog.Shell.Title"));

		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;

		ModifyListener lsMod = new ModifyListener() {
			public void modifyText(ModifyEvent e) {
				inputMeta.setChanged();
			}
		};
		ModifyListener lsTableMod = new ModifyListener() {
			public void modifyText(ModifyEvent arg0) {
				inputMeta.setChanged();
				setTableFieldCombo();
			}
		};
		SelectionListener lsSelection = new SelectionAdapter() {
			public void widgetSelected(SelectionEvent e) {
				inputMeta.setChanged();
				setTableFieldCombo();
			}
		};
		backupChanged = inputMeta.hasChanged();
		dbMeta = inputMeta.getDatabaseMeta();

		// Stepname line
		wlStepname = new Label(shell, SWT.RIGHT);
		wlStepname.setText(BaseMessages.getString(PKG, "LoadDialog.Stepname.Label"));
		props.setLook(wlStepname);

		fdlStepname = new FormData();
		fdlStepname.left = new FormAttachment(0, 0);
		fdlStepname.right = new FormAttachment(middle, -margin);
		fdlStepname.top = new FormAttachment(0, margin);
		wlStepname.setLayoutData(fdlStepname);
		wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wStepname.setText(stepname);
		props.setLook(wStepname);
		wStepname.addModifyListener(lsMod);
		fdStepname = new FormData();
		fdStepname.left = new FormAttachment(middle, 0);
		fdStepname.top = new FormAttachment(0, margin);
		fdStepname.right = new FormAttachment(100, 0);
		wStepname.setLayoutData(fdStepname);

		// Connection line
		wConnection = addConnectionLine(shell, wStepname, middle, margin);
		if (inputMeta.getDatabaseMeta() == null && transMeta.nrDatabases() == 1) {
			wConnection.select(0);
		}
		wConnection.addModifyListener(lsMod);
		wConnection.addSelectionListener(lsSelection);
		wConnection.addModifyListener(new ModifyListener() {
			public void modifyText(ModifyEvent e) {
				// We have new content: change ci connection:
				dbMeta = transMeta.findDatabase(wConnection.getText());
				inputMeta.setChanged();
				resetColumnsCache();
			}
		});

		// Schema line...
		wlSchema = new Label(shell, SWT.RIGHT);
		wlSchema.setText(BaseMessages.getString(PKG, "LoadDialog.TargetSchema.Label"));
		props.setLook(wlSchema);
		FormData fdlSchema = new FormData();
		fdlSchema.left = new FormAttachment(0, 0);
		fdlSchema.right = new FormAttachment(middle, -margin);
		fdlSchema.top = new FormAttachment(wConnection, margin);
		wlSchema.setLayoutData(fdlSchema);

		wbSchema = new Button(shell, SWT.PUSH | SWT.CENTER);
		props.setLook(wbSchema);
		wbSchema.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
		fdbSchema = new FormData();
		fdbSchema.top = new FormAttachment(wConnection, margin);
		fdbSchema.right = new FormAttachment(100, 0);
		wbSchema.setLayoutData(fdbSchema);

		wSchema = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wSchema);
		wSchema.addModifyListener(lsTableMod);
		FormData fdSchema = new FormData();
		fdSchema.left = new FormAttachment(middle, 0);
		fdSchema.top = new FormAttachment(wConnection, margin);
		fdSchema.right = new FormAttachment(wbSchema, -margin);
		wSchema.setLayoutData(fdSchema);

		// Sat Table line...
		wlSatTable = new Label(shell, SWT.RIGHT);
		wlSatTable.setText(BaseMessages.getString(PKG, "LoadSatDialog.Target.Label"));
		props.setLook(wlSatTable);
		FormData fdlTable = new FormData();
		fdlTable.left = new FormAttachment(0, 0);
		fdlTable.right = new FormAttachment(middle, -margin);
		fdlTable.top = new FormAttachment(wbSchema, margin);
		wlSatTable.setLayoutData(fdlTable);

		wbSatTable = new Button(shell, SWT.PUSH | SWT.CENTER);
		props.setLook(wbSatTable);
		wbSatTable.setText(BaseMessages.getString(PKG, "LoadDialog.BrowseTable.Button"));
		FormData fdbTable = new FormData();
		fdbTable.right = new FormAttachment(100, 0);
		fdbTable.top = new FormAttachment(wbSchema, margin);
		wbSatTable.setLayoutData(fdbTable);

		wSatTable = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wSatTable);
		wSatTable.addModifyListener(lsTableMod);
		FormData fdTable = new FormData();
		fdTable.left = new FormAttachment(middle, 0);
		fdTable.top = new FormAttachment(wbSchema, margin);
		fdTable.right = new FormAttachment(wbSatTable, -margin);
		wSatTable.setLayoutData(fdTable);

		// Idempotent ?
		wlIsIdempotentSat = new Label(shell, SWT.RIGHT);
		wlIsIdempotentSat.setText(BaseMessages.getString(PKG, "LoadSatDialog.IdempotentTransf.Label"));
		props.setLook(wlIsIdempotentSat);
		FormData fdlIdempotent = new FormData();
		fdlIdempotent.left = new FormAttachment(0, 0);
		fdlIdempotent.right = new FormAttachment(middle, -margin);
		fdlIdempotent.top = new FormAttachment(wbSatTable, margin);
		wlIsIdempotentSat.setLayoutData(fdlIdempotent);

		wbIsIdempotentSat = new Button(shell, SWT.CHECK);
		props.setLook(wbIsIdempotentSat);
		FormData fdbExtNatkeyTable = new FormData();
		fdbExtNatkeyTable.left = new FormAttachment(middle, 0);
		fdbExtNatkeyTable.right = new FormAttachment(middle + (100 - middle) / 3, -margin);
		fdbExtNatkeyTable.top = new FormAttachment(wbSatTable, margin);
		wbIsIdempotentSat.setLayoutData(fdbExtNatkeyTable);

		
		// Batch size ...
		wlBatchSize = new Label(shell, SWT.RIGHT);
		wlBatchSize.setText(BaseMessages.getString(PKG, "LoadDialog.Batchsize.Label"));
		props.setLook(wlBatchSize);
		FormData fdlBatch = new FormData();
		fdlBatch.left = new FormAttachment(0, 0);
		fdlBatch.right = new FormAttachment(middle, -margin);
		fdlBatch.top = new FormAttachment(wbIsIdempotentSat, margin);
		wlBatchSize.setLayoutData(fdlBatch);
		wBatchSize = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wBatchSize);
		wBatchSize.addModifyListener(lsMod);
		FormData fdBatch = new FormData();
		fdBatch.top = new FormAttachment(wbIsIdempotentSat, margin);
		fdBatch.left = new FormAttachment(middle, 0);
		fdBatch.right = new FormAttachment(middle + (100 - middle) / 3, -margin);
		wBatchSize.setLayoutData(fdBatch);

		
		//
		// The fields mapping
		//
		wlKey = new Label(shell, SWT.NONE);
		wlKey.setText(BaseMessages.getString(PKG, "LoadSatDialog.Attfields.Label"));
		props.setLook(wlKey);
		FormData fdlKey = new FormData();
		fdlKey.left = new FormAttachment(0, 0);
		fdlKey.top = new FormAttachment(wBatchSize, margin*3);
		fdlKey.right = new FormAttachment(100, 0);
		wlKey.setLayoutData(fdlKey);

		int nrKeyCols = 3;
		int nrRows = (inputMeta.getAttField() != null ? inputMeta.getAttField().length : 1);

		ciKey = new ColumnInfo[nrKeyCols];
		ciKey[0] = new ColumnInfo(BaseMessages.getString(PKG, "LoadSatDialog.ColumnInfo.TableColumn"),
				ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false);
		ciKey[1] = new ColumnInfo(BaseMessages.getString(PKG, "LoadSatDialog.ColumnInfo.FieldInStream"),
				ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false);
		ciKey[2] = new ColumnInfo(BaseMessages.getString(PKG, "LoadSatDialog.ColumnInfo.Type"),
				ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { LoadSatMeta.ATTRIBUTE_NORMAL, 
							LoadSatMeta.ATTRIBUTE_SURR_FK , LoadSatMeta.ATTRIBUTE_TEMPORAL });
		
		// attach the tableFieldColumns List to the widget
		tableFieldColumns.add(ciKey[0]);
		wKey = new TableView(transMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL
				| SWT.H_SCROLL, ciKey, nrRows, lsMod, props);

		
		ciKey[2].setSelectionAdapter(new SelectionAdapter() {
			public void widgetSelected(SelectionEvent arg0) {
				hasOneTemporalField = false;
				//iterate mapping list and "activate" temporal or not...
				for (int i=0 ; i < wKey.nrNonEmpty() ; i++){
					TableItem item = wKey.getNonEmpty(i);	
					if (item.getText(3).equals(LoadSatMeta.ATTRIBUTE_TEMPORAL)){
						hasOneTemporalField = true;
						break;
					}
				}
				enableFields();
			}
		});

		
		// THE BUTTONS
		wOK = new Button(shell, SWT.PUSH);
		wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));
		wGet = new Button(shell, SWT.PUSH);
		wGet.setText(BaseMessages.getString(PKG, "LoadDialog.GetFields.Button"));
		wCancel = new Button(shell, SWT.PUSH);
		wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
		
		setButtonPositions(new Button[] { wOK, wCancel, wGet }, margin, null);
		
		// The optional Group for "ToDate"
		wAdditionalFields = new Group( shell, SWT.SHADOW_NONE );
	    props.setLook( wAdditionalFields );
	    wAdditionalFields.setText( BaseMessages.getString( PKG, "LoadSatDialog.UsingOptToDate.Label" ) );
	    FormLayout AdditionalFieldsgroupLayout = new FormLayout();
	    AdditionalFieldsgroupLayout.marginWidth = 100;
	    AdditionalFieldsgroupLayout.marginHeight = 5;
	    wAdditionalFields.setLayout( AdditionalFieldsgroupLayout );
		
 
		// ToDate Expire Column name...
		wlToDateCol = new Label(wAdditionalFields, SWT.RIGHT);
		wlToDateCol.setText(BaseMessages.getString(PKG, "LoadSatDialog.ToDateExpCol.Label"));
		props.setLook(wlToDateCol);
		FormData fdlNatTable = new FormData();
		fdlNatTable.left = new FormAttachment(0, 0);
		fdlNatTable.right = new FormAttachment(middle, -margin);
		fdlNatTable.top = new FormAttachment(wAdditionalFields, margin);
		wlToDateCol.setLayoutData(fdlNatTable);

		wcbToDateCol = new CCombo(wAdditionalFields, SWT.BORDER | SWT.READ_ONLY);
		props.setLook(wcbToDateCol);
		wcbToDateCol.addModifyListener(lsMod);
		FormData fdToDate = new FormData();
		fdToDate.left = new FormAttachment(middle, 0);
		fdToDate.right = new FormAttachment(middle + (100 - middle) / 2, -margin);
		fdToDate.top = new FormAttachment(wAdditionalFields, margin);
		wcbToDateCol.setLayoutData(fdToDate);
		wcbToDateCol.addFocusListener(new FocusListener() {
			public void focusLost(FocusEvent arg0) { }

			public void focusGained(FocusEvent arg0) {
				Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
				shell.setCursor(busy);
				setToDateColumns();
				shell.setCursor(null);
				busy.dispose();
			}
		});


		// Expire toDate MAX flag value 
		wlToDateMax = new Label(wAdditionalFields, SWT.RIGHT);
	    String flagText = BaseMessages.getString(PKG, "LoadSatDialog.ExpRecFlag.Label") + " (" + LoadSatMeta.DATE_FORMAT + ")";
		wlToDateMax.setText(flagText);
		props.setLook(wlToDateMax);
		FormData fdlToDateMax = new FormData();
		fdlToDateMax.left = new FormAttachment(0, 0);
		fdlToDateMax.right = new FormAttachment(middle, -margin);
		fdlToDateMax.top = new FormAttachment(wcbToDateCol, margin);
		wlToDateMax.setLayoutData(fdlToDateMax);
		
		wToDateMax = new Text(wAdditionalFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wToDateMax);
		wToDateMax.addModifyListener(lsMod);
		FormData fdToDateMax = new FormData();
		fdToDateMax.left = new FormAttachment(middle, 0);
		fdToDateMax.right = new FormAttachment(middle + (100 - middle) / 2, -margin);
		fdToDateMax.top = new FormAttachment(wcbToDateCol, margin);
		wToDateMax.setLayoutData(fdToDateMax);

		
		// to fix the Grid
		FormData fdKey = new FormData();
		fdKey.left = new FormAttachment(0, 0);
		fdKey.top = new FormAttachment(wlKey, margin);
		fdKey.right = new FormAttachment(100, 0);
		fdKey.bottom = new FormAttachment(wAdditionalFields, -margin);
		wKey.setLayoutData(fdKey);

		
	    FormData fdOptGroup = new FormData();
	    fdOptGroup.left = new FormAttachment( 0, 0 );
	    fdOptGroup.right = new FormAttachment( 100, 0 );
	    fdOptGroup.bottom = new FormAttachment( wOK, -2*margin );
	    wAdditionalFields.setLayoutData( fdOptGroup );

		
		//
		// Search the fields in the background
		//
		final Runnable runnable = new Runnable() {
			public void run() {
				StepMeta stepMeta = transMeta.findStep(stepname);
				if (stepMeta != null) {
					try {
						RowMetaInterface row = transMeta.getPrevStepFields(stepMeta);

						// Remember these fields...
						for (int i = 0; i < row.size(); i++) {
							inputFields.put(row.getValueMeta(i).getName(), i);
						}

						setComboBoxes();
					} catch (KettleException e) {
						logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
					}
				}
			}
		};
		new Thread(runnable).start();

		// Add listeners
		lsOK = new Listener() {
			public void handleEvent(Event e) {
				ok();
			}
		};
		lsGet = new Listener() {
			public void handleEvent(Event e) {
				getFieldsFromInput();
			}
		};
		lsCancel = new Listener() {
			public void handleEvent(Event e) {
				cancel();
			}
		};

		wOK.addListener(SWT.Selection, lsOK);
		wGet.addListener(SWT.Selection, lsGet);
		wCancel.addListener(SWT.Selection, lsCancel);

		lsDef = new SelectionAdapter() {
			public void widgetDefaultSelected(SelectionEvent e) {
				ok();
			}
		};

		wStepname.addSelectionListener(lsDef);
		wSchema.addSelectionListener(lsDef);
		wSatTable.addSelectionListener(lsDef);
		wBatchSize.addSelectionListener(lsDef);
		wbIsIdempotentSat.addSelectionListener(lsDef);
		wcbToDateCol.addSelectionListener(lsDef);
		wToDateMax.addSelectionListener(lsDef);
		
		

		// Detect X or ALT-F4 or something that kills this window...
		shell.addShellListener(new ShellAdapter() {
			public void shellClosed(ShellEvent e) {
				cancel();
			}
		});
		wbSchema.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(SelectionEvent e) {
				getSchemaNames();
			}
		});

		wbSatTable.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(SelectionEvent e) {
				getTableName();
			}
		});

		// Set the shell size, based upon previous time...
		setSize();

		setTableFieldCombo();
		setToDateColumns();
		getData();
		
		inputMeta.setChanged(backupChanged);

		shell.open();
		while (!shell.isDisposed()) {
			if (!display.readAndDispatch()) {
				display.sleep();
			}
		}

		return stepname;
	}

	
	protected void setComboBoxes() {

		final Map<String, Integer> fields = new HashMap<String, Integer>();

		// Add the currentMeta fields...
		fields.putAll(inputFields);

		Set<String> keySet = fields.keySet();
		List<String> entries = new ArrayList<String>(keySet);

		String[] fieldNames = entries.toArray(new String[entries.size()]);
		Const.sortStrings(fieldNames);
		// pop fields
		ciKey[1].setComboValues(fieldNames);
	}

	
	public void enableFields() {
		wlIsIdempotentSat.setEnabled(hasOneTemporalField);
		wbIsIdempotentSat.setEnabled(hasOneTemporalField);
		wlToDateCol.setEnabled(hasOneTemporalField);
		wcbToDateCol.setEnabled(hasOneTemporalField);
		wlToDateMax.setEnabled(hasOneTemporalField);
		wToDateMax.setEnabled(hasOneTemporalField);

	}

	/*
	 * To reset cacheColumnMap when connection is changed at the UI
	 */
	private void resetColumnsCache() {
		cacheColumnMap.clear();
	}

	/*
	 * Returns from cache the RowMetaInterface associated to "schema.table" as
	 * entered in UI (populate the cache when "schema.table" not found) Returns
	 * null for any type of Exception (unknown "schema.table", DB error..)
	 */
	private RowMetaInterface getColumnsFromCache(String schemaUI, String tableUI){
		if (Const.isEmpty(tableUI)){
			return null;
		}

		String key = (Const.isEmpty(schemaUI) ? "" : schemaUI ) + "." + tableUI;
		
		if (cacheColumnMap.get(key) != null){
			logBasic("Cols return from cache: " + cacheColumnMap.get(key).toStringMeta() );
			return cacheColumnMap.get(key);
		} else {
			//fetch DB data 
			String connectionName = wConnection.getText();
			DatabaseMeta ci = transMeta.findDatabase(connectionName);
			if (ci != null){
				Database db = new Database(loggingObject, ci);
				try {
					db.connect();
					String schemaTable = ci.getQuotedSchemaTableCombination(
							transMeta.environmentSubstitute(schemaUI),
							transMeta.environmentSubstitute(tableUI));
					RowMetaInterface found = db.getTableFields(schemaTable);

					if (found != null) {
						cacheColumnMap.put(key,found);
						logBasic("new Cols from table : " + key +  " now in cache : " + found.toStringMeta()); 
						return found;
					} 
				} catch (Exception e) {
					logDebug("Error connecting to DB for caching column names", e);
					return null;
				}
			}
		}
		return null;
	}

	private void setToDateColumns() {
		// clear and reset..
		wcbToDateCol.removeAll();
		RowMetaInterface toCols = null;
		toCols = getColumnsFromCache(wSchema.getText(),wSatTable.getText() );	

		if (toCols != null){
			wcbToDateCol.add(LoadSatMeta.NA);
			for (int i = 0; i < toCols.getFieldNames().length; i++){
				wcbToDateCol.add(toCols.getFieldNames()[i]);	
			}
		} else {
			wcbToDateCol.add("");
		}
	}


	
	private void setTableFieldCombo() {
		Runnable fieldLoader = new Runnable() {
			public void run() {

				if (!wSatTable.isDisposed() && !wConnection.isDisposed() && !wSchema.isDisposed()) {
					String tableName = wSatTable.getText();
					String schemaName = wSchema.getText();

					// clear
					for (ColumnInfo colInfo : tableFieldColumns) {
						colInfo.setComboValues(new String[] {});
					}
					
					RowMetaInterface r = getColumnsFromCache(schemaName, tableName);
					if (r != null && r.getFieldNames() != null){
						String[] fieldNames = r.getFieldNames();
						for (ColumnInfo colInfo : tableFieldColumns) {
								colInfo.setComboValues(fieldNames);
						}
					} else {
						for (ColumnInfo colInfo : tableFieldColumns) {
							colInfo.setComboValues(new String[] {});	
						}
					}
					
				}
			}
		};
		shell.getDisplay().asyncExec(fieldLoader);
	}

	
	/**
	 * Copy information from the meta-data input to the dialog fields.
	 * 
	 */
	public void getData() {

		if (inputMeta.getDatabaseMeta() != null) {
			wConnection.setText(inputMeta.getDatabaseMeta().getName());
		} else if (transMeta.nrDatabases() == 1) {
			wConnection.setText(transMeta.getDatabase(0).getName());
		}

		if (inputMeta.getSchemaName() != null) {
			wSchema.setText(inputMeta.getSchemaName());
		}

		if (inputMeta.getSatTable() != null) {
			wSatTable.setText(inputMeta.getSatTable());
		}

		hasOneTemporalField = inputMeta.getFromDateColumn() != null ; 
		enableFields();


		wBatchSize.setText("" + inputMeta.getBufferSize());

		if (inputMeta.getAttField() != null) {
			for (int i = 0; i < inputMeta.getAttField().length; i++) {
				TableItem item = wKey.table.getItem(i);
				if (inputMeta.getAttCol()[i] != null) {
					item.setText(1, inputMeta.getAttCol()[i]);
				}
				if (inputMeta.getAttField()[i] != null) {
					item.setText(2, inputMeta.getAttField()[i]);
				}
				if (inputMeta.getAttType()[i] != null) {
					item.setText(3, inputMeta.getAttType()[i]);
				}
				
			}
		}

		if (inputMeta.getToDateColumn() != null) {
			wcbToDateCol.setText(inputMeta.getToDateColumn());
		}	
		if (inputMeta.getToDateMaxFlag() != null) {
			wToDateMax.setText(inputMeta.getToDateMaxFlag());
		}	
		wbIsIdempotentSat.setSelection(inputMeta.isIdempotent());

		wKey.setRowNums();
		wKey.optWidth(true);

		wStepname.selectAll();
		wStepname.setFocus();
	}

	private void cancel() {
		stepname = null;
		inputMeta.setChanged(backupChanged);
		dispose();
	}

	private void ok() {
		if (Const.isEmpty(wStepname.getText())) {
			return;
		}

		LoadSatMeta oldMetaState = (LoadSatMeta) inputMeta.clone();

		getInfo(inputMeta);
		stepname = wStepname.getText(); // return value

		if (transMeta.findDatabase(wConnection.getText()) == null) {
			MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
			mb.setMessage(BaseMessages.getString(PKG, "LoadDialog.NoValidConnection.DialogMessage"));
			mb.setText(BaseMessages.getString(PKG, "LoadDialog.NoValidConnection.DialogTitle"));
			mb.open();
		}
		if (!inputMeta.equals(oldMetaState)) {
			inputMeta.setChanged();
		}
		dispose();
	}

	/*
	 * Update the Meta object according to UI widgets
	 */
	private void getInfo(LoadSatMeta inMeta) {

		inMeta.setDatabaseMeta(transMeta.findDatabase(wConnection.getText()));
		inMeta.setSchemaName(wSchema.getText());
		inMeta.setSatTable(wSatTable.getText());
		inMeta.setBufferSize(Const.toInt(wBatchSize.getText(), 0));

		int nrkeys = wKey.nrNonEmpty();
		inMeta.allocateArray(nrkeys);

		logDebug("Found nb of Keys=", String.valueOf(nrkeys));
		
		//in case temporal not set, then null is used as flag
		inMeta.setFromDateColumn(null);
		
		for (int i = 0; i < nrkeys; i++) {
			TableItem item = wKey.getNonEmpty(i);
			inMeta.getAttCol()[i] = item.getText(1);
			inMeta.getAttField()[i] = item.getText(2);
			String t = item.getText(3);
			//Unknown category is default to Normal 
			if (!(t.equals(LoadSatMeta.ATTRIBUTE_NORMAL)) &&
					!(t.equals(LoadSatMeta.ATTRIBUTE_SURR_FK)) &&
					!(t.equals(LoadSatMeta.ATTRIBUTE_TEMPORAL))){
				t = LoadSatMeta.ATTRIBUTE_NORMAL;
			}
			inMeta.getAttType()[i] = t;
			
			//first temporal found is the one we keep
			if (item.getText(3).equals(LoadSatMeta.ATTRIBUTE_TEMPORAL)){
				inMeta.setFromDateColumn(LoadSatMeta.ATTRIBUTE_TEMPORAL);
			}
		}
		
		inMeta.setToDateColumn(wcbToDateCol.getText());
		inMeta.setToDateMaxFlag(wToDateMax.getText());
		inMeta.setIdempotent(wbIsIdempotentSat.getSelection());
		
	}

	private void getSchemaNames() {
		DatabaseMeta databaseMeta = transMeta.findDatabase(wConnection.getText());
		if (databaseMeta != null) {
			Database database = new Database(loggingObject, databaseMeta);
			try {
				database.connect();
				String[] schemas = database.getSchemas();

				if (null != schemas && schemas.length > 0) {
					schemas = Const.sortStrings(schemas);
					EnterSelectionDialog dialog = new EnterSelectionDialog(shell, schemas, BaseMessages.getString(PKG,
							"LoadDialog.AvailableSchemas.Title", wConnection.getText()), BaseMessages.getString(PKG,
							"LoadDialog.AvailableSchemas.Message", wConnection.getText()));
					String d = dialog.open();
					if (d != null) {
						wSchema.setText(Const.NVL(d, ""));
						setTableFieldCombo();
					}

				} else {
					MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
					mb.setMessage(BaseMessages.getString(PKG, "LoadDialog.NoSchema.Error"));
					mb.setText(BaseMessages.getString(PKG, "LoadDialog.GetSchemas.Error"));
					mb.open();
				}
			} catch (Exception e) {
				new ErrorDialog(shell, BaseMessages.getString(PKG, "System.Dialog.Error.Title"),
						BaseMessages.getString(PKG, "LoadDialog.ErrorGettingSchemas"), e);
			} finally {
				database.disconnect();
			}
		}
	}

	private void getTableName() {
		DatabaseMeta inf = null;
		// New class: SelectTableDialog
		int connr = wConnection.getSelectionIndex();
		if (connr >= 0) {
			inf = transMeta.getDatabase(connr);
		}

		if (inf != null) {
			logDebug("Looking at connection: ", inf.toString());

			DatabaseExplorerDialog std = new DatabaseExplorerDialog(shell, SWT.NONE, inf, transMeta.getDatabases());
			std.setSelectedSchemaAndTable(wSchema.getText(), wSatTable.getText());
			if (std.open()) {
				wSchema.setText(Const.NVL(std.getSchemaName(), ""));
				wSatTable.setText(Const.NVL(std.getTableName(), ""));
				setTableFieldCombo();
			}
		} else {
			MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
			mb.setMessage(BaseMessages.getString(PKG, "LoadDialog.ConnectionError2.DialogMessage"));
			mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
			mb.open();
		}
	}

	private void getFieldsFromInput() {
		try {
			RowMetaInterface r = transMeta.getPrevStepFields(stepname);
			if (r != null && !r.isEmpty()) {
				BaseStepDialog.getFieldsFromPrevious(r, wKey, 1, new int[] { 1, 2 }, new int[] {}, -1, -1,
						new TableItemInsertListener() {
							public boolean tableItemInserted(TableItem tableItem, ValueMetaInterface v) {
								tableItem.setText(3, "N");
								return true;
							}
						});
			}
		} catch (KettleException ke) {
			new ErrorDialog(shell, BaseMessages.getString(PKG, "LoadDialog.UnableToGetFieldsError.DialogTitle"),
					BaseMessages.getString(PKG, "LoadDialog.UnableToGetFieldsError.DialogMessage"), ke);
		}
	}

}
