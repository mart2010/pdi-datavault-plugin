package plugin.mo.trans.steps.loadlink.ui;

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
import org.pentaho.di.trans.steps.googleanalytics.GaInputStepMeta;
import org.pentaho.di.ui.core.database.dialog.DatabaseExplorerDialog;
import org.pentaho.di.ui.core.dialog.EnterSelectionDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.trans.step.TableItemInsertListener;

import plugin.mo.trans.steps.common.BaseLoadMeta;
import plugin.mo.trans.steps.common.CompositeValues;
import plugin.mo.trans.steps.loadhub.LoadHubMeta;
import plugin.mo.trans.steps.loadlink.LoadLinkMeta;

/*
 * 
 * Responsible for the creation/setup of all widgets. 
 * Dialog elements created in order of appearance on dialog (top to bottom) 
 * Each widget consists of 1- its label and 2- the entry widget itself
 * FormData objects define the anchor points of the widgets
 * 
 * TODO: check out GoogleAnalytics for good UI design
 */

public class LoadLinkDialog extends BaseStepDialog implements StepDialogInterface {
	private static Class<?> PKG = CompositeValues.class;
	

	private CCombo wConnection;

	private Label wlSchema;
	private TextVar wSchema;
	private Button wbSchema;
	private FormData fdbSchema;

	private Label wlLinkTable;
	private Button wbLinkTable;
	private TextVar wLinkTable;

	private Label wlBatchSize;
	private Text wBatchSize;

	private Label wlSurrKey;
	private CCombo wSurrKey;

	private Group gSurrGroup;
	private FormData fdSurrGroup;

	private Label wlAutoinc;
	private Button wAutoinc;

	private Label wlTableMax;
	private Button wTableMax;

	private Label wlSeqButton;
	private Button wSeqButton;
	private Text wSeq;

	private Label wlKey;
	private TableView wKey;

	private Label wlCreationDateCol;
	private Text wCreationDateCol;

	private Button wGet;
	private Listener lsGet;

	private ColumnInfo[] ciKey;
	private LoadLinkMeta inputMeta;
	private DatabaseMeta dbMeta;
	private Map<String, Integer> inputFields;
	// used to cache columns any tables for connection.schema
	private Map<String, RowMetaInterface> cacheColumnMap;

	/**
	 * List of ColumnInfo that should have the field names of the selected
	 * database table
	 */
	private List<ColumnInfo> tableFieldColumns = new ArrayList<ColumnInfo>();

	public LoadLinkDialog(Shell parent, Object in, TransMeta transMeta, String sname) {
		super(parent, (BaseStepMeta) in, transMeta, sname);
		inputMeta = (LoadLinkMeta) in;
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
		shell.setText(BaseMessages.getString(PKG, "LoadLinkDialog.Shell.Title"));

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
				setAutoincUse();
				setSequence();
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

		// Table line...
		wlLinkTable = new Label(shell, SWT.RIGHT);
		wlLinkTable.setText(BaseMessages.getString(PKG, "LoadLinkDialog.Target.Label"));
		props.setLook(wlLinkTable);
		FormData fdlTable = new FormData();
		fdlTable.left = new FormAttachment(0, 0);
		fdlTable.right = new FormAttachment(middle, -margin);
		fdlTable.top = new FormAttachment(wbSchema, margin);
		wlLinkTable.setLayoutData(fdlTable);

		wbLinkTable = new Button(shell, SWT.PUSH | SWT.CENTER);
		props.setLook(wbLinkTable);
		wbLinkTable.setText(BaseMessages.getString(PKG, "LoadDialog.BrowseTable.Button"));
		FormData fdbTable = new FormData();
		fdbTable.right = new FormAttachment(100, 0);
		fdbTable.top = new FormAttachment(wbSchema, margin);
		wbLinkTable.setLayoutData(fdbTable);

		wLinkTable = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wLinkTable);
		wLinkTable.addModifyListener(lsTableMod);
		FormData fdTable = new FormData();
		fdTable.left = new FormAttachment(middle, 0);
		fdTable.top = new FormAttachment(wbSchema, margin);
		fdTable.right = new FormAttachment(wbLinkTable, -margin);
		wLinkTable.setLayoutData(fdTable);


		// Batch size ...
		wlBatchSize = new Label(shell, SWT.RIGHT);
		wlBatchSize.setText(BaseMessages.getString(PKG, "LoadDialog.Batchsize.Label"));
		props.setLook(wlBatchSize);
		FormData fdlBatch = new FormData();
		fdlBatch.left = new FormAttachment(0, 0);
		fdlBatch.right = new FormAttachment(middle, -margin);
		fdlBatch.top = new FormAttachment(wLinkTable, margin);
		wlBatchSize.setLayoutData(fdlBatch);
		wBatchSize = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wBatchSize);
		wBatchSize.addModifyListener(lsMod);
		FormData fdBatch = new FormData();
		fdBatch.top = new FormAttachment(wLinkTable, margin);
		fdBatch.left = new FormAttachment(middle, 0);
		fdBatch.right = new FormAttachment(middle + (100 - middle) / 3, -margin);
		wBatchSize.setLayoutData(fdBatch);

		//
		// The fields: keys + none-keys
		//
		wlKey = new Label(shell, SWT.NONE);
		wlKey.setText(BaseMessages.getString(PKG, "LoadLinkDialog.Keyfields.Label"));
		props.setLook(wlKey);
		FormData fdlKey = new FormData();
		fdlKey.left = new FormAttachment(0, 0);
		fdlKey.top = new FormAttachment(wBatchSize, margin*2);
		fdlKey.right = new FormAttachment(100, 0);
		wlKey.setLayoutData(fdlKey);

		int nrKeyCols = 3;
		int nrKeyRows = (inputMeta.getFields() != null ? inputMeta.getFields().length : 1);

		ciKey = new ColumnInfo[nrKeyCols];		
		ciKey[0] = new ColumnInfo(BaseMessages.getString(PKG, "LoadLinkDialog.ColumnInfo.TableColumn"),
				ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false);
		ciKey[1] = new ColumnInfo(BaseMessages.getString(PKG, "LoadLinkDialog.ColumnInfo.FieldInStream"),
				ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false);
		ciKey[2] = new ColumnInfo(BaseMessages.getString(PKG, "LoadLinkDialog.ColumnInfo.FieldType"),
				ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { LoadLinkMeta.IDENTIFYING_KEY, LoadLinkMeta.OTHER_TYPE }, false);
		
		// attach the tableFieldColumns List to the widget
		tableFieldColumns.add(ciKey[0]);
		wKey = new TableView(transMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL
				| SWT.H_SCROLL, ciKey, nrKeyRows, lsMod, props);

		// THE BUTTONS
		wOK = new Button(shell, SWT.PUSH);
		wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));
		wGet = new Button(shell, SWT.PUSH);
		wGet.setText(BaseMessages.getString(PKG, "LoadDialog.GetFields.Button"));
		wCancel = new Button(shell, SWT.PUSH);
		wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

		setButtonPositions(new Button[] { wOK, wCancel, wGet }, margin, null);

		// Creation Date :
		wlCreationDateCol = new Label(shell, SWT.RIGHT);
		wlCreationDateCol.setText(BaseMessages.getString(PKG, "LoadDialog.CreationDateField.Label"));
		props.setLook(wlCreationDateCol);
		FormData fdlLastUpdateField = new FormData();
		fdlLastUpdateField.left = new FormAttachment(0, 0);
		fdlLastUpdateField.right = new FormAttachment(middle, -margin);
		fdlLastUpdateField.bottom = new FormAttachment(wOK, -2 * margin);
		wlCreationDateCol.setLayoutData(fdlLastUpdateField);
		wCreationDateCol = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wCreationDateCol);
		wCreationDateCol.addModifyListener(lsMod);
		FormData fdLastUpdateField = new FormData();
		fdLastUpdateField.left = new FormAttachment(middle, 0);
		fdLastUpdateField.right = new FormAttachment(100, 0);
		fdLastUpdateField.bottom = new FormAttachment(wOK, -2 * margin);
		wCreationDateCol.setLayoutData(fdLastUpdateField);


		// Creation of surrogate key
		gSurrGroup = new Group(shell, SWT.SHADOW_ETCHED_IN);
		gSurrGroup.setText(BaseMessages.getString(PKG, "LoadDialog.SurrGroup.Label"));
		GridLayout gridLayout = new GridLayout(3, false);
		gSurrGroup.setLayout(gridLayout);
		fdSurrGroup = new FormData();
		fdSurrGroup.left = new FormAttachment(middle, 0);
		fdSurrGroup.bottom = new FormAttachment(wCreationDateCol, -margin);
		fdSurrGroup.right = new FormAttachment(100, 0);
		gSurrGroup.setBackground(shell.getBackground());

		gSurrGroup.setLayoutData(fdSurrGroup);

		// Use maximum of table + 1
		wTableMax = new Button(gSurrGroup, SWT.RADIO);
		props.setLook(wTableMax);
		wTableMax.setSelection(false);
		GridData gdTableMax = new GridData();
		wTableMax.setLayoutData(gdTableMax);
		wTableMax.setToolTipText(BaseMessages.getString(PKG, "LoadDialog.TableMaximum.Tooltip", Const.CR));
		wlTableMax = new Label(gSurrGroup, SWT.LEFT);
		wlTableMax.setText(BaseMessages.getString(PKG, "LoadDialog.TableMaximum.Label"));
		props.setLook(wlTableMax);
		GridData gdlTableMax = new GridData(GridData.FILL_BOTH);
		gdlTableMax.horizontalSpan = 2;
		gdlTableMax.verticalSpan = 1;
		wlTableMax.setLayoutData(gdlTableMax);

		// Sequence Check Button
		wSeqButton = new Button(gSurrGroup, SWT.RADIO);
		props.setLook(wSeqButton);
		wSeqButton.setSelection(false);
		GridData gdSeqButton = new GridData();
		wSeqButton.setLayoutData(gdSeqButton);
		wSeqButton.setToolTipText(BaseMessages.getString(PKG, "LoadDialog.Sequence.Tooltip", Const.CR));
		wlSeqButton = new Label(gSurrGroup, SWT.LEFT);
		wlSeqButton.setText(BaseMessages.getString(PKG, "LoadDialog.Sequence.Label"));
		props.setLook(wlSeqButton);
		GridData gdlSeqButton = new GridData();
		wlSeqButton.setLayoutData(gdlSeqButton);

		wSeq = new Text(gSurrGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wSeq);
		wSeq.addModifyListener(lsMod);
		GridData gdSeq = new GridData(GridData.FILL_HORIZONTAL);
		wSeq.setLayoutData(gdSeq);
		wSeq.addFocusListener(new FocusListener() {
			public void focusGained(FocusEvent arg0) {
				inputMeta.setKeyGeneration(LoadLinkMeta.CREATION_METHOD_SEQUENCE);
				wSeqButton.setSelection(true);
				wAutoinc.setSelection(false);
				wTableMax.setSelection(false);
			}

			public void focusLost(FocusEvent arg0) {
			}
		});

		// Use an auto-increment field?
		wAutoinc = new Button(gSurrGroup, SWT.RADIO);
		props.setLook(wAutoinc);
		wAutoinc.setSelection(false);
		GridData gdAutoinc = new GridData();
		wAutoinc.setLayoutData(gdAutoinc);
		wAutoinc.setToolTipText(BaseMessages.getString(PKG, "LoadDialog.AutoincButton.Tooltip", Const.CR));
		wlAutoinc = new Label(gSurrGroup, SWT.LEFT);
		wlAutoinc.setText(BaseMessages.getString(PKG, "LoadDialog.Autoincrement.Label"));
		props.setLook(wlAutoinc);
		GridData gdlAutoinc = new GridData();
		wlAutoinc.setLayoutData(gdlAutoinc);

		setTableMax();
		setSequence();
		setAutoincUse();

		
		// Surrogate key column:
		wlSurrKey = new Label(shell, SWT.RIGHT);
		wlSurrKey.setText(BaseMessages.getString(PKG, "LoadLinkDialog.SurrKey.Label"));
		props.setLook(wlSurrKey);
		FormData fdlTk = new FormData();
		fdlTk.left = new FormAttachment(0, 0);
		fdlTk.right = new FormAttachment(middle, -margin);
		fdlTk.bottom = new FormAttachment(gSurrGroup, -margin);
		wlSurrKey.setLayoutData(fdlTk);

		wSurrKey = new CCombo(shell, SWT.BORDER | SWT.READ_ONLY);
		props.setLook(wSurrKey);
		// set its listener
		wSurrKey.addModifyListener(lsMod);
		FormData fdTk = new FormData();
		fdTk.left = new FormAttachment(middle, 0);
		fdTk.bottom = new FormAttachment(gSurrGroup, -margin);
		fdTk.right = new FormAttachment(100, 0);
		wSurrKey.setLayoutData(fdTk);
		wSurrKey.addFocusListener(new FocusListener() {
			public void focusLost(FocusEvent arg0) {}
			public void focusGained(FocusEvent arg0) {
				Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
				shell.setCursor(busy);
				setColumnsCombo();
				shell.setCursor(null);
				busy.dispose();
			}
		});

		// to fix the Grid
		FormData fdKey = new FormData();
		fdKey.left = new FormAttachment(0, 0);
		fdKey.top = new FormAttachment(wlKey, margin);
		fdKey.right = new FormAttachment(100, 0);
		fdKey.bottom = new FormAttachment(wSurrKey, -margin);
		wKey.setLayoutData(fdKey);

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
		wLinkTable.addSelectionListener(lsDef);
		wBatchSize.addSelectionListener(lsDef);
		wSeq.addSelectionListener(lsDef);
		wSurrKey.addSelectionListener(lsDef);

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

		wbLinkTable.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(SelectionEvent e) {
				getTableName();
			}
		});

		
		// Set the shell size, based upon previous time...
		setSize();

		getData();
		setTableFieldCombo();

		inputMeta.setChanged(backupChanged);

		shell.open();
		while (!shell.isDisposed()) {
			if (!display.readAndDispatch()) {
				display.sleep();
			}
		}

		return stepname;
	}

	private void setComboBoxes() {
		// Something was changed in the row.
		//
		final Map<String, Integer> fields = new HashMap<String, Integer>();

		// Add the currentMeta fields...
		fields.putAll(inputFields);

		Set<String> keySet = fields.keySet();
		List<String> entries = new ArrayList<String>(keySet);

		String[] fieldNames = entries.toArray(new String[entries.size()]);
		Const.sortStrings(fieldNames);
		// Key fields
		ciKey[1].setComboValues(fieldNames);

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

	
	private void setColumnsCombo() {
		// clear and reset..
		wSurrKey.removeAll();

		RowMetaInterface surCols = null;
		surCols = getColumnsFromCache(wSchema.getText(),wLinkTable.getText() );	
		
		if (surCols != null){
			for (int i = 0; i < surCols.getFieldNames().length; i++){
				wSurrKey.add(surCols.getFieldNames()[i]);
			}
		} 
	}

	
	
	private void setTableFieldCombo() {
		Runnable fieldLoader = new Runnable() {
			public void run() {

				if (!wLinkTable.isDisposed() && !wConnection.isDisposed() && !wSchema.isDisposed()) {
					String tableName = wLinkTable.getText();
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

	public void setAutoincUse() {
		boolean enable = (dbMeta == null) || dbMeta.supportsAutoinc();
		wlAutoinc.setEnabled(enable);
		wAutoinc.setEnabled(enable);
		if (!enable && wAutoinc.getSelection()) {
			wAutoinc.setSelection(false);
			wSeqButton.setSelection(false);
			wTableMax.setSelection(true);
		}
	}

	public void setTableMax() {
		wlTableMax.setEnabled(true);
		wTableMax.setEnabled(true);
	}

	public void setSequence() {
		boolean seq = (dbMeta == null) || dbMeta.supportsSequences();
		wSeq.setEnabled(seq);
		wlSeqButton.setEnabled(seq);
		wSeqButton.setEnabled(seq);
		if (!seq && wSeqButton.getSelection()) {
			wAutoinc.setSelection(false);
			wSeqButton.setSelection(false);
			wTableMax.setSelection(true);
		}
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

		if (inputMeta.getTargetTable() != null) {
			wLinkTable.setText(inputMeta.getTargetTable());
		}

		wBatchSize.setText("" + inputMeta.getBufferSize());

		if (inputMeta.getFields() != null) {
			for (int i = 0; i < inputMeta.getFields().length; i++) {
				TableItem item = wKey.table.getItem(i);
				if (inputMeta.getCols()[i] != null) {
					item.setText(1, inputMeta.getCols()[i]);
				}
				if (inputMeta.getFields()[i] != null) {
					item.setText(2, inputMeta.getFields()[i]);
				}
				if (inputMeta.getTypes()[i] != null) {
					item.setText(3, inputMeta.getTypes()[i]);
				}

			}
		}

		if (inputMeta.getTechKeyCol() != null) {
			wSurrKey.setText(inputMeta.getTechKeyCol());
		}

		wCreationDateCol.setText(Const.NVL(inputMeta.getAuditDtsCol(), ""));

		String surrKeyCreation = inputMeta.getKeyGeneration();

		if (LoadLinkMeta.CREATION_METHOD_AUTOINC.equals(surrKeyCreation)) {
			wAutoinc.setSelection(true);
		} else if ((LoadLinkMeta.CREATION_METHOD_SEQUENCE.equals(surrKeyCreation))) {
			wSeqButton.setSelection(true);
		} else { // TableMax is also the default when no creation is yet defined
			wTableMax.setSelection(true);
			inputMeta.setKeyGeneration(LoadLinkMeta.CREATION_METHOD_TABLEMAX);
		}
		if (inputMeta.getSequenceName() != null) {
			wSeq.setText(inputMeta.getSequenceName());
		}

		setAutoincUse();
		setSequence();
		setTableMax();

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
		LoadLinkMeta oldMetaState = (LoadLinkMeta) inputMeta.clone();
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
	private void getInfo(LoadLinkMeta in) {

		in.setDatabaseMeta(transMeta.findDatabase(wConnection.getText()));
		in.setSchemaName(wSchema.getText());
		in.setTargetTable(wLinkTable.getText());
		in.setTechKeyCol(wSurrKey.getText());
		
		in.setBufferSize(Const.toInt(wBatchSize.getText(), 0));

		int nb = wKey.nrNonEmpty();
		in.allocateKeyArray(nb);
		
		logDebug("Found nb of Keys=", String.valueOf(nb));

		for (int i = 0; i < nb; i++) {
			TableItem item = wKey.getNonEmpty(i);
			in.getCols()[i] = item.getText(1);
			in.getFields()[i] = item.getText(2);
			in.getTypes()[i] = item.getText(3);
		}
		
		
		if (wAutoinc.getSelection()) {
			in.setKeyGeneration(LoadLinkMeta.CREATION_METHOD_AUTOINC);
		} else if (wSeqButton.getSelection()) {
			in.setKeyGeneration(LoadLinkMeta.CREATION_METHOD_SEQUENCE);
			in.setSequenceName(wSeq.getText());
		} else { // TableMax
			in.setKeyGeneration(LoadHubMeta.CREATION_METHOD_TABLEMAX);
		}

		in.setAuditDtsCol(wCreationDateCol.getText());
		
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
						setColumnsCombo();
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
			logDebug("Looking at connection: " + inf.toString());

			DatabaseExplorerDialog std = new DatabaseExplorerDialog(shell, SWT.NONE, inf, transMeta.getDatabases());
			std.setSelectedSchemaAndTable(wSchema.getText(), wLinkTable.getText());
			if (std.open()) {
				wSchema.setText(Const.NVL(std.getSchemaName(), ""));
				wLinkTable.setText(Const.NVL(std.getTableName(), ""));
				setTableFieldCombo();
				setColumnsCombo();
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
