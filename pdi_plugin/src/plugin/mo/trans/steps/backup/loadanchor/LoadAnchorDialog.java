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
package plugin.mo.trans.steps.backup.loadanchor;

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
import org.pentaho.di.ui.core.database.dialog.DatabaseExplorerDialog;
import org.pentaho.di.ui.core.dialog.EnterSelectionDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.trans.step.TableItemInsertListener;

import plugin.mo.trans.steps.common.CompositeValues;

public class LoadAnchorDialog extends BaseStepDialog implements StepDialogInterface {
	private static Class<?> PKG = CompositeValues.class;

	private CCombo wConnection;

	private Label wlSchema;
	private TextVar wSchema;
	private Button wbSchema;
	private FormData fdbSchema;

	private Label wlHubTable;
	private Button wbHubTable;
	private TextVar wHubTable;

	private Label wlExtNatkeyTable;
	private Button wbExtNatkeyTable;

	private Label wlNatkeyTable;
	private Button wbNatkeyTable;
	private TextVar wNatkeyTable;

	private Label wlBatchSize;
	private Text wBatchSize;

	private Label wlSurrKey;
	private CCombo wSurrKey;

	private Label wlSurrForeignKey;
	private CCombo wSurrForeignKey;

	private Group gSurrGroup;
	private FormData fdSurrGroup;

	private Label wlAutoinc;
	private Button wAutoinc;

	private Label wlTableMax;
	private Button wTableMax;

	private Label wlSeqButton;
	private Button wSeqButton;
	private Text wSeq;

	// private Label wlRemoveNatkey;
	// private Button wRemoveNatkey;

	private Label wlKey;
	private TableView wKey;

	private Label wlCreationDateCol;
	private Text wCreationDateCol;

	private Button wGet;
	private Listener lsGet;

	private ColumnInfo[] ciKey;
	private LoadAnchorMeta inputMeta;
	private DatabaseMeta dbMeta;
	private Map<String, Integer> inputFields;
	// used to cache columns any tables for connection.schema
	private Map<String, RowMetaInterface> cacheColumnMap;

	/**
	 * List of ColumnInfo that should have the field names of the selected
	 * database table
	 */
	private List<ColumnInfo> tableFieldColumns = new ArrayList<ColumnInfo>();

	public LoadAnchorDialog(Shell parent, Object in, TransMeta transMeta, String sname) {
		super(parent, (BaseStepMeta) in, transMeta, sname);
		inputMeta = (LoadAnchorMeta) in;
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
		shell.setText(BaseMessages.getString(PKG, "LoadHubDialog.Shell.Title"));

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

		// Hub Table line...
		wlHubTable = new Label(shell, SWT.RIGHT);
		wlHubTable.setText(BaseMessages.getString(PKG, "LoadHubDialog.Target.Label"));
		props.setLook(wlHubTable);
		FormData fdlTable = new FormData();
		fdlTable.left = new FormAttachment(0, 0);
		fdlTable.right = new FormAttachment(middle, -margin);
		fdlTable.top = new FormAttachment(wbSchema, margin);
		wlHubTable.setLayoutData(fdlTable);

		wbHubTable = new Button(shell, SWT.PUSH | SWT.CENTER);
		props.setLook(wbHubTable);
		wbHubTable.setText(BaseMessages.getString(PKG, "LoadDialog.BrowseTable.Button"));
		FormData fdbTable = new FormData();
		fdbTable.right = new FormAttachment(100, 0);
		fdbTable.top = new FormAttachment(wbSchema, margin);
		wbHubTable.setLayoutData(fdbTable);

		wHubTable = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wHubTable);
		wHubTable.addModifyListener(lsTableMod);
		FormData fdTable = new FormData();
		fdTable.left = new FormAttachment(middle, 0);
		fdTable.top = new FormAttachment(wbSchema, margin);
		fdTable.right = new FormAttachment(wbHubTable, -margin);
		wHubTable.setLayoutData(fdTable);

		// Use External Nat-key table ?
		wlExtNatkeyTable = new Label(shell, SWT.RIGHT);
		wlExtNatkeyTable.setText(BaseMessages.getString(PKG, "LoadHubDialog.ExtNatkey.Label"));
		props.setLook(wlExtNatkeyTable);
		FormData fdlExtNatTable = new FormData();
		fdlExtNatTable.left = new FormAttachment(0, 0);
		fdlExtNatTable.right = new FormAttachment(middle, -margin);
		fdlExtNatTable.top = new FormAttachment(wbHubTable, margin);
		wlExtNatkeyTable.setLayoutData(fdlExtNatTable);

		wbExtNatkeyTable = new Button(shell, SWT.CHECK);
		props.setLook(wbExtNatkeyTable);
		FormData fdbExtNatkeyTable = new FormData();
		fdbExtNatkeyTable.left = new FormAttachment(middle, 0);
		fdbExtNatkeyTable.right = new FormAttachment(100, 0);
		fdbExtNatkeyTable.top = new FormAttachment(wbHubTable, margin);
		wbExtNatkeyTable.setLayoutData(fdbExtNatkeyTable);
		wbExtNatkeyTable.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(SelectionEvent e) {
				enableFields();
				setTableFieldCombo();
			}
		});

		// Natkey table line...
		wlNatkeyTable = new Label(shell, SWT.RIGHT);
		wlNatkeyTable.setText(BaseMessages.getString(PKG, "LoadHubDialog.NatKeyTable.Label"));
		props.setLook(wlNatkeyTable);
		FormData fdlNatTable = new FormData();
		fdlNatTable.left = new FormAttachment(0, 0);
		fdlNatTable.right = new FormAttachment(middle, -margin);
		fdlNatTable.top = new FormAttachment(wbExtNatkeyTable, margin);
		wlNatkeyTable.setLayoutData(fdlNatTable);

		wbNatkeyTable = new Button(shell, SWT.PUSH | SWT.CENTER);
		props.setLook(wbNatkeyTable);
		wbNatkeyTable.setText(BaseMessages.getString(PKG, "LoadDialog.BrowseTable.Button"));
		FormData fdbNatTable = new FormData();
		fdbNatTable.right = new FormAttachment(100, 0);
		fdbNatTable.top = new FormAttachment(wbExtNatkeyTable, margin);
		wbNatkeyTable.setLayoutData(fdbNatTable);

		wNatkeyTable = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wNatkeyTable);
		wNatkeyTable.addModifyListener(lsTableMod);
		FormData fdNatTable = new FormData();
		fdNatTable.left = new FormAttachment(middle, 0);
		fdNatTable.top = new FormAttachment(wbExtNatkeyTable, margin);
		fdNatTable.right = new FormAttachment(wbNatkeyTable, -margin);
		wNatkeyTable.setLayoutData(fdNatTable);

		// Batch size ...
		wlBatchSize = new Label(shell, SWT.RIGHT);
		wlBatchSize.setText(BaseMessages.getString(PKG, "LoadDialog.Batchsize.Label"));
		props.setLook(wlBatchSize);
		FormData fdlBatch = new FormData();
		fdlBatch.left = new FormAttachment(0, 0);
		fdlBatch.right = new FormAttachment(middle, -margin);
		fdlBatch.top = new FormAttachment(wbNatkeyTable, margin);
		wlBatchSize.setLayoutData(fdlBatch);
		wBatchSize = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wBatchSize);
		wBatchSize.addModifyListener(lsMod);
		FormData fdBatch = new FormData();
		fdBatch.top = new FormAttachment(wbNatkeyTable, margin);
		fdBatch.left = new FormAttachment(middle, 0);
		fdBatch.right = new FormAttachment(middle + (100 - middle) / 3, -margin);
		wBatchSize.setLayoutData(fdBatch);

		//
		// The Lookup fields: the natural key (business)
		//
		wlKey = new Label(shell, SWT.NONE);
		wlKey.setText(BaseMessages.getString(PKG, "LoadHubDialog.Keyfields.Label"));
		props.setLook(wlKey);
		FormData fdlKey = new FormData();
		fdlKey.left = new FormAttachment(0, 0);
		fdlKey.top = new FormAttachment(wBatchSize, margin*2);
		fdlKey.right = new FormAttachment(100, 0);
		wlKey.setLayoutData(fdlKey);

		int nrKeyCols = 2;
		int nrKeyRows = (inputMeta.getNatKeyField() != null ? inputMeta.getNatKeyField().length : 1);

		ciKey = new ColumnInfo[nrKeyCols];
		ciKey[0] = new ColumnInfo(BaseMessages.getString(PKG, "LoadHubDialog.ColumnInfo.TableColumn"),
				ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false);
		ciKey[1] = new ColumnInfo(BaseMessages.getString(PKG, "LoadHubDialog.ColumnInfo.FieldInStream"),
				ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false);
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
		wlCreationDateCol.setText(BaseMessages.getString(PKG, "LoadDialog.AuditDTSField.Label"));
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
				inputMeta.setSurrKeyCreation(LoadAnchorMeta.CREATION_METHOD_SEQUENCE);
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

		// Optional FK surrogate key column:
		wlSurrForeignKey = new Label(shell, SWT.RIGHT);
		wlSurrForeignKey.setText(BaseMessages.getString(PKG, "LoadHubDialog.SurrForeignKey.Label"));
		props.setLook(wlSurrForeignKey);
		FormData fdlSurrForeignKey = new FormData();
		fdlSurrForeignKey.left = new FormAttachment(0, 0);
		fdlSurrForeignKey.right = new FormAttachment(middle, -margin);
		fdlSurrForeignKey.bottom = new FormAttachment(gSurrGroup, -margin);
		wlSurrForeignKey.setLayoutData(fdlSurrForeignKey);
		
		wSurrForeignKey = new CCombo(shell, SWT.BORDER | SWT.READ_ONLY);
		props.setLook(wSurrForeignKey);
		wSurrForeignKey.addModifyListener(lsMod);
		FormData fdSurrForeignKey = new FormData();
		fdSurrForeignKey.left = new FormAttachment(middle, 0);
		fdSurrForeignKey.right = new FormAttachment(100, 0);
		fdSurrForeignKey.bottom = new FormAttachment(gSurrGroup, -margin);
		wSurrForeignKey.setLayoutData(fdSurrForeignKey);
		wSurrForeignKey.addFocusListener(new FocusListener() {
			public void focusLost(FocusEvent arg0) {}
			public void focusGained(FocusEvent arg0) {
				Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
				shell.setCursor(busy);
				setColumnsCombo(wSurrForeignKey);
				shell.setCursor(null);
				busy.dispose();
			}
		});

		
		// Surrogate key column:
		wlSurrKey = new Label(shell, SWT.RIGHT);
		wlSurrKey.setText(BaseMessages.getString(PKG, "LoadHubDialog.SurrKey.Label"));
		props.setLook(wlSurrKey);
		FormData fdlTk = new FormData();
		fdlTk.left = new FormAttachment(0, 0);
		fdlTk.right = new FormAttachment(middle, -margin);
		fdlTk.bottom = new FormAttachment(wSurrForeignKey, -margin);
		wlSurrKey.setLayoutData(fdlTk);

		wSurrKey = new CCombo(shell, SWT.BORDER | SWT.READ_ONLY);
		props.setLook(wSurrKey);
		// set its listener
		wSurrKey.addModifyListener(lsMod);
		FormData fdTk = new FormData();
		fdTk.left = new FormAttachment(middle, 0);
		fdTk.bottom = new FormAttachment(wSurrForeignKey, -margin);
		fdTk.right = new FormAttachment(100, 0);
		wSurrKey.setLayoutData(fdTk);
		wSurrKey.addFocusListener(new FocusListener() {
			public void focusLost(FocusEvent arg0) {}
			public void focusGained(FocusEvent arg0) {
				Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
				shell.setCursor(busy);
				setColumnsCombo(wSurrKey);
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
		wHubTable.addSelectionListener(lsDef);
		wBatchSize.addSelectionListener(lsDef);
		wSeq.addSelectionListener(lsDef);
		wSurrKey.addSelectionListener(lsDef);
		wNatkeyTable.addSelectionListener(lsDef);

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

		wbHubTable.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(SelectionEvent e) {
				getTableName(wHubTable);
			}
		});

		wbNatkeyTable.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(SelectionEvent e) {
				getTableName(wNatkeyTable);
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

	public void enableFields() {
		wlNatkeyTable.setEnabled(wbExtNatkeyTable.getSelection());
		wNatkeyTable.setEnabled(wbExtNatkeyTable.getSelection());
		wNatkeyTable.setVisible(wbExtNatkeyTable.getSelection());
		wbNatkeyTable.setEnabled(wbExtNatkeyTable.getSelection());
		wbNatkeyTable.setVisible(wbExtNatkeyTable.getSelection());

		wlSurrForeignKey.setEnabled(wbExtNatkeyTable.getSelection());
		wSurrForeignKey.setEnabled(wbExtNatkeyTable.getSelection());
		wSurrForeignKey.setVisible(wbExtNatkeyTable.getSelection());

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

	
	private void setColumnsCombo(CCombo combo) {
		// clear and reset..
		if (combo != null){
			combo.removeAll();

			RowMetaInterface surCols = null;
			if (combo == wSurrKey){
				surCols = getColumnsFromCache(wSchema.getText(),wHubTable.getText() );	
			} else {
				surCols = getColumnsFromCache(wSchema.getText(),wNatkeyTable.getText() );
			}
			
			if (surCols != null){
				for (int i = 0; i < surCols.getFieldNames().length; i++){
					combo.add(surCols.getFieldNames()[i]);
				}
			} 
		}
	}

	
	
	private void setTableFieldCombo() {
		Runnable fieldLoader = new Runnable() {
			public void run() {
				// get the "right" table to fetch from
				TextVar hubOrNatkey = wbExtNatkeyTable.getSelection() ? wNatkeyTable : wHubTable;

				if (!hubOrNatkey.isDisposed() && !wConnection.isDisposed() && !wSchema.isDisposed()) {
					String tableName = hubOrNatkey.getText();
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

		if (inputMeta.getHubTable() != null) {
			wHubTable.setText(inputMeta.getHubTable());
		}

		wbExtNatkeyTable.setSelection(inputMeta.getNatkeyTable() != null);
		enableFields();

		if (inputMeta.getNatkeyTable() != null) {
			wNatkeyTable.setText(inputMeta.getNatkeyTable());
		}

		wBatchSize.setText("" + inputMeta.getBufferSize());

		if (inputMeta.getNatKeyField() != null) {
			for (int i = 0; i < inputMeta.getNatKeyField().length; i++) {
				TableItem item = wKey.table.getItem(i);
				if (inputMeta.getNatKeyCol()[i] != null) {
					item.setText(1, inputMeta.getNatKeyCol()[i]);
				}
				if (inputMeta.getNatKeyField()[i] != null) {
					item.setText(2, inputMeta.getNatKeyField()[i]);
				}
			}
		}

		if (inputMeta.getSurrPKeyColumn() != null) {
			wSurrKey.setText(inputMeta.getSurrPKeyColumn());
		}

		if (inputMeta.getSurrFKeyInNatkeyTable() != null) {
			wSurrForeignKey.setText(inputMeta.getSurrFKeyInNatkeyTable());
		}

		// wRemoveNatkey.setSelection(input.isRemoveNatkeyFields());
		wCreationDateCol.setText(Const.NVL(inputMeta.getCreationDateCol(), ""));

		String surrKeyCreation = inputMeta.getSurrKeyCreation();

		if (LoadAnchorMeta.CREATION_METHOD_AUTOINC.equals(surrKeyCreation)) {
			wAutoinc.setSelection(true);
		} else if ((LoadAnchorMeta.CREATION_METHOD_SEQUENCE.equals(surrKeyCreation))) {
			wSeqButton.setSelection(true);
		} else { // TableMax is also the default when no creation is yet defined
			wTableMax.setSelection(true);
			inputMeta.setSurrKeyCreation(LoadAnchorMeta.CREATION_METHOD_TABLEMAX);
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

		LoadAnchorMeta oldMetaState = (LoadAnchorMeta) inputMeta.clone();

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
	private void getInfo(LoadAnchorMeta in) {

		in.setDatabaseMeta(transMeta.findDatabase(wConnection.getText()));
		in.setSchemaName(wSchema.getText());
		in.setHubTable(wHubTable.getText());
		in.setSurrPKeyColumn(wSurrKey.getText());

		if (wbExtNatkeyTable.getSelection()) {
			in.setNatkeyTable(wNatkeyTable.getText());
			in.setSurrFKeyInNatkeyTable(wSurrForeignKey.getText());
		} else {
			// flag indicating NOT using separate table
			in.setNatkeyTable(null);
			in.setSurrFKeyInNatkeyTable(null);
		}
		
		in.setBufferSize(Const.toInt(wBatchSize.getText(), 0));

		int nrkeys = wKey.nrNonEmpty();
		in.allocateKeyArray(nrkeys);

		logDebug("Found nb of Keys=", String.valueOf(nrkeys));
		for (int i = 0; i < nrkeys; i++) {
			TableItem item = wKey.getNonEmpty(i);
			in.getNatKeyCol()[i] = item.getText(1);
			in.getNatKeyField()[i] = item.getText(2);
		}

		if (wAutoinc.getSelection()) {
			in.setSurrKeyCreation(LoadAnchorMeta.CREATION_METHOD_AUTOINC);
		} else if (wSeqButton.getSelection()) {
			in.setSurrKeyCreation(LoadAnchorMeta.CREATION_METHOD_SEQUENCE);
			in.setSequenceName(wSeq.getText());
		} else { // TableMax
			in.setSurrKeyCreation(LoadAnchorMeta.CREATION_METHOD_TABLEMAX);
		}

		// in.setRemoveNatkeyFields(wRemoveNatkey.getSelection());
		in.setCreationDateCol(wCreationDateCol.getText());
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
						setColumnsCombo(wSurrKey);
						setColumnsCombo(wSurrForeignKey);
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

	private void getTableName(TextVar hubOrNatkey) {
		DatabaseMeta inf = null;
		// New class: SelectTableDialog
		int connr = wConnection.getSelectionIndex();
		if (connr >= 0) {
			inf = transMeta.getDatabase(connr);
		}

		if (inf != null) {
			logDebug("Looking at connection: ", inf.toString());

			DatabaseExplorerDialog std = new DatabaseExplorerDialog(shell, SWT.NONE, inf, transMeta.getDatabases());
			std.setSelectedSchemaAndTable(wSchema.getText(), hubOrNatkey.getText());
			if (std.open()) {
				wSchema.setText(Const.NVL(std.getSchemaName(), ""));
				hubOrNatkey.setText(Const.NVL(std.getTableName(), ""));
				setTableFieldCombo();
				if (hubOrNatkey == wHubTable){
					setColumnsCombo(wSurrKey);	
				} else {
					setColumnsCombo(wSurrForeignKey);	
				}
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
