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
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
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

import plugin.mo.trans.steps.common.BaseLoadMeta;
import plugin.mo.trans.steps.common.CompositeValues;
import plugin.mo.trans.steps.loadsat.LoadSatMeta;

/**
 * 
 *  @author mouellet
 *  
 */
public class LoadSatDialog extends BaseStepDialog implements StepDialogInterface {
	private static Class<?> PKG = BaseLoadMeta.class;
	
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

	private Label wlToDateCol;
	private CCombo wcbToDateCol;
	private Label wlToDateMax;
	private Text wToDateMax;

	private Label wlAuditDTSCol;
	private CCombo wAuditDTSCol;
	private Label wlAuditRecSrcCol;
	private CCombo wAuditRecSrcCol;
	private Label wlAuditRecSrcVal;
	private TextVar wAuditRecSrcVal;


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

		// Batch size ...
		wlBatchSize = new Label(shell, SWT.RIGHT);
		wlBatchSize.setText(BaseMessages.getString(PKG, "LoadDialog.Batchsize.Label"));
		props.setLook(wlBatchSize);
		FormData fdlBatch = new FormData();
		fdlBatch.left = new FormAttachment(0, 0);
		fdlBatch.right = new FormAttachment(middle, -margin);
		fdlBatch.top = new FormAttachment(wSatTable, margin);
		wlBatchSize.setLayoutData(fdlBatch);
		wBatchSize = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wBatchSize);
		wBatchSize.addModifyListener(lsMod);
		FormData fdBatch = new FormData();
		fdBatch.top = new FormAttachment(wSatTable, margin);
		fdBatch.left = new FormAttachment(middle, 0);
		fdBatch.right = new FormAttachment(middle + (100 - middle) / 3, -margin);
		wBatchSize.setLayoutData(fdBatch);

		
		//Idempotent ?
		wlIsIdempotentSat = new Label(shell, SWT.RIGHT);
		wlIsIdempotentSat.setText(BaseMessages.getString(PKG, "LoadSatDialog.IdempotentTransf.Label"));
		props.setLook(wlIsIdempotentSat);
		FormData fdlIdempotent = new FormData();
		fdlIdempotent.left = new FormAttachment(0, 0);
		fdlIdempotent.right = new FormAttachment(middle, -margin);
		fdlIdempotent.top = new FormAttachment(wBatchSize, margin);
		wlIsIdempotentSat.setLayoutData(fdlIdempotent);

		wbIsIdempotentSat = new Button(shell, SWT.CHECK);
		props.setLook(wbIsIdempotentSat);
		FormData fdbExtNatkeyTable = new FormData();
		fdbExtNatkeyTable.left = new FormAttachment(middle, 0);
		fdbExtNatkeyTable.right = new FormAttachment(middle + (100 - middle) / 3, -margin);
		fdbExtNatkeyTable.top = new FormAttachment(wBatchSize, margin);
		wbIsIdempotentSat.setLayoutData(fdbExtNatkeyTable);

				
		//
		// The fields mapping
		//
		wlKey = new Label(shell, SWT.NONE);
		wlKey.setText(BaseMessages.getString(PKG, "LoadSatDialog.Attfields.Label"));
		props.setLook(wlKey);
		FormData fdlKey = new FormData();
		fdlKey.left = new FormAttachment(0, 0);
		fdlKey.top = new FormAttachment(wbIsIdempotentSat, margin*3);
		fdlKey.right = new FormAttachment(100, 0);
		wlKey.setLayoutData(fdlKey);

		int nrKeyCols = 3;
		int nrRows = (inputMeta.getFields() != null ? inputMeta.getFields().length : 1);

		ciKey = new ColumnInfo[nrKeyCols];
		ciKey[0] = new ColumnInfo(BaseMessages.getString(PKG, "LoadSatDialog.ColumnInfo.TableColumn"),
				ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false);
		ciKey[1] = new ColumnInfo(BaseMessages.getString(PKG, "LoadSatDialog.ColumnInfo.FieldInStream"),
				ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false);
		ciKey[2] = new ColumnInfo(BaseMessages.getString(PKG, "LoadSatDialog.ColumnInfo.Type"),
				ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { LoadSatMeta.ATTRIBUTE_NORMAL, 
							LoadSatMeta.ATTRIBUTE_FK , LoadSatMeta.ATTRIBUTE_TEMPORAL });
		
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

		// The Audit Group 
		Group wAuditFields = new Group( shell, SWT.SHADOW_ETCHED_IN ); 
		wAuditFields.setText( BaseMessages.getString( PKG, "LoadDialog.AuditGroupFields.Label" ) );
		FormLayout auditGroupLayout = new FormLayout();
	    auditGroupLayout.marginWidth = 3;
	    auditGroupLayout.marginHeight = 3;
	    wAuditFields.setLayout( auditGroupLayout );
	    props.setLook( wAuditFields );

		
		// Audit RecSrc
		// Audit DTS :
		wlAuditDTSCol = new Label(wAuditFields, SWT.RIGHT);
		wlAuditDTSCol.setText(BaseMessages.getString(PKG, "LoadDialog.AuditDTSField.Label"));
		props.setLook(wlAuditDTSCol);
		FormData fdlLastUpdateField = new FormData();
		fdlLastUpdateField.left = new FormAttachment(0, 0);
		fdlLastUpdateField.right = new FormAttachment(middle, -margin);
		fdlLastUpdateField.top = new FormAttachment(wAuditFields, margin);
		wlAuditDTSCol.setLayoutData(fdlLastUpdateField);
		
		wAuditDTSCol = new CCombo(wAuditFields, SWT.BORDER );
		wAuditDTSCol.setToolTipText(BaseMessages.getString(PKG, "LoadDialog.AuditDTSField.Tooltip"));
		props.setLook(wAuditDTSCol);
		wAuditDTSCol.addModifyListener(lsMod);
		FormData fdLastUpdateField = new FormData();
		fdLastUpdateField.left = new FormAttachment(middle, 0);
		fdLastUpdateField.right = new FormAttachment(100, 0);
		fdLastUpdateField.top = new FormAttachment(wAuditFields, margin);
		wAuditDTSCol.setLayoutData(fdLastUpdateField);
		wAuditDTSCol.addFocusListener(new FocusListener() {
			public void focusLost(FocusEvent arg0) {}
			public void focusGained(FocusEvent arg0) {
				Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
				shell.setCursor(busy);
				String t = wAuditDTSCol.getText();
				setColumnsCombo(wAuditDTSCol, ValueMetaInterface.TYPE_DATE, ValueMetaInterface.TYPE_TIMESTAMP);
				shell.setCursor(null);
				wAuditDTSCol.setText(t);
				busy.dispose();
			}
		});
		
		//RecSrc Col
		wlAuditRecSrcCol = new Label(wAuditFields, SWT.RIGHT);
		wlAuditRecSrcCol.setText(BaseMessages.getString(PKG, "LoadDialog.AuditRecSrcCol.Label"));
		props.setLook(wlAuditRecSrcCol);
		FormData fdlRecSrcField = new FormData();
		fdlRecSrcField.left = new FormAttachment(0, 0);
		fdlRecSrcField.right = new FormAttachment(middle, -margin);
		fdlRecSrcField.top = new FormAttachment(wAuditDTSCol, margin);
		wlAuditRecSrcCol.setLayoutData(fdlRecSrcField);
		
		wAuditRecSrcCol = new CCombo(wAuditFields, SWT.BORDER );
		wAuditRecSrcCol.setToolTipText(BaseMessages.getString(PKG, "LoadDialog.AuditRecField.Tooltip"));
		props.setLook(wAuditRecSrcCol);
		wAuditRecSrcCol.addModifyListener(lsMod);
		FormData fdRecSrcField = new FormData();
		fdRecSrcField.left = new FormAttachment(middle, 0);
		fdRecSrcField.right = new FormAttachment(100, 0);
		fdRecSrcField.top = new FormAttachment(wAuditDTSCol, margin);
		wAuditRecSrcCol.setLayoutData(fdRecSrcField);
		wAuditRecSrcCol.addFocusListener(new FocusListener() {
			public void focusLost(FocusEvent arg0) {}
			public void focusGained(FocusEvent arg0) {
				Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
				shell.setCursor(busy);
				String t = wAuditRecSrcCol.getText();
				setColumnsCombo(wAuditRecSrcCol, ValueMetaInterface.TYPE_STRING, -1);
				shell.setCursor(null);
				wAuditRecSrcCol.setText(t);
				busy.dispose();
			}
		});

	    
	    // RecSrc Value ...
		wlAuditRecSrcVal = new Label(wAuditFields, SWT.RIGHT);
		wlAuditRecSrcVal.setText(BaseMessages.getString(PKG, "LoadDialog.AuditRecSrcVal.Label"));
		props.setLook(wlAuditRecSrcVal);
		FormData fdlRcVal = new FormData();
		fdlRcVal.left = new FormAttachment(0, 0);
		fdlRcVal.right = new FormAttachment(middle, -margin);
		fdlRcVal.top = new FormAttachment(wAuditRecSrcCol, margin);
		wlAuditRecSrcVal.setLayoutData(fdlRcVal);
		
		wAuditRecSrcVal = new TextVar(transMeta, wAuditFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wAuditRecSrcVal);
		wAuditRecSrcVal.addModifyListener(lsMod);
		FormData fdRcVal = new FormData();
		fdRcVal.top = new FormAttachment(wAuditRecSrcCol, margin);
		fdRcVal.left = new FormAttachment(middle, 0);
		fdRcVal.right = new FormAttachment(100, 0);
		wAuditRecSrcVal.setLayoutData(fdRcVal);

		
		//Fixing the Audit group 
	    FormData fdAuditGroup = new FormData();
	    fdAuditGroup.left = new FormAttachment( 0, 0 );
	    fdAuditGroup.right = new FormAttachment( 100, 0 );
	    fdAuditGroup.bottom = new FormAttachment( wOK, -2*margin );
	    wAuditFields.setLayoutData( fdAuditGroup );
	    wAuditFields.setTabList(new Control[] { wAuditRecSrcVal, wAuditRecSrcCol, wAuditDTSCol} );

	    
		// The optional Group for closing "ToDate"
		Group wClosingDateFields = new Group( shell, SWT.SHADOW_ETCHED_IN ); 
		wClosingDateFields.setText( BaseMessages.getString( PKG, "LoadSatDialog.UsingOptToDate.Label" ) );
	    
		FormLayout closingDateGroupLayout = new FormLayout();
	    closingDateGroupLayout.marginWidth = 3;
	    closingDateGroupLayout.marginHeight = 3;
	    wClosingDateFields.setLayout( closingDateGroupLayout );
	    props.setLook( wClosingDateFields );
		
 
		// ToDate Expire Column name...
		wlToDateCol = new Label(wClosingDateFields, SWT.RIGHT);
		wlToDateCol.setText(BaseMessages.getString(PKG, "LoadSatDialog.ToDateExpCol.Label"));
		props.setLook(wlToDateCol);
		FormData fdlNatTable = new FormData();
		fdlNatTable.left = new FormAttachment(0, 0);
		fdlNatTable.right = new FormAttachment(middle, -margin);
		fdlNatTable.top = new FormAttachment(wClosingDateFields, margin);
		wlToDateCol.setLayoutData(fdlNatTable);

		wcbToDateCol = new CCombo(wClosingDateFields, SWT.BORDER | SWT.READ_ONLY);
		props.setLook(wcbToDateCol);
		wcbToDateCol.addModifyListener(lsMod);
		FormData fdToDate = new FormData();
		fdToDate.left = new FormAttachment(middle, 0);
		fdToDate.right = new FormAttachment(middle + (100 - middle) / 2, -margin);
		fdToDate.top = new FormAttachment(wClosingDateFields, margin);
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
		wlToDateMax = new Label(wClosingDateFields, SWT.RIGHT);
	    String flagText = BaseMessages.getString(PKG, "LoadSatDialog.ExpRecFlag.Label");
		wlToDateMax.setText(flagText);
		props.setLook(wlToDateMax);
		FormData fdlToDateMax = new FormData();
		fdlToDateMax.left = new FormAttachment(0, 0);
		fdlToDateMax.right = new FormAttachment(middle, -margin);
		fdlToDateMax.top = new FormAttachment(wcbToDateCol, margin);
		wlToDateMax.setLayoutData(fdlToDateMax);
		
		wToDateMax = new Text(wClosingDateFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wToDateMax);
		wToDateMax.addModifyListener(lsMod);
		FormData fdToDateMax = new FormData();
		fdToDateMax.left = new FormAttachment(middle, 0);
		fdToDateMax.right = new FormAttachment(middle + (100 - middle) / 2, -margin);
		fdToDateMax.top = new FormAttachment(wcbToDateCol, margin);
		wToDateMax.setLayoutData(fdToDateMax);

		Label wlToDateFlag = new Label(wClosingDateFields, SWT.RIGHT);
	    wlToDateFlag.setText("("+LoadSatMeta.DATE_FORMAT+")");
		props.setLook(wlToDateFlag);
		FormData fdlToDateFlag = new FormData();
		fdlToDateFlag.left = new FormAttachment(wToDateMax, margin);
		fdlToDateFlag.top = new FormAttachment(wcbToDateCol, 2*margin);
		wlToDateFlag.setLayoutData(fdlToDateFlag);
		
		
		
		//Fixing the "ClosingDate" group 
	    FormData fdOptGroup = new FormData();
	    fdOptGroup.left = new FormAttachment( 0, 0 );
	    fdOptGroup.right = new FormAttachment( 100, 0 );
	    fdOptGroup.bottom = new FormAttachment( wAuditFields, -2*margin );
	    wClosingDateFields.setLayoutData( fdOptGroup );
	    wClosingDateFields.setTabList(new Control[] { wcbToDateCol, wToDateMax } );

	    
		// to fix the Mapping Grid
		FormData fdKey = new FormData();
		fdKey.left = new FormAttachment(0, 0);
		fdKey.top = new FormAttachment(wlKey, margin);
		fdKey.right = new FormAttachment(100, 0);
		fdKey.bottom = new FormAttachment(wClosingDateFields, -2*margin);
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

	
	private void setColumnsCombo(CCombo combo, int filterType1, int filterType2) {
		// clear and reset..
		combo.removeAll();
		RowMetaInterface surCols = null;

		//ValueMetaInterface
		surCols = getColumnsFromCache(wSchema.getText(),wSatTable.getText() );	
		
		if (surCols != null){
			for (int i = 0; i < surCols.getFieldNames().length; i++){
				if (filterType1 != -1 ){
					if (filterType1 == surCols.getValueMeta(i).getType()){
						combo.add(surCols.getFieldNames()[i]);	
					} else if (filterType2 != -1 && filterType2 == surCols.getValueMeta(i).getType()){
						combo.add(surCols.getFieldNames()[i]);
					}
				} else {
					combo.add(surCols.getFieldNames()[i]);	
				}
			}
		} 
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

		if (inputMeta.getTargetTable() != null) {
			wSatTable.setText(inputMeta.getTargetTable());
		}

		
		if (inputMeta.getAuditDtsCol() != null) {
			wAuditDTSCol.setText(inputMeta.getAuditDtsCol());
		}

		if (inputMeta.getAuditRecSourceCol() != null){
			wAuditRecSrcCol.setText(inputMeta.getAuditRecSourceCol());
		}

		if (inputMeta.getAuditRecSourceValue() != null){
			wAuditRecSrcVal.setText(inputMeta.getAuditRecSourceValue());
		}

		
		hasOneTemporalField = inputMeta.getFromDateColumn() != null ; 
		enableFields();

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
		inMeta.setTargetTable(wSatTable.getText());
		inMeta.setBufferSize(Const.toInt(wBatchSize.getText(), 0));
		inMeta.setAuditDtsCol(wAuditDTSCol.getText());
		inMeta.setAuditRecSourceCol(wAuditRecSrcCol.getText());
		inMeta.setAuditRecSourceValue(wAuditRecSrcVal.getText());
		
		int nrkeys = wKey.nrNonEmpty();
		inMeta.allocateKeyArray(nrkeys);

		logDebug("Found nb of Keys=", String.valueOf(nrkeys));
		
		//in case temporal not set, then null is used as flag
		inMeta.setFromDateColumn(null);
		
		for (int i = 0; i < nrkeys; i++) {
			TableItem item = wKey.getNonEmpty(i);
			inMeta.getCols()[i] = item.getText(1);
			inMeta.getFields()[i] = item.getText(2);
			String t = item.getText(3);
			//Unknown category is default to Normal 
			if (!(t.equals(LoadSatMeta.ATTRIBUTE_NORMAL)) &&
					!(t.equals(LoadSatMeta.ATTRIBUTE_FK)) &&
					!(t.equals(LoadSatMeta.ATTRIBUTE_TEMPORAL))){
				t = LoadSatMeta.ATTRIBUTE_NORMAL;
			}
			inMeta.getTypes()[i] = t;
			
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
