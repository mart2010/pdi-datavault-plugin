/*
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
 * Copyright (c) 2014 Martin Ouellet
 *
 */
package plugin.dvloader.trans.steps.common;

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
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;


/**
 * Dialog superclass used for all shared UI components
 * @author mouellet
 *
 */
public abstract class BaseLoadDialog extends BaseStepDialog implements StepDialogInterface {
	protected static Class<?> PKG = BaseLoadMeta.class;
	
	protected CCombo wConnection;
	protected Label wlSchema;
	protected TextVar wSchema;
	protected Button wbSchema;
	protected FormData fdbSchema;

	protected Label wlTargetTable;
	protected Button wbTargetTable;
	protected TextVar wTargetTable;

	protected Label wlBatchSize;
	protected Text wBatchSize;
	
	protected Label wlAuditDTSCol;
	protected CCombo wAuditDTSCol;
	protected Label wlAuditRecSrcCol;
	protected CCombo wAuditRecSrcCol;
	protected Label wlAuditRecSrcVal;
	protected TextVar wAuditRecSrcVal;

	protected Button wGet;
	protected Listener lsGet;

	protected ColumnInfo[] ciKey;
	protected BaseLoadMeta inputMeta;
	protected DatabaseMeta dbMeta;
	
	protected Map<String, Integer> inputFields;
	// used to cache columns of tables for connection.schema
	protected Map<String, RowMetaInterface> cacheColumnMap;
	
	protected int middle;
	protected int margin;
	protected ModifyListener lsMod;
	protected Group wAuditFields;
	protected Display display;

	/**
	 * List of ColumnInfo that should have the field names of the selected
	 * database table
	 */
	protected List<ColumnInfo> tableFieldColumns = new ArrayList<ColumnInfo>();

	
	public BaseLoadDialog(Shell parent, Object in, TransMeta transMeta, String stepname) {
		super(parent, (BaseStepMeta) in, transMeta, stepname);
		inputMeta = (BaseLoadMeta) in;
		inputFields = new HashMap<String, Integer>();
		cacheColumnMap = new HashMap<String, RowMetaInterface>();
	}

	
	@Override
	public String open() {
		Shell parent = getParent();
		display = parent.getDisplay();

		shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
		props.setLook(shell);
		setShellImage(shell, inputMeta);

		FormLayout formLayout = new FormLayout();
		formLayout.marginWidth = Const.FORM_MARGIN;
		formLayout.marginHeight = Const.FORM_MARGIN;

		shell.setLayout(formLayout);
		middle = props.getMiddlePct();
		margin = Const.MARGIN;

		lsMod = new ModifyListener() {
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
		wConnection.addSelectionListener(lsSelection);
		

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
		wlTargetTable = new Label(shell, SWT.RIGHT);
		wlTargetTable.setText(BaseMessages.getString(PKG, "LoadLinkDialog.Target.Label"));
		props.setLook(wlTargetTable);
		FormData fdlTable = new FormData();
		fdlTable.left = new FormAttachment(0, 0);
		fdlTable.right = new FormAttachment(middle, -margin);
		fdlTable.top = new FormAttachment(wbSchema, margin);
		wlTargetTable.setLayoutData(fdlTable);

		wbTargetTable = new Button(shell, SWT.PUSH | SWT.CENTER);
		props.setLook(wbTargetTable);
		wbTargetTable.setText(BaseMessages.getString(PKG, "LoadDialog.BrowseTable.Button"));
		FormData fdbTable = new FormData();
		fdbTable.right = new FormAttachment(100, 0);
		fdbTable.top = new FormAttachment(wbSchema, margin);
		wbTargetTable.setLayoutData(fdbTable);

		wTargetTable = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wTargetTable);
		wTargetTable.addModifyListener(lsTableMod);
		FormData fdTable = new FormData();
		fdTable.left = new FormAttachment(middle, 0);
		fdTable.top = new FormAttachment(wbSchema, margin);
		fdTable.right = new FormAttachment(wbTargetTable, -margin);
		wTargetTable.setLayoutData(fdTable);


		// Batch size ...
		wlBatchSize = new Label(shell, SWT.RIGHT);
		wlBatchSize.setText(BaseMessages.getString(PKG, "LoadDialog.Batchsize.Label"));
		props.setLook(wlBatchSize);
		FormData fdlBatch = new FormData();
		fdlBatch.left = new FormAttachment(0, 0);
		fdlBatch.right = new FormAttachment(middle, -margin);
		fdlBatch.top = new FormAttachment(wTargetTable, margin);
		wlBatchSize.setLayoutData(fdlBatch);
		wBatchSize = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wBatchSize);
		wBatchSize.addModifyListener(lsMod);
		FormData fdBatch = new FormData();
		fdBatch.top = new FormAttachment(wTargetTable, margin);
		fdBatch.left = new FormAttachment(middle, 0);
		fdBatch.right = new FormAttachment(middle + (100 - middle) / 3, -margin);
		wBatchSize.setLayoutData(fdBatch);

		// THE BUTTONS
		wOK = new Button(shell, SWT.PUSH);
		wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));
		wGet = new Button(shell, SWT.PUSH);
		wGet.setText(BaseMessages.getString(PKG, "LoadDialog.GetFields.Button"));
		wCancel = new Button(shell, SWT.PUSH);
		wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
		wCreate = new Button(shell, SWT.PUSH);
		wCreate.setText(BaseMessages.getString(PKG, "System.Button.SQL"));
		
		setButtonPositions(new Button[] { wOK, wCancel, wGet, wCreate }, margin, null);

		// The Audit Group 
		wAuditFields = new Group( shell, SWT.SHADOW_ETCHED_IN ); 
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

		// Add listeners
		lsOK = new Listener() {
			public void handleEvent(Event e) {
				ok();
			}
		};
		lsCreate = new Listener() {
			public void handleEvent(Event e) {
				sql();
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
		wCreate.addListener(SWT.Selection, lsCreate);

		lsDef = new SelectionAdapter() {
			public void widgetDefaultSelected(SelectionEvent e) {
				ok();
			}
		};

		wStepname.addSelectionListener(lsDef);
		wSchema.addSelectionListener(lsDef);
		wTargetTable.addSelectionListener(lsDef);
		wBatchSize.addSelectionListener(lsDef);
		wAuditDTSCol.addSelectionListener(lsDef);
		wAuditRecSrcCol.addSelectionListener(lsDef);
		wAuditRecSrcVal.addSelectionListener(lsDef);

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

		wbTargetTable.addSelectionListener(new SelectionAdapter() {
			public void widgetSelected(SelectionEvent e) {
				getTableName();
			}
		});

		return stepname;
	}

	
	protected void setTableFieldCombo() {
		Runnable fieldLoader = new Runnable() {
			public void run() {

				if (!wTargetTable.isDisposed() && !wConnection.isDisposed() && !wSchema.isDisposed()) {
					String tableName = wTargetTable.getText();
					String schemaName = wSchema.getText();

					// clear
					for (ColumnInfo colInfo : tableFieldColumns) {
						colInfo.setComboValues(new String[] {});
					}

					RowMetaInterface r = getColumnsFromCache(schemaName, tableName);
					if (r != null && r.getFieldNames() != null) {
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

	
	protected void setColumnsCombo(CCombo combo, int filterType1, int filterType2) {
		// clear and reset..
		combo.removeAll();
		RowMetaInterface surCols = null;

		// ValueMetaInterface
		surCols = getColumnsFromCache(wSchema.getText(), wTargetTable.getText());

		if (surCols != null) {
			for (int i = 0; i < surCols.getFieldNames().length; i++) {
				if (filterType1 != -1) {
					if (filterType1 == surCols.getValueMeta(i).getType()) {
						combo.add(surCols.getFieldNames()[i]);
					} else if (filterType2 != -1 && filterType2 == surCols.getValueMeta(i).getType()) {
						combo.add(surCols.getFieldNames()[i]);
					}
				} else {
					combo.add(surCols.getFieldNames()[i]);
				}
			}
		}
	}

	protected void getTableName() {
		DatabaseMeta inf = null;
		int connr = wConnection.getSelectionIndex();
		if (connr >= 0) {
			inf = transMeta.getDatabase(connr);
		}

		if (inf != null) {
			logDebug("Looking at connection: " + inf.toString());

			DatabaseExplorerDialog std = new DatabaseExplorerDialog(shell, SWT.NONE, inf, transMeta.getDatabases());
			std.setSelectedSchemaAndTable(wSchema.getText(), wTargetTable.getText());
			if (std.open()) {
				wSchema.setText(Const.NVL(std.getSchemaName(), ""));
				wTargetTable.setText(Const.NVL(std.getTableName(), ""));
				setTableFieldCombo();
			}
		} else {
			MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
			mb.setMessage(BaseMessages.getString(PKG, "LoadDialog.ConnectionError2.DialogMessage"));
			mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
			mb.open();
		}
	}

	protected void getSchemaNames() {
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

	
	
	/*
	 * Returns from cache the RowMetaInterface associated to "schema.table" as
	 * entered in UI (populate the cache when "schema.table" not found) Returns
	 * null for any type of Exception (unknown "schema.table", DB error..)
	 */
	protected RowMetaInterface getColumnsFromCache(String schemaUI, String tableUI) {
		if (Const.isEmpty(tableUI)) {
			return null;
		}

		String key = (Const.isEmpty(schemaUI) ? "" : schemaUI) + "." + tableUI;

		if (cacheColumnMap.get(key) != null) {
			return cacheColumnMap.get(key);
		} else {
			// fetch DB data
			String connectionName = wConnection.getText();
			DatabaseMeta ci = transMeta.findDatabase(connectionName);
			if (ci != null) {
				Database db = new Database(loggingObject, ci);
				try {
					db.connect();
					String schemaTable = ci.getQuotedSchemaTableCombination(transMeta.environmentSubstitute(schemaUI),
							transMeta.environmentSubstitute(tableUI));
					RowMetaInterface found = db.getTableFields(schemaTable);

					if (found != null) {
						cacheColumnMap.put(key, found);
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

	/*
	 * To reset cacheColumnMap when connection is changed at the UI
	 */
	protected void resetColumnsCache() {
		cacheColumnMap.clear();
	}
	
	
	/**
	 * Copy information from the meta-data input to the dialog fields.
	 * Sub-class completes the copying with specialized properties
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
			wTargetTable.setText(inputMeta.getTargetTable());
		}

		wBatchSize.setText("" + inputMeta.getBufferSize());


		if (inputMeta.getAuditDtsCol() != null) {
			wAuditDTSCol.setText(inputMeta.getAuditDtsCol());
		}

		if (inputMeta.getAuditRecSourceCol() != null) {
			wAuditRecSrcCol.setText(inputMeta.getAuditRecSourceCol());
		}

		if (inputMeta.getAuditRecSourceValue() != null) {
			wAuditRecSrcVal.setText(inputMeta.getAuditRecSourceValue());
		}
	}

	/*
	 * Update the Meta object according to UI widgets
	 */
	protected void getInfo(BaseLoadMeta in) {
		in.setDatabaseMeta(transMeta.findDatabase(wConnection.getText()));
		in.setSchemaName(wSchema.getText());
		in.setTargetTable(wTargetTable.getText());
		in.setAuditDtsCol(wAuditDTSCol.getText());
		in.setAuditRecSourceCol(wAuditRecSrcCol.getText());
		in.setAuditRecSourceValue(wAuditRecSrcVal.getText());
		in.setBufferSize(Const.toInt(wBatchSize.getText(), 0));

	}

	protected abstract void sql();
	protected abstract void getFieldsFromInput();
	
	
	protected void cancel() {
		stepname = null;
		inputMeta.setChanged(backupChanged);
		dispose();
	}

	
	protected void ok() {
		if (Const.isEmpty(wStepname.getText())) {
			return;
		}
		BaseLoadMeta oldMetaState = (BaseLoadMeta) inputMeta.clone();
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

	
}
