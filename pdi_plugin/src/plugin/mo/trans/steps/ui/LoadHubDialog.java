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
package plugin.mo.trans.steps.ui;

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
import org.pentaho.di.core.SQLStatement;
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
import org.pentaho.di.ui.core.database.dialog.SQLEditor;
import org.pentaho.di.ui.core.dialog.EnterSelectionDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.trans.step.TableItemInsertListener;

import plugin.mo.trans.steps.common.BaseLoadDialog;
import plugin.mo.trans.steps.common.BaseLoadMeta;
import plugin.mo.trans.steps.loadhub.LoadHubMeta;

/**
 * Load Hub dialog
 * 
 * @author mouellet
 */
public class LoadHubDialog extends BaseLoadDialog implements StepDialogInterface {

	private Label wlTechKey;
	private CCombo wTechKey;

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

	public LoadHubDialog(Shell parent, Object in, TransMeta transMeta, String sname) {
		super(parent, (BaseStepMeta) in, transMeta, sname);
	}

	/*
	 * Constructing all Dialog widgets Return the (possibly new) name of the
	 * step. If it returns null, Kettle assumes the dialog was cancelled
	 * (done by Cancel handler).
	 */
	public String open() {
		String t = super.open();
		shell.setText(BaseMessages.getString(PKG, "LoadHubDialog.Shell.Title"));


		// Stepname line
		wlStepname.setText(BaseMessages.getString(PKG, "LoadDialog.Stepname.Label"));

		// Connection line
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
		wlSchema.setText(BaseMessages.getString(PKG, "LoadDialog.TargetSchema.Label"));

		// Table line...
		wlTargetTable.setText(BaseMessages.getString(PKG, "LoadHubDialog.Target.Label"));

		// Batch size ...
		
		//
		// The fields: keys + none-keys
		//
		wlKey = new Label(shell, SWT.NONE);
		wlKey.setText(BaseMessages.getString(PKG, "LoadHubDialog.Keyfields.Label"));
		props.setLook(wlKey);
		FormData fdlKey = new FormData();
		fdlKey.left = new FormAttachment(0, 0);
		fdlKey.top = new FormAttachment(wBatchSize, margin * 2);
		fdlKey.right = new FormAttachment(100, 0);
		wlKey.setLayoutData(fdlKey);

		int nrKeyCols = 3;
		int nrKeyRows = (inputMeta.getFields() != null ? inputMeta.getFields().length : 1);

		ciKey = new ColumnInfo[nrKeyCols];
		ciKey[0] = new ColumnInfo(BaseMessages.getString(PKG, "LoadHubDialog.ColumnInfo.TableColumn"),
				ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false);
		ciKey[1] = new ColumnInfo(BaseMessages.getString(PKG, "LoadHubDialog.ColumnInfo.FieldInStream"),
				ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false);
		ciKey[2] = new ColumnInfo(BaseMessages.getString(PKG, "LoadLinkDialog.ColumnInfo.FieldType"),
				ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { LoadHubMeta.IDENTIFYING_KEY, LoadHubMeta.OTHER_TYPE },
				false);
		// attach the tableFieldColumns List to the widget
		tableFieldColumns.add(ciKey[0]);
		wKey = new TableView(transMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL
				| SWT.H_SCROLL, ciKey, nrKeyRows, lsMod, props);


		// The Key Creation Group
		Group gKeyCreationFields = new Group(shell, SWT.SHADOW_ETCHED_IN);
		gKeyCreationFields.setText(BaseMessages.getString(PKG, "LoadDialog.KeyGenGroupFields.Label"));

		FormLayout keyGroupLayout = new FormLayout();
		keyGroupLayout.marginWidth = 3;
		keyGroupLayout.marginHeight = 3;
		gKeyCreationFields.setLayout(keyGroupLayout);
		props.setLook(gKeyCreationFields);

		// Tech/surr key column:
		wlTechKey = new Label(gKeyCreationFields, SWT.RIGHT);
		wlTechKey.setText(BaseMessages.getString(PKG, "LoadLinkDialog.SurrKey.Label"));
		props.setLook(wlTechKey);
		FormData fdlTk = new FormData();
		fdlTk.left = new FormAttachment(0, 0);
		fdlTk.right = new FormAttachment(middle, -margin);
		fdlTk.top = new FormAttachment(gKeyCreationFields, margin);
		wlTechKey.setLayoutData(fdlTk);

		wTechKey = new CCombo(gKeyCreationFields, SWT.BORDER);
		props.setLook(wTechKey);
		// set its listener
		wTechKey.addModifyListener(lsMod);
		FormData fdTk = new FormData();
		fdTk.left = new FormAttachment(middle, 0);
		fdTk.top = new FormAttachment(gKeyCreationFields, margin);
		fdTk.right = new FormAttachment(100, 0);
		wTechKey.setLayoutData(fdTk);
		wTechKey.addFocusListener(new FocusListener() {
			public void focusLost(FocusEvent arg0) {
			}

			public void focusGained(FocusEvent arg0) {
				Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
				shell.setCursor(busy);
				setColumnsCombo(wTechKey, ValueMetaInterface.TYPE_INTEGER, -1);
				shell.setCursor(null);
				busy.dispose();
			}
		});

		// Creation of surrogate key
		Group gSurrGroup = new Group(gKeyCreationFields, SWT.SHADOW_ETCHED_IN);
		gSurrGroup.setText(BaseMessages.getString(PKG, "LoadDialog.SurrGroup.Label"));
		GridLayout gridLayout = new GridLayout(3, false);
		gSurrGroup.setLayout(gridLayout);
		fdSurrGroup = new FormData();
		fdSurrGroup.left = new FormAttachment(middle, 0);
		fdSurrGroup.top = new FormAttachment(wTechKey, margin);
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
				inputMeta.setKeyGeneration(BaseLoadMeta.CREATION_METHOD_SEQUENCE);
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

		// Fixing Key Creation Group
		FormData fdKeyGroup = new FormData();
		fdKeyGroup.left = new FormAttachment(0, 0);
		fdKeyGroup.right = new FormAttachment(100, 0);
		fdKeyGroup.bottom = new FormAttachment(wAuditFields, -margin);
		gKeyCreationFields.setLayoutData(fdKeyGroup);

		// to fix the Mapping Grid
		FormData fdKey = new FormData();
		fdKey.left = new FormAttachment(0, 0);
		fdKey.top = new FormAttachment(wlKey, margin);
		fdKey.right = new FormAttachment(100, 0);
		fdKey.bottom = new FormAttachment(gKeyCreationFields, -margin);
		wKey.setLayoutData(fdKey);

		// search the fields in the background
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
	
		
		wSeq.addSelectionListener(lsDef);
		wTechKey.addSelectionListener(lsDef);

		getData();
		setTableFieldCombo();
		inputMeta.setChanged(backupChanged);
	
		// Set the shell size, based upon previous time...
		setSize();
		
		shell.open();
		while (!shell.isDisposed()) {
			if (!display.readAndDispatch()) {
				display.sleep();
			}
		}
		return stepname;
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
		super.getData();
		
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
			wTechKey.setText(inputMeta.getTechKeyCol());
		}

		String surrKeyCreation = inputMeta.getKeyGeneration();

		if (BaseLoadMeta.CREATION_METHOD_AUTOINC.equals(surrKeyCreation)) {
			wAutoinc.setSelection(true);
		} else if ((BaseLoadMeta.CREATION_METHOD_SEQUENCE.equals(surrKeyCreation))) {
			wSeqButton.setSelection(true);
		} else { // TableMax is also the default when no creation is yet defined
			wTableMax.setSelection(true);
			inputMeta.setKeyGeneration(BaseLoadMeta.CREATION_METHOD_TABLEMAX);
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


	protected void sql() {
		LoadHubMeta metaH = new LoadHubMeta();
		getInfo(metaH);

		try {
			SQLStatement sql = metaH.getSQLStatements(transMeta, stepMeta, null, repository, metaStore);
			
			if (!sql.hasError()) {
				if (sql.hasSQL()) {
					SQLEditor sqledit = new SQLEditor(transMeta, shell, SWT.NONE, metaH.getDatabaseMeta(),
							transMeta.getDbCache(), sql.getSQL());
					sqledit.open();
				} else {
					MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
					mb.setMessage(BaseMessages.getString(PKG, "LoadMeta.NoSQL.DialogMessage"));
					mb.setText(BaseMessages.getString(PKG, "LoadMeta.NoSQL.DialogTitle"));
					mb.open();
				}
			} else {
				MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
				mb.setMessage(sql.getError());
				mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
				mb.open();
			}
		} catch (KettleException ke) {
			new ErrorDialog(shell, BaseMessages.getString(PKG, "LoadDialog.BuildSQLError.DialogTitle"),
					BaseMessages.getString(PKG, "LoadDialog.BuildSQLError.DialogMessage"), ke);
		}
	}

	/*
	 * Update the Meta object according to UI widgets
	 */
	protected void getInfo(BaseLoadMeta in) {
		super.getInfo(in);
		in.setTechKeyCol(wTechKey.getText());

		int nrkeys = wKey.nrNonEmpty();
		in.allocateKeyArray(nrkeys);

		for (int i = 0; i < nrkeys; i++) {
			TableItem item = wKey.getNonEmpty(i);
			in.getCols()[i] = item.getText(1);
			in.getFields()[i] = item.getText(2);
			in.getTypes()[i] = item.getText(3);
		}

		if (wAutoinc.getSelection()) {
			in.setKeyGeneration(BaseLoadMeta.CREATION_METHOD_AUTOINC);
		} else if (wSeqButton.getSelection()) {
			in.setKeyGeneration(BaseLoadMeta.CREATION_METHOD_SEQUENCE);
			in.setSequenceName(wSeq.getText());
		} else { // TableMax
			in.setKeyGeneration(BaseLoadMeta.CREATION_METHOD_TABLEMAX);
		}

	}

	protected void getFieldsFromInput() {
		try {
			RowMetaInterface r = transMeta.getPrevStepFields(stepname);
			if (r != null && !r.isEmpty()) {
				BaseStepDialog.getFieldsFromPrevious(r, wKey, 2, new int[] { 2 }, new int[] {}, -1, -1,
						new TableItemInsertListener() {
							public boolean tableItemInserted(TableItem tableItem, ValueMetaInterface v) {
								tableItem.setText(3, LoadHubMeta.IDENTIFYING_KEY);
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