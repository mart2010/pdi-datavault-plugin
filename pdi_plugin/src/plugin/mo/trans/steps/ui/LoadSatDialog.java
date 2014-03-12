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
import plugin.mo.trans.steps.loadsat.LoadSatMeta;

/**
 * 
 * Load Sat dialog
 *  @author mouellet
 *  
 */
public class LoadSatDialog extends BaseLoadDialog implements StepDialogInterface {

	private Label wlKey;
	private TableView wKey;
	private boolean hasOneTemporalField;
	
	private Label wlIsIdempotentSat;
	private Button wbIsIdempotentSat;

	private Label wlToDateCol;
	private CCombo wcbToDateCol;
	private Label wlToDateMax;
	private Text wToDateMax;


	public LoadSatDialog(Shell parent, Object in, TransMeta transMeta, String sname) {
		super(parent, (BaseStepMeta) in, transMeta, sname);
	}

	/*
	 * Constructing all Dialog widgets Return the (possibly new) name of the
	 * step. If it returns null, Kettle assumes that the dialog was cancelled
	 * (done by Cancel handler).
	 */
	public String open() {
		String t = super.open();
		shell.setText(BaseMessages.getString(PKG, "LoadSatDialog.Shell.Title"));


		// Stepname line
		wlStepname.setText(BaseMessages.getString(PKG, "LoadDialog.Stepname.Label"));

		// Connection line
		wConnection.addModifyListener(new ModifyListener() {
			public void modifyText(ModifyEvent e) {
				// We have new content: change ci connection:
				dbMeta = transMeta.findDatabase(wConnection.getText());
				inputMeta.setChanged();
				resetColumnsCache();
			}
		});

		// Schema line...
		wlSchema.setText(BaseMessages.getString(PKG, "LoadDialog.TargetSchema.Label"));

		// Sat Table line...
		wlTargetTable.setText(BaseMessages.getString(PKG, "LoadSatDialog.Target.Label"));

		// Batch size ...

		
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
		wbIsIdempotentSat.setToolTipText(BaseMessages.getString(PKG, "LoadSatDialog.IdempotentTransf.Tooltip"));
				
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
    
		wbIsIdempotentSat.addSelectionListener(lsDef);
		wcbToDateCol.addSelectionListener(lsDef);
		wToDateMax.addSelectionListener(lsDef);

		getData();
		setTableFieldCombo();
		inputMeta.setChanged(backupChanged);

		// Set the shell size, based upon previous time...
		setSize();
		setToDateColumns();
		
		shell.open();
		while (!shell.isDisposed()) {
			if (!display.readAndDispatch()) {
				display.sleep();
			}
		}
		return stepname;
	}


	public void enableFields() {
		wlIsIdempotentSat.setEnabled(hasOneTemporalField);
		wbIsIdempotentSat.setEnabled(hasOneTemporalField);
		wlToDateCol.setEnabled(hasOneTemporalField);
		wcbToDateCol.setEnabled(hasOneTemporalField);
		wlToDateMax.setEnabled(hasOneTemporalField);
		wToDateMax.setEnabled(hasOneTemporalField);

	}

	
	private void setToDateColumns() {
		// clear and reset..
		wcbToDateCol.removeAll();
		RowMetaInterface toCols = null;
		toCols = getColumnsFromCache(wSchema.getText(),wTargetTable.getText() );	

		if (toCols != null){
			wcbToDateCol.add(LoadSatMeta.NA);
			for (int i = 0; i < toCols.getFieldNames().length; i++){
				wcbToDateCol.add(toCols.getFieldNames()[i]);	
			}
		} else {
			wcbToDateCol.add("");
		}
	}

	
	/**
	 * Copy information from the meta-data input to the dialog fields.
	 * 
	 */
	public void getData() {
		super.getData();

		hasOneTemporalField = ((LoadSatMeta) inputMeta).getFromDateColumn() != null ; 
		enableFields();

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

		if (((LoadSatMeta) inputMeta).getToDateColumn() != null) {
			wcbToDateCol.setText(((LoadSatMeta) inputMeta).getToDateColumn());
		}	
		if (((LoadSatMeta) inputMeta).getToDateMaxFlag() != null) {
			wToDateMax.setText(((LoadSatMeta) inputMeta).getToDateMaxFlag());
		}	
		wbIsIdempotentSat.setSelection(((LoadSatMeta) inputMeta).isIdempotent());

		wKey.setRowNums();
		wKey.optWidth(true);

		wStepname.selectAll();
		wStepname.setFocus();
	}



	protected void sql() {
		LoadSatMeta metaH = new LoadSatMeta();
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
	protected void getInfo(BaseLoadMeta inMeta) {
		super.getInfo(inMeta);
		
		int nrkeys = wKey.nrNonEmpty();
		inMeta.allocateKeyArray(nrkeys);

		logDebug("Found nb of Keys=", String.valueOf(nrkeys));
		
		//in case temporal not set, then null is used as flag
		((LoadSatMeta) inMeta).setFromDateColumn(null);
		
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
				((LoadSatMeta) inMeta).setFromDateColumn(LoadSatMeta.ATTRIBUTE_TEMPORAL);
			}
		}
		
		((LoadSatMeta) inMeta).setToDateColumn(wcbToDateCol.getText());
		((LoadSatMeta) inMeta).setToDateMaxFlag(wToDateMax.getText());
		((LoadSatMeta) inMeta).setIdempotent(wbIsIdempotentSat.getSelection());
		
	}


	protected void getFieldsFromInput() {
		try {
			RowMetaInterface r = transMeta.getPrevStepFields(stepname);
			if (r != null && !r.isEmpty()) {
				BaseStepDialog.getFieldsFromPrevious(r, wKey, 1, new int[] { 1, 2 }, new int[] {}, -1, -1,
						new TableItemInsertListener() {
							public boolean tableItemInserted(TableItem tableItem, ValueMetaInterface v) {
								tableItem.setText(3, LoadSatMeta.ATTRIBUTE_NORMAL);
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