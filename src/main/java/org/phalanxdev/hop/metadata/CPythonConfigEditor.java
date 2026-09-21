package org.phalanxdev.hop.metadata;

import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;

public class CPythonConfigEditor extends MetadataEditor<CPythonConfig> {

	public CPythonConfigEditor(HopGui hopGui, MetadataManager<CPythonConfig> manager, CPythonConfig metadata) {
		super(hopGui, manager, metadata);
	}

	private GuiCompositeWidgets guiCompositeWidgets;
	private Composite wAutoFields;

	@Override
	public void createControl(Composite parent) {
		int margin = PropsUi.getMargin();

		FormLayout formLayout = new FormLayout();
		formLayout.marginWidth = margin;
		formLayout.marginHeight = margin;
		parent.setLayout(formLayout);

		wAutoFields = new Composite(parent, SWT.NONE);
		PropsUi.setLook(wAutoFields);
		wAutoFields.setLayout(new FormLayout());

		FormData fdAuto = new FormData();
		fdAuto.left = new FormAttachment(0, 0);
		fdAuto.right = new FormAttachment(100, 0);
		fdAuto.top = new FormAttachment(0, 0);
		wAutoFields.setLayoutData(fdAuto);

		guiCompositeWidgets = new GuiCompositeWidgets(getVariables());
		guiCompositeWidgets.createCompositeWidgets(getMetadata(), null, wAutoFields, CPythonConfig.GUI_PLUGIN_ID, null);

		guiCompositeWidgets.setWidgetsContents(getMetadata(), wAutoFields, CPythonConfig.GUI_PLUGIN_ID);

		wAutoFields.layout(true, true);
		parent.layout(true, true);
	}

	@Override
	public void setWidgetsContent() {
		guiCompositeWidgets.setWidgetsContents(getMetadata(), wAutoFields, CPythonConfig.GUI_PLUGIN_ID);
	}

	@Override
	public void getWidgetsContent(CPythonConfig meta) {
		guiCompositeWidgets.getWidgetsContents(meta, CPythonConfig.GUI_PLUGIN_ID);
	}

}