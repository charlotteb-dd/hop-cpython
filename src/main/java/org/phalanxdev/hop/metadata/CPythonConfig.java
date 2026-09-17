package org.phalanxdev.hop.metadata;

import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@GuiPlugin(id = CPythonConfig.GUI_PLUGIN_ID)
@HopMetadata(name = "i18n::CPythonConfig.Name",
             description = "i18n::CPythonConfig.Description",
             image = "pylogo.svg",
             category = "run-config",
             key = "cpython-config")
public class CPythonConfig extends HopMetadataBase {
	public static final String GUI_PLUGIN_ID = "CPythonConfig_GUI";
	
	@GuiWidgetElement(
	        id = "name",
	        order = "01",
	        label = "i18n::CPythonConfigEditor.Name.Label",
	        toolTip = "i18n::CPythonConfigEditor.Name.Tooltip",
	        type = GuiElementType.TEXT,
	        variables = true,
	        multiLineTextHeight = 5,
	        parentId = GUI_PLUGIN_ID
	)
	@HopMetadataProperty
	private String name;
	
	@GuiWidgetElement(
	        id = "libs",
	        order = "02",
	        label = "i18n::CPythonConfigEditor.Libs.Label",
	        toolTip = "i18n::CPythonConfigEditor.Libs.Tooltip",
	        type = GuiElementType.TEXT,
	        variables = true,
	        parentId = GUI_PLUGIN_ID
	)
	@HopMetadataProperty
	private String libs;
}