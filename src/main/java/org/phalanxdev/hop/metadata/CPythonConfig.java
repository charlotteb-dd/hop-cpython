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
	
//	@GuiWidgetElement(
//	        id = "libs",
//	        order = "02",
//	        label = "i18n::CPythonConfigEditor.Libs.Label",
//	        toolTip = "i18n::CPythonConfigEditor.Libs.Tooltip",
//	        type = GuiElementType.TEXT,
//	        variables = true,
//	        parentId = GUI_PLUGIN_ID
//	)
//	@HopMetadataProperty
//	private String libs;
//	
	/**
	 * User-supplied path to python executable. If not specified, then we use the
	 * default python in the path
	 */
	@GuiWidgetElement(
			id = "pythonCommand",
			order = "02",
			label = "i18n::CPythonConfigEditor.PythonCommand.Label",
			toolTip = "i18n::CPythonConfigEditor.PythonCommand.ToolTip",
			type = GuiElementType.TEXT,
			variables = true,
			parentId = GUI_PLUGIN_ID)
	@HopMetadataProperty
	protected String pythonCommand = "";

	/**
	 * Optional entries for the PATH, required so that python will execute
	 * correctly. Used when user has specified path to python executable. E.g. under
	 * windows, Anaconda requires Library/bin to be in the PATH as well as the
	 * python executable.
	 */
	@GuiWidgetElement(
			id = "pyPathEntries",
			order = "03",
			label = "i18n::CPythonConfigEditor.PyPathEntries.Label",
			toolTip = "i18n::CPythonConfigEditor.PyPathEntries.ToolTip",
			type = GuiElementType.TEXT,
			variables = true,
			parentId = GUI_PLUGIN_ID)
	@HopMetadataProperty
	protected String pyPathEntries = "";

	/**
	 * An optional ID for the python server. This plus the path to the executable
	 * uniquely identifies a non-default server. Can be used to share a given server
	 * instance among several clients, or to ensure that a given client has a
	 * dedicated server.
	 */
	@GuiWidgetElement(
			id = "serverID",
			order = "04",
			label = "i18n::CPythonConfigEditor.ServerID.Label",
			toolTip = "i18n::CPythonConfigEditor.ServerID.ToolTip",
			type = GuiElementType.TEXT,
			variables = true,
			parentId = GUI_PLUGIN_ID)
	@HopMetadataProperty
	protected String serverID = "";
}