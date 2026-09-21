/*! ******************************************************************************
 *
 * CPython for the Hop orchestration platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.phalanxdev.hop.pipeline.transforms.cpython;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variable;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transform.stream.IStream.StreamType;
import org.apache.hop.pipeline.transform.stream.Stream;
import org.apache.hop.pipeline.transform.stream.StreamIcon;
import org.phalanxdev.hop.metadata.CPythonConfig;
import org.phalanxdev.python.PythonSession;

import java.util.ArrayList;
import java.util.List;

/**
 * Meta class for the CPythonScriptExecutor step
 *
 * @author Mark Hall (mhall{[at]}phalanxdev{[dot]}com)
 */
@Transform(id = "CPythonScriptExecutor", image = "pylogo.svg", name = "CPython Script Executor", description = "Executes a python script", categoryDescription = "Statistics")
public class CPythonScriptExecutorMeta extends BaseTransformMeta<CPythonScriptExecutor, CPythonScriptExecutorData> {

	private static Class<?> PKG = CPythonScriptExecutor.class;

	protected static final String ROWS_TO_PROCESS_TAG = "rows_to_process";
	protected static final String ROWS_TO_PROCESS_SIZE_TAG = "rows_to_process_size";
	protected static final String RESERVOIR_SAMPLING_TAG = "reservoir_sampling";
	protected static final String RESERVOIR_SAMPLING_SIZE_TAG = "reservoir_sampling_size";
	protected static final String RESERVOIR_SAMPLING_SEED_TAG = "reservoir_sampling_seed";
	protected static final String INCLUDE_INPUT_AS_OUTPUT_TAG = "include_input_as_output";
	protected static final String INCLUDE_FRAME_ROW_INDEX_AS_OUTPUT_FIELD_TAG = "include_frame_row_index";
	protected static final String LOAD_SCRIPT_AT_RUNTIME_TAG = "load_script_at_runtime";
	protected static final String SCRIPT_TO_LOAD_TAG = "script_to_load";
	protected static final String SCRIPT_TAG = "py_script";
	protected static final String FRAME_NAMES_TAG = "frame_names";
	protected static final String PY_VARS_TO_GET_TAG = "py_vars_to_get";
	protected static final String CONTINUE_ON_UNSET_VARS_TAG = "continue_on_unset_vars";
	protected static final String SINGLE_FRAME_NAME_PREFIX_TAG = "frame_name";
	protected static final String INCOMING_STEP_NAMES_TAG = "incoming_step_names";
	protected static final String SINGLE_INCOMING_STEP_NAME_TAG = "step_name";
	protected static final String OUTPUT_FIELDS_TAG = "output_fields";
	protected static final String SINGLE_OUTPUT_FIELD_TAG = "output_field";
	protected static final String PYTHON_COMMAND = "python_command";
	protected static final String PYTHON_PATH_ENTRIES = "python_path_entries";
	protected static final String PYTHON_SERVER_ID = "python_server_id";

	/**
	 * Default prefix for kettle data -> pandas frame name
	 */
	public static final String DEFAULT_FRAME_NAME_PREFIX = "hop_data";

	/**
	 * Default row handling strategy
	 */
	public static final String DEFAULT_ROWS_TO_PROCESS = ProcessingMode.ALL.getCode();
//      BaseMessages.getString(PKG,
//          "CPythonScriptExecutorDialog.NumberOfRowsToProcess.Dropdown.AllEntry.Label");

	/**
	 * The script to execute
	 */
	@HopMetadataProperty
	protected String script = BaseMessages.getString(PKG, "CPythonScriptExecutorMeta.InitialScriptText");

//	/**
//	 * User-supplied path to python executable. If not specified, then we use the
//	 * default python in the path
//	 */
//	@HopMetadataProperty
//	protected String pythonCommand = "";
//
//	/**
//	 * Optional entries for the PATH, required so that python will execute
//	 * correctly. Used when user has specified path to python executable. E.g. under
//	 * windows, Anaconda requires Library/bin to be in the PATH as well as the
//	 * python executable.
//	 */
//	@HopMetadataProperty
//	protected String pyPathEntries = "";
//
//	/**
//	 * An optional ID for the python server. This plus the path to the executable
//	 * uniquely identifies a non-default server. Can be used to share a given server
//	 * instance among several clients, or to ensure that a given client has a
//	 * dedicated server.
//	 */
//	@HopMetadataProperty
//	protected String serverID = "";

	@HopMetadataProperty
	protected String libraries = "";
	
	public String getLibraries() {
		return libraries;
	}

	public void setLibraries(String libraries) {
		this.libraries = libraries;
	}

	/**
	 * Whether to load a script at runtime
	 */
	@HopMetadataProperty
	protected boolean loadScriptAtRuntime;

	/**
	 * The script to load (if loading at runtime)
	 */
	@HopMetadataProperty
	protected String loadScriptFile = ""; //$NON-NLS-1$

	/**
	 * The name(s) of the data frames to create in python - one corresponding to
	 * each incoming row set
	 */
	@HopMetadataProperty
	private List<CPythonInputFrame> inputFrames;
//  @HopMetadataProperty(groupKey = "frame_names", key = "frame_name")
//  protected List<String> frameNames = new ArrayList<>();

	/**
	 * List of variables to get from python. This should hold exactly one variable
	 * in the case of extracting a data frame. There can be more than one if all
	 * variables are either strings or images
	 */
	@HopMetadataProperty
	protected List<String> pyVarsToGet = new ArrayList<>();

	/**
	 * Whether to include the pandas frame row index as an output field (when
	 * retrieving a single data frame from python as output.
	 */
	@HopMetadataProperty
	protected boolean includeRowIndex;

	/**
	 * Whether to continue processing if one or more requested variables are not set
	 * in the python environment after executing the script.
	 */
	@HopMetadataProperty
	protected boolean continueOnUnsetVars;

	/**
	 * Default Rows to Process
	 */
	@HopMetadataProperty
	protected String rowsToProcess = DEFAULT_ROWS_TO_PROCESS;

	/**
	 * Number of rows to process if <code>rows to process is batch </code>
	 */
	@HopMetadataProperty
	protected String rowsToProcessSize = "";

	/**
	 * True if reservoir sampling is to be used, in which case the batch size is the
	 * reservoir size
	 */
	@HopMetadataProperty
	protected boolean doingReservoirSampling = false;

	/**
	 * If reservoir sampling is enabled, this value is used to define the sampling
	 * size
	 */
	@HopMetadataProperty
	protected String reservoirSamplingSize = "";

	/**
	 * Random seed for reservoir sampling
	 */
	@HopMetadataProperty
	protected String seed = "1"; //$NON-NLS-1$

	/**
	 * True if input stream values should be copied to the output stream. Only
	 * applies when output is a single pandas data frame; furthermore, number of
	 * output rows must match number of input rows.
	 */
	@HopMetadataProperty
	protected boolean includeInputAsOutput = false;

	/**
	 * True if Apache Arrow should be used for data transfer (when available)
	 */
	@HopMetadataProperty
	protected boolean useArrow = true;

	@HopMetadataProperty
	protected String configName;

	public String getConfigName() {
		return configName;
	}

	public void setConfigName(String configName) {
		this.configName = configName;
	}

	private int configSelectionIndex;

	public int getConfigSelectionIndex() {
		return configSelectionIndex;
	}

	public void setConfigSelectionIndex(int configSelectionIndex) {
		this.configSelectionIndex = configSelectionIndex;
	}

	/**
	 * Outgoing fields
	 */
	@HopMetadataProperty
	protected List<CPythonOutputField> outputFields;
//  protected IRowMeta outputsFields;

//	public void setPythonCommand(String pythonCommand) {
//		this.pythonCommand = pythonCommand;
//	}
//
//	public String getPythonCommand() {
//		return pythonCommand;
//	}
//
//	public void setPyPathEntries(String pyPathEntries) {
//		this.pyPathEntries = pyPathEntries;
//	}
//
//	public String getPyPathEntries() {
//		return pyPathEntries;
//	}
//
//	public void setServerID(String pyServerID) {
//		this.serverID = pyServerID;
//	}
//
//	public String getServerID() {
//		return serverID;
//	}

	/**
	 * Get the output structure
	 *
	 * @return the output structure
	 */
	public List<CPythonOutputField> getOutputFields() {
		return outputFields;
	}

	/**
	 * Set the output structure
	 *
	 * @param rm the output structure
	 */
	public void setOutputFields(List<CPythonOutputField> outputFields) {
		this.outputFields = outputFields;
	}

	public void setRowsToProcess(String s) {
		rowsToProcess = s;
	}

	public String getRowsToProcess() {
		return rowsToProcess;
	}

	public void setRowsToProcessSize(String s) {
		rowsToProcessSize = s;
	}

	public String getRowsToProcessSize() {
		return rowsToProcessSize;
	}

	/**
	 * Set whether reservoir sampling is to be used in the single input case.
	 * Sampling is always used when there are multiple input row sets.
	 *
	 * @param r true if reservoir sampling is to be used
	 */
	public void setDoingReservoirSampling(boolean r) {
		doingReservoirSampling = r;
	}

	/**
	 * Get whether reservoir sampling is to be used in the single input case.
	 * Sampling is always used when there are multiple input row sets.
	 *
	 * @return true if reservoir sampling is to be used in the single input case.
	 */
	public boolean isDoingReservoirSampling() {
		return doingReservoirSampling;
	}

	/**
	 * Set the size of the reservoir
	 *
	 * @param s the size of the reservoir
	 */
	public void setReservoirSamplingSize(String s) {
		reservoirSamplingSize = s;
	}

	/**
	 * Get the size of the reservoir
	 *
	 * @return the size of the reservoir
	 */
	public String getReservoirSamplingSize() {
		return reservoirSamplingSize;
	}

	/**
	 * Set the random seed to use for reservoir sampling
	 *
	 * @param seed the random seed to use when reservoir sampling
	 */
	public void setSeed(String seed) {
		this.seed = seed;
	}

	/**
	 * Get the random seed to use for reservoir sampling
	 *
	 * @return the random seed to use when reservoir sampling
	 */
	public String getSeed() {
		return seed;
	}

	/**
	 * Sets whether the step should or not include input values in the output stream
	 */
	public void setIncludeInputAsOutput(boolean s) {
		includeInputAsOutput = s;
	}

	/**
	 * Gets whether the step should or not include input values in the output stream
	 *
	 * @return true if step should include input values in output
	 */
	public boolean isIncludeInputAsOutput() {
		return includeInputAsOutput;
	}

	/**
	 * Set whether to use Apache Arrow for data transfer
	 *
	 * @param useArrow true to use Arrow when available
	 */
	public void setUseArrow(boolean useArrow) {
		this.useArrow = useArrow;
	}

	/**
	 * Get whether to use Apache Arrow for data transfer
	 *
	 * @return true if Arrow should be used when available
	 */
	public boolean isUseArrow() {
		return useArrow;
	}

	/**
	 * Set whether to load a script from the file system at runtime rather than
	 * executing the user supplied script
	 *
	 * @param l true if a script is to be loaded at runtime
	 */
	public void setLoadScriptAtRuntime(boolean l) {
		loadScriptAtRuntime = l;
	}

	/**
	 * Get whether to load a script from the file system at runtime rather than
	 * executing the user supplied script
	 *
	 * @return true if a script is to be loaded at runtime
	 */
	public boolean isLoadScriptAtRuntime() {
		return loadScriptAtRuntime;
	}

	/**
	 * Set the path to the script to load at runtime (if loading at runtime)
	 *
	 * @param scriptFile the script file to load at runtime
	 */
	public void setLoadScriptFile(String scriptFile) {
		loadScriptFile = scriptFile;
	}

	/**
	 * Get the path to the script to load at runtime (if loading at runtime)
	 *
	 * @return the script file to load at runtime
	 */
	public String getLoadScriptFile() {
		return loadScriptFile;
	}

	/**
	 * Set the python script to execute
	 *
	 * @param script the script to execute
	 */
	public void setScript(String script) {
		this.script = script;
	}

	/**
	 * Get the python script to execute
	 *
	 * @return the script to execute
	 */
	public String getScript() {
		return script;
	}

	/**
	 * Set the frame names to use when converting incoming row sets into pandas data
	 * frames in python. These are the variable names that the user can reference
	 * the data by
	 *
	 * @param names a list of frame names to use - one for each incoming row set
	 */
//  public void setFrameNames(List<String> names) {
//    frameNames = names;
//  }
	public void setInputFrames(List<CPythonInputFrame> inputFrames) {
		this.inputFrames = inputFrames;
	}

	/**
	 * Get the frame names to use when converting incoming row sets into pandas data
	 * frames in python. These are the variable names that the user can reference
	 * the data by
	 *
	 * @return a list of frame names to use - one for each incoming row set
	 */
//  public List<String> getFrameNames() {
//    return frameNames;
//  }
	public List<CPythonInputFrame> getInputFrames() {
		return inputFrames;
	}

	/**
	 * Set the list of python variables to retrieve. If there is more than one
	 * variable being retrieved, then each variable will be extracted from python as
	 * a string, unless it is an image, in which case the image data is retrieved.
	 * The names of fields output by the step are expected to match the variable
	 * names in this case; furthermore, the user is expected to set the appropriate
	 * outgoing Kettle field type (this must be binary in the case of image data).
	 * Note that the step will not know the types of the specified variables before
	 * runtime.
	 * <p/>
	 * If there is just one variable being extracted from python, then the output
	 * fields must match the names of the columns of a pandas data frame (in the
	 * case that the variable is a data frame), or the name of the variable in the
	 * case that is not a data frame. In both cases, appropriate Kettle types must
	 * be specified by the user.
	 *
	 * @param pyVars the list of python variables to retrieve
	 */
//  public void setPythonVariablesToGet(List<String> pyVars) {
//    pyVarsToGet = pyVars;
//  }

	public void setPyVarsToGet(List<String> pyVars) {
		pyVarsToGet = pyVars;
	}

	/**
	 * Get the list of python variables to retrieve. If there is more than one
	 * variable being retrieved, then each variable will be extracted from python as
	 * a string, unless it is an image, in which case the image data is retrieved.
	 * The names of fields output by the step are expected to match the variable
	 * names in this case; furthermore, the user is expected to set the appropriate
	 * outgoing Kettle field type (this must be binary in the case of image data).
	 * Note that the step will not know the types of the specified variables before
	 * runtime.
	 * <p/>
	 * If there is just one variable being extracted from python, then the output
	 * fields must match the names of the columns of a pandas data frame (in the
	 * case that the variable is a data frame), or the name of the variable in the
	 * case that is not a data frame. In both cases, appropriate Kettle types must
	 * be specified by the user.
	 *
	 * @return the list of python variables to retrieve
	 */
	public List<String> getPyVarsToGet() {
		return pyVarsToGet;
	}

	/**
	 * Set whether to include the pandas data frame row index as an output field, in
	 * the case where the output of the step is a single pandas data frame. Has no
	 * affect if multiple variables are being retrieved from python.
	 *
	 * @param includeFrameRowIndexAsOutputField true to include the frame row index
	 *                                          as an output field
	 */
//  public void setIncludeFrameRowIndexAsOutputField(boolean includeFrameRowIndexAsOutputField) {
//    includeRowIndex = includeFrameRowIndexAsOutputField;
//  }

	public void setIncludeRowIndex(boolean includeFrameRowIndexAsOutputField) {
		includeRowIndex = includeFrameRowIndexAsOutputField;
	}

	/**
	 * Get whether to include the pandas data frame row index as an output field, in
	 * the case where the output of the step is a single pandas data frame. Has no
	 * affect if multiple variables are being retrieved from python.
	 *
	 * @return true to include the frame row index as an output field
	 */
//  public boolean getIncludeFrameRowIndexAsOutputField() {
//    return includeRowIndex;
//  }
	public boolean isIncludeRowIndex() {
		return includeRowIndex;
	}

	/**
	 * Set whether to continue in the case that one or more user specified variables
	 * to retrieve are not set in the python environment after executing the script.
	 *
	 * @param continueOnUnset true to continue processing if there are unset
	 *                        variables
	 */
	public void setContinueOnUnsetVars(boolean continueOnUnset) {
		continueOnUnsetVars = continueOnUnset;
	}

	/**
	 * Get whether to continue in the case that one or more user specified variables
	 * to retrieve are not set in the python environment after executing the script.
	 *
	 * @return true to continue processing if there are unset variables
	 */
	public boolean isContinueOnUnsetVars() {
		return continueOnUnsetVars;
	}

	public IRowMeta determineOutputRowMeta(IRowMeta[] info, IVariables space, IHopMetadataProvider metadataProvider) throws HopException {

		List<IRowMeta> incomingMetas = new ArrayList<>();

		// possibly multiple incoming row sets
		for (IRowMeta r : info) {
			if (r != null) {
				incomingMetas.add(r);
			}
		}
		String resolvedConfigName = space.resolve(this.getConfigName());
		CPythonConfig config = metadataProvider.getSerializer(CPythonConfig.class).load(resolvedConfigName);
		PythonSession.RowMetaAndRows scriptRM = CPythonScriptExecutorData.determineOutputMetaSingleVariable(this,
				incomingMetas, this, config, getLog(), space);

		return scriptRM.rowMeta;
	}

	@Override
	public void getFields(IRowMeta rowMeta, String transformName, IRowMeta[] info, TransformMeta nextTransform,
			IVariables space, IHopMetadataProvider metaStore) throws HopTransformException {

		rowMeta.clear();
		if (outputFields != null && !outputFields.isEmpty()) {
			for (CPythonOutputField field : outputFields) {
				try {
					IValueMeta vm = ValueMetaFactory.createValueMeta(field.getName(),
							ValueMetaFactory.getIdForValueMeta(field.getType()));
					rowMeta.addValueMeta(vm);
				} catch (HopPluginException e) {
					throw new HopTransformException("Error creating value meta", e);
				}
			}

//      rowMeta.addRowMeta(outputFields);

			// Check across all input fields to see if they are in the output, and
			// whether they are binary storage. If binary storage then copy over the
			// original input value meta
			// (this is because get fields in the dialog just creates new ValueMetas without
			// knowledge of storage type)
			if (isIncludeInputAsOutput()) {
				for (IRowMeta r : info) {
					if (r != null) {
						for (IValueMeta vm : r.getValueMetaList()) {
							int outIndex = rowMeta.indexOfValue(vm.getName());
							if (outIndex >= 0) {
								rowMeta.setValueMeta(outIndex, vm);
							}
						}
					}
				}
			}
		} else {
			int numRowMetas = 0;
			for (IRowMeta r : info) {
				if (r != null) {
					numRowMetas++;
				}
			}

			int expectedStreams = (getInputFrames() != null) ? getInputFrames().size() : 0;

			if (numRowMetas != expectedStreams) {
				throw new HopTransformException(
						BaseMessages.getString(PKG, "CPythonScriptExecutorMeta.Error.IncorrectNumberOfIncomingStreams",
								expectedStreams, numRowMetas));
			}

			// incoming fields
			addAllIncomingFieldsToOutput(rowMeta, transformName, info);

			// script fields
			try {
				addScriptFieldsToOutput(rowMeta, info, transformName, space, metaStore);
			} catch (HopException ex) {
				throw new HopTransformException(ex);
			}
		}
	}

	/**
	 * Add all incoming fields to the output row meta in the case where no output
	 * fields have been defined/edited by the user
	 */
	private void addAllIncomingFieldsToOutput(IRowMeta rowMeta, String transformName, IRowMeta[] info) {
		if (isIncludeInputAsOutput()) {
			for (IRowMeta r : info) {
				rowMeta.addRowMeta(r);
			}
		}
	}

	/**
	 * Add script fields to output row meta in the case where no output fields have
	 * been defined/edited by the user. If there is just one variable to extract
	 * from python, then the script will be executed on some randomly generated data
	 * and the type of the variable will be determined; if it is a pandas frame,
	 * then the field meta data can be determined.
	 */
	private void addScriptFieldsToOutput(IRowMeta rowMeta, IRowMeta[] info, String transformName, IVariables space, IHopMetadataProvider metadataProvider)
			throws HopException {
		if (pyVarsToGet.size() == 1) {
			// could be just a single pandas data frame - see if we can determine
			// the fields in this frame...
			IRowMeta scriptRM = determineOutputRowMeta(info, space, metadataProvider);

			for (IValueMeta vm : scriptRM.getValueMetaList()) {
				vm.setOrigin(transformName);
				rowMeta.addValueMeta(vm);
			}
		} else {
			for (String varName : pyVarsToGet) {
				// IValueMeta vm = new ValueMeta( varName, IValueMeta.TYPE_STRING );
				IValueMeta vm = ValueMetaFactory.createValueMeta(varName, IValueMeta.TYPE_STRING);
				vm.setOrigin(transformName);
				rowMeta.addValueMeta(vm);
			}
		}
	}

	/**
	 * Given a fully defined output row metadata structure, determine which of the
	 * output fields are being copied from the input fields and which must be the
	 * output of the script.
	 *
	 * @param fullOutputRowMeta    the fully defined output row metadata structure
	 * @param scriptFields         row meta that will hold script only fields
	 * @param inputPresentInOutput row meta that will hold input fields being copied
	 * @param infos                the array of info row metas
	 * @param transformName        the name of the step
	 */
	protected void determineInputFieldScriptFieldSplit(IRowMeta fullOutputRowMeta, IRowMeta scriptFields,
			IRowMeta inputPresentInOutput, IRowMeta[] infos, String transformName) {

		scriptFields.clear();
		inputPresentInOutput.clear();
		IRowMeta consolidatedInputFields = new RowMeta();
		for (IRowMeta r : infos) {
			consolidatedInputFields.addRowMeta(r);
		}

		for (IValueMeta vm : fullOutputRowMeta.getValueMetaList()) {
			int index = consolidatedInputFields.indexOfValue(vm.getName());
			if (index >= 0) {
				inputPresentInOutput.addValueMeta(vm);
			} else {
				// must be a script output (either a variable name field or data frame column
				// name
				scriptFields.addValueMeta(vm);
			}
		}
	}

	@Override
	public void setDefault() {
		rowsToProcess = BaseMessages.getString(PKG,
				"CPythonScriptExecutorDialog.NumberOfRowsToProcess.Dropdown.AllEntry.Label");
		rowsToProcessSize = "";
		doingReservoirSampling = false;
		reservoirSamplingSize = "";
		inputFrames = new ArrayList<>();
		continueOnUnsetVars = false;
		pyVarsToGet = new ArrayList<>();
		script = BaseMessages.getString(PKG, "CPythonScriptExecutorMeta.InitialScriptText"); //$NON-NLS-1$
		configName = "";
		configSelectionIndex = -1;
	}

	protected String varListToString() {
		StringBuilder b = new StringBuilder();
		for (String v : pyVarsToGet) {
			if (!org.apache.hop.core.util.Utils.isEmpty(v.trim())) {
				b.append(v.trim()).append(",");
			}
		}

		if (b.length() > 0) {
			b.setLength(b.length() - 1);
		}

		return b.toString();
	}

	protected void stringToVarList(String list) {
		pyVarsToGet.clear();
		String[] vars = list.split(",");
		for (String v : vars) {
			if (!org.apache.hop.core.util.Utils.isEmpty(v.trim())) {
				pyVarsToGet.add(v.trim());
			}
		}
	}

	@Override
	public Object clone() {
		CPythonScriptExecutorMeta retval = (CPythonScriptExecutorMeta) super.clone();

		return retval;
	}

	public void clearStepIOMeta() {
		ITransformIOMeta ioMeta = super.getTransformIOMeta();
		((TransformIOMeta) ioMeta).clearStreams();
	}

	@Override
	public void resetTransformIoMeta() {
		// Don't reset
	}

	@Override
	public ITransformIOMeta getTransformIOMeta() {

		ITransformIOMeta ioMeta = super.getTransformIOMeta();
		if (ioMeta.getInfoStreams().isEmpty()) {
			((TransformIOMeta) ioMeta).setInputAcceptor(true);
			((TransformIOMeta) ioMeta).setOutputProducer(true);
			((TransformIOMeta) ioMeta).setInputOptional(true);
			((TransformIOMeta) ioMeta).setSortedDataRequired(false);
			ioMeta.setInputDynamic(false);
			ioMeta.setOutputDynamic(false);

			int expectedStreams = (getInputFrames() != null) ? getInputFrames().size() : 0;
			for (int i = 0; i < expectedStreams; i++) {
				ioMeta.addStream(
						new Stream(StreamType.INFO, null, "Input to pandas frame " + (i + 1), StreamIcon.INFO, null));
			}
		}
		return ioMeta;
	}

	@Override
	public void searchInfoAndTargetTransforms(List<TransformMeta> steps) {
		List<IStream> targetStreams = getTransformIOMeta().getTargetStreams();

		for (IStream stream : targetStreams) {
			stream.setTransformMeta(TransformMeta.findTransform(steps, (String) stream.getSubject()));
		}
	}

	@Override
	public String getDialogClassName() {
		return CPythonScriptExecutorDialog.class.getCanonicalName();
	}

	public enum ProcessingMode {
		ROW_BY_ROW("RowByRow"), BATCH("Batch"), ALL("All");

		private final String code;

		ProcessingMode(String code) {
			this.code = code;
		}

		public String getCode() {
			return code;
		}

		public static ProcessingMode fromCode(String code) {
			if (code == null || code.trim().isEmpty()) {
				return ALL; // Default fallback
			}
			for (ProcessingMode mode : values()) {
				if (mode.getCode().equalsIgnoreCase(code) || mode.name().equalsIgnoreCase(code)) {
					return mode;
				}
			}
			return ALL;
		}
	}

	@Override
	public void check(List<ICheckResult> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta, IRowMeta prev,
			String[] input, String[] output, IRowMeta info, IVariables variables,
			IHopMetadataProvider metadataProvider) {
		CheckResult cr;

		if (org.apache.hop.core.util.Utils.isEmpty(getScript())
				&& org.apache.hop.core.util.Utils.isEmpty(getLoadScriptFile())) {
			cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR,
					BaseMessages.getString(PKG, "CPythonScriptExecutor.Error.NoScriptProvided"), transformMeta);
		} else {
			cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, "A Python script or script file is provided.",
					transformMeta);
		}
		remarks.add(cr);

		List<IStream> infoStreams = getTransformIOMeta().getInfoStreams();
		List<CPythonInputFrame> inputFrames = getInputFrames();

		if (inputFrames != null && !inputFrames.isEmpty()) {
			if (infoStreams.size() != inputFrames.size()) {
				cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR,
						BaseMessages.getString(PKG, "CPythonScriptExecutor.Error.InputStreamToFrameNameMismatch"),
						transformMeta);
				remarks.add(cr);
			} else {
				cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, "Input frames map correctly to input streams.",
						transformMeta);
				remarks.add(cr);
			}
		}

		if (input.length > 0) {
			cr = new CheckResult(ICheckResult.TYPE_RESULT_OK,
					"Transform is receiving info/data from previous transforms.", transformMeta);
		} else {
			cr = new CheckResult(ICheckResult.TYPE_RESULT_WARNING,
					"No input received from other transforms. Ensure this is intentional.", transformMeta);
		}
		remarks.add(cr);
	}
}
