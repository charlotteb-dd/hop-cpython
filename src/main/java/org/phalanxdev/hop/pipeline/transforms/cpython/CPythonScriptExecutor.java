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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.phalanxdev.hop.pipeline.transforms.reservoirsampling.ReservoirSamplingData;
import org.phalanxdev.python.PythonSession;

/**
 * Step that executes a python script using CPython. The step can accept 0 or more incoming row
 * sets. Row sets are sent to python as named pandas data frames. Data can be sent to python in
 * batches, as samples, row-by-row or as all available rows.
 * </p>
 * Output can be one or more variables that are set in python after the user's script executes. In
 * the case of a single variable this can be a data frame, in which case the columns of the frame
 * become output fields from this step. In the case of multiple variables they are retrieved in
 * string form or as png image data - the step automatically detects if a variable is an image and
 * retrieves it as png. In this mode there is one row output from the step, where each outgoing
 * field holds the string/serializable value of a single variable.
 * </p>
 * The step requires python 2.7 or 3.4. It also requires the pandas, numpy, matplotlib and sklearn.
 * The python executable must be available in the user's path.
 *
 * @author Mark Hall (mhall{[at]}phalanxdev{[dot]}com)
 */
public class CPythonScriptExecutor extends BaseTransform<CPythonScriptExecutorMeta, CPythonScriptExecutorData> {

  private static Class<?> PKG = CPythonScriptExecutorMeta.class;

  protected CPythonScriptExecutorData data;
  protected CPythonScriptExecutorMeta meta;

  protected boolean noInputRowSets = false;

  public CPythonScriptExecutor( TransformMeta transformMeta, CPythonScriptExecutorMeta meta,
      CPythonScriptExecutorData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );

    this.meta = meta;
    this.data = data;
  }

  public boolean init() {
    if ( super.init() ) {
      try {
        if ( org.apache.hop.core.util.Utils.isEmpty( meta.getScript() ) && org.apache.hop.core.util.Utils
            .isEmpty( meta.getLoadScriptFile() ) ) {
          throw new HopException( BaseMessages.getString( PKG, "CPythonScriptExecutor.Error.NoScriptProvided" ) );
        }

        List<CPythonInputFrame> inputFrames = meta.getInputFrames();
        if (inputFrames != null && inputFrames.size() > 0) {
        	if ( meta.getStepIOMeta().getInfoStreams().size() != inputFrames.size() ) {
                throw new HopException(
                    BaseMessages.getString( PKG, "CPythonScriptExecutor.Error.InputStreamToFrameNameMismatch" ) );
              }
        }

        if ( data.script == null ) {
          // loading from a file overrides any user-supplied script
          if ( meta.isLoadScriptAtRuntime() ) {
            String scriptFile = resolve( meta.getLoadScriptFile() );
            if ( org.apache.hop.core.util.Utils.isEmpty( scriptFile ) ) {
              throw new HopException(
                  BaseMessages.getString( PKG, "CPythonScriptExecutor.Error.NoScriptFileNameProvided" ) );
            }

            data.script = CPythonScriptExecutorData.loadScriptFromFile( scriptFile );
          } else {
            data.script = meta.getScript();
          }
        }

        if ( !data.includeInputAsOutput ) {
          data.includeInputAsOutput = meta.isIncludeInputAsOutput();
        }

        // check python availability
        CPythonScriptExecutorData
            .initPython( meta.getPythonCommand(), meta.getServerID(), meta.getPyPathEntries(), this, getLogChannel() );
      } catch ( HopException ex ) {
        logError( ex.getMessage(), ex ); //$NON-NLS-1$

        return false;
      }

      return true;
    }

    return false;
  }

  @Override public boolean processRow() throws HopException {

    if ( first ) {
      first = false;

      List<IStream> infoStreams = meta.getStepIOMeta().getInfoStreams();
      IRowMeta[] infos = new IRowMeta[infoStreams.size()];
      data.incomingRowSets = new ArrayList<IRowSet>();

      if ( infoStreams.size() == 0 ) {
        noInputRowSets = true;
      } else {
        String rowsToProcess = meta.getRowsToProcess();
        String rowsToProcessSize = resolve( meta.getRowsToProcessSize() );

        // TODO: should not compare to diff language like that because can break if the user change language,
        // should make an enum [ALL, ROW_BY_ROW, BATCH]
        if ( rowsToProcess.equals( BaseMessages
            .getString( PKG, "CPythonScriptExecutorDialog.NumberOfRowsToProcess.Dropdown.BatchEntry.Label" ) ) ) {
          data.batchSize = Integer.parseInt( rowsToProcessSize.isEmpty() ? "0" : rowsToProcessSize );
        } else if ( rowsToProcess.equals( BaseMessages
            .getString( PKG, "CPythonScriptExecutorDialog.NumberOfRowsToProcess.Dropdown.RowByRowEntry.Label" ) ) ) {
          data.batchSize = 1;
        } else {
          data.batchSize = 0;
        }

        String reservoirSamplersSize = resolve( meta.getReservoirSamplingSize() );
        boolean doingReservoirSampling = meta.isDoingReservoirSampling();
        if ( doingReservoirSampling ) {
          data.reservoirSamplersSize =
              Integer.parseInt( reservoirSamplersSize.isEmpty() ? "0" : reservoirSamplersSize );
        } else {
          data.reservoirSamplersSize = 0;
        }

        // check for reservoir sampling and set up Reservoirs
        if ( !doingReservoirSampling ) {
          for ( int i = 0; i < infoStreams.size(); i++ ) {
            data.frameBuffers.add( new ArrayList<Object[]>() );
          }
        } else {
          data.reservoirSamplers = new ArrayList<ReservoirSamplingData>();
          String seed = resolve( meta.getSeed() );
          for ( int i = 0; i < infoStreams.size(); i++ ) {
            ReservoirSamplingData rs = new ReservoirSamplingData();
            rs.setProcessingMode( ReservoirSamplingData.PROC_MODE.SAMPLING );
            if ( data.reservoirSamplersSize < 0 && ( doingReservoirSampling || infoStreams.size() > 1 ) ) {
              // The reservoir sampler is disabled when the sample size is < 0, so we have to
              // set some arbitrarily large sample size in this case in order to simulate the
              // "don't sample, just store all rows" scenario
              data.reservoirSamplersSize = CPythonScriptExecutorData.DEFAULT_RESERVOIR_SAMPLING_STORE_ALL_ROWS_SIZE;
            }
            rs.initialize( data.reservoirSamplersSize, seed.isEmpty() ? 0 : Integer.parseInt( seed ) );
            data.reservoirSamplers.add( rs );

            if ( data.batchSize == 1 ) { //only the first input frame should be considered
              break;
            }
          }
        }

        List<CPythonInputFrame> inputFrames = meta.getInputFrames();
        for (int i = 0; i < inputFrames.size(); i++) {
//        for ( int i = 0; i < infoStreams.size(); i++ ) {
        	String transformName = inputFrames.get(i).getTransformName();
        	String frameName = inputFrames.get(i).getFrameName();
          IRowSet current = findInputRowSet(transformName);
          IRowMeta associatedRowMeta = getPipelineMeta().getTransformFields( this, transformName );
//              getPipelineMeta().getTransformFields( variables, infoStreams.get( i ).getSubject().toString() );

          if ( current == null ) {
            throw new HopException( BaseMessages
                .getString( PKG, "CPythonScriptExecutor.Error.UnableToFindSpecifiedInputStep",
                   transformName ) ); //$NON-NLS-1$
          }
          data.incomingRowSets.add( current );
          infos[i] = associatedRowMeta;

          if ( infos[i] == null ) {
            throw new HopException( "No row meta for incoming row set " + i ); //$NON-NLS-1$
          }
        }

        data.finishedRowSets = new boolean[data.incomingRowSets.size()];
        data.infoMetas.addAll( List.of( infos ) );
      }
      data.outputRowMeta = new RowMeta();
      data.scriptOnlyOutputRowMeta = new RowMeta();
      data.incomingFieldsIncludedInOutputRowMeta = new RowMeta();

      meta.getFields( data.outputRowMeta, getTransformName(), infos, null, variables, metadataProvider);
      meta.determineInputFieldScriptFieldSplit( data.outputRowMeta, data.scriptOnlyOutputRowMeta,
          data.incomingFieldsIncludedInOutputRowMeta, infos, getTransformName() );
      data.initNonScriptOutputIndexLookup();
    }

    if ( isStopped() ) {
      return false;
    }

    boolean allDone = true;
    for ( int i = 0; i < data.incomingRowSets.size(); i++ ) {
      if ( isStopped() ) {
        return false;
      }

      if ( !data.finishedRowSets[i] ) {
        IRowSet r = data.incomingRowSets.get( i );
        Object[] row = getRowFrom( r );

        if ( row != null ) {
          allDone = false;

          if ( !meta.isDoingReservoirSampling() ) {
            data.frameBuffers.get( i ).add( row );
          } else {
            data.reservoirSamplers.get( i ).processRow( row );
          }
        } else {
          data.finishedRowSets[i] = true;
        }
      }
    }

    processBatch( allDone );

    if ( allDone ) {
      setOutputDone();

      return false;
    }

    if ( checkFeedback( getLinesRead() ) ) {
      logBasic(
          BaseMessages.getString( PKG, "CPythonScriptExecutor.Message.LineNumber", getLinesRead() ) ); //$NON-NLS-1$
    }

    return true;
  }

  protected void processBatch( boolean allDone ) throws HopException {
    PythonSession session = null;

    try {
      if ( !noInputRowSets && !meta.isDoingReservoirSampling() && data.incomingRowSets.size() >= 1 ) {
        boolean framesAdded = false;
        for ( int i = 0; i < data.frameBuffers.size(); i++ ) {
          List<Object[]> frameBuffer = data.frameBuffers.get( i );
          if ( (frameBuffer.size() == data.batchSize && frameBuffer.size() > 0) || ( allDone && frameBuffer.size() > 0 ) ) {
            // push buffer into python and process result
            String frameName = resolve( meta.getInputFrames().get( i ).getFrameName() );

            logDetailed( BaseMessages.getString( PKG, "CPythonScriptExecutor.Message.PushingBatchIntoPandasDataFrame",
                //$NON-NLS-1$
                frameBuffer.size(), frameName ) );

            // session = CPythonScriptExecutorData.acquirePySession(this, getLogChannel(), this);
            session =
                CPythonScriptExecutorData
                    .acquirePySession( this, meta.getPythonCommand(), meta.getServerID(), getLogChannel(),
                        this );
            // Configure Arrow usage based on meta configuration
            session.setUseArrow( meta.isUseArrow() );
            rowsToPyDataFrame( session, data.incomingRowSets.get( i ).getRowMeta(), frameBuffer, frameName );
            framesAdded = true;
          } else {
            framesAdded = false;
          }
        }

        if ( framesAdded ) {
          executeScriptAndProcessResult( session, meta.isContinueOnUnsetVars() );
          //clean the current frame buffers
          for ( List<Object[]> frame : data.frameBuffers ) {
            frame.clear();
          }
        }
      } else if ( !noInputRowSets && allDone ) {
        boolean framesAdded = false;
        session =
            CPythonScriptExecutorData
                .acquirePySession( this, meta.getPythonCommand(), meta.getServerID(), getLogChannel(), this );
        // Configure Arrow usage based on meta configuration
        session.setUseArrow( meta.isUseArrow() );

        // grab all the reservoirs an push to python; then process result
        logDetailed( BaseMessages.getString( PKG, "CPythonScriptExecutor.Message.RetrievingReservoirs" ) );
        for ( int j = 0; j < data.reservoirSamplers.size(); j++ ) {
          ReservoirSamplingData reservoirSamplers = data.reservoirSamplers.get( j );
          String frameName = resolve( meta.getInputFrames().get( j ).getFrameName() );
          List<Object[]> sample = reservoirSamplers.getSample();
          CPythonScriptExecutorData.pruneNullRowsFromSample( sample );

          if ( sample != null && sample.size() > 0 ) {
            logDetailed( BaseMessages
                .getString( PKG, "CPythonScriptExecutor.Message.PushingSampleFromReservoirIntoPandasDataFrame", j,
                    frameName ) ); //$NON-NLS-1$
            logDetailed( BaseMessages
                .getString( PKG, "CPythonScriptExecutor.Message.SampleSize", sample.size() ) ); //$NON-NLS-1$

            if ( data.batchSize == 1 ) { //we need to process row by row the sample. we will only have one sample
              List<Object[]> sampleSpliced = new ArrayList<Object[]>();
              for ( int k = 0; k < sample.size(); k++ ) {
                Object[] objects = sample.get( k );
                session =
                    CPythonScriptExecutorData
                        .acquirePySession( this, meta.getPythonCommand(), meta.getServerID(), getLogChannel(),
                            this );
                // Configure Arrow usage based on meta configuration
                session.setUseArrow( meta.isUseArrow() );
                sampleSpliced.clear();
                sampleSpliced.add( objects );
                rowsToPyDataFrame( session, data.incomingRowSets.get( j ).getRowMeta(), sampleSpliced, frameName );
                data.rowByRowReservoirSampleIndex = k;

                executeScriptAndProcessResult( session, meta.isContinueOnUnsetVars() );

                if ( session != null ) {
                  CPythonScriptExecutorData
                      .releasePySession( this, meta.getPythonCommand(), meta.getServerID(), this );
                }
              }

            } else { //process the full sample
              rowsToPyDataFrame( session, data.incomingRowSets.get( j ).getRowMeta(), sample, frameName );
            }
          }
        }

        if ( data.batchSize != 1 ) {
          executeScriptAndProcessResult( session, meta.isContinueOnUnsetVars() );
        }
      } else if ( noInputRowSets ) {
        // just get results from script as we have no inputs to us
        session =
            CPythonScriptExecutorData
                .acquirePySession( this, meta.getPythonCommand(), meta.getServerID(), getLogChannel(), this );
        // Configure Arrow usage based on meta configuration
        session.setUseArrow( meta.isUseArrow() );
        executeScriptAndProcessResult( session, meta.isContinueOnUnsetVars() );
      }
    } finally {
      if ( session != null ) {
        CPythonScriptExecutorData.releasePySession( this, meta.getPythonCommand(), meta.getServerID(), this );
      }
    }
  }

  protected void executeScriptAndProcessResult( PythonSession session, boolean continueOnUnsetVars )
      throws HopException {
    executeScript( session, data.script );

    Object[][] scriptOutRows = null;
    if ( meta.getPyVarsToGet().size() == 1 ) {
      // check for existence first...
      if ( !checkIfPythonVariableIsSet( session, meta.getPyVarsToGet().get( 0 ) ) ) {
        if ( !continueOnUnsetVars ) {
          throw new HopException( BaseMessages.getString( PKG, "CPythonScriptExecutor.Error.PythonVariableNotSet",
              meta.getPyVarsToGet().get( 0 ) ) );
        }
      } else {
        // Are we getting a variable (image/text) or a frame?
        PythonSession.PythonVariableType
            type =
            getPythonVariableType( session, meta.getPyVarsToGet().get( 0 ) );
        Object[][] outputRows = new Object[1][];
        if ( type == PythonSession.PythonVariableType.DataFrame ) {
          outputRows =
              data.constructOutputRowsFromFrame( session, meta.getPyVarsToGet().get( 0 ),
                  meta.isIncludeRowIndex(), getLogChannel() );
          includeInputInOutput( outputRows );
        } else {
          outputRows[0] =
              data.constructOutputRowNonFrame( session, meta.getPyVarsToGet(), continueOnUnsetVars,
                  getLogChannel() );
          includeInputInOutput( outputRows );
        }
      }
    } else {
      // more than one variable to get - only non-frame case
      Object[][] outputRows = new Object[1][];
      outputRows[0] =
          data.constructOutputRowNonFrame( session, meta.getPyVarsToGet(), continueOnUnsetVars,
              getLogChannel() );
      includeInputInOutput( outputRows );
    }
  }

  protected void includeInputInOutput( Object[][] outputRows ) throws HopException {
    if ( !meta.isIncludeInputAsOutput() ) {

      for ( Object[] r : outputRows ) {
        putRow( data.outputRowMeta, r );
      }

      return;
    }

    List<Object[]> flattenedInputRows = new ArrayList<Object[]>();
    int[]
        rowCounts =
        new int[meta.isDoingReservoirSampling() ? data.reservoirSamplers.size() : data.frameBuffers.size()];
    // IRowMeta[] infoMetas = new IRowMeta[rowCounts.length];
    int index = 0;
    int sum = 0;
    if ( !meta.isDoingReservoirSampling() ) {
      for ( List<Object[]> frameBuffer : data.frameBuffers ) {
        sum += frameBuffer.size();
        rowCounts[index++] = sum;
        flattenedInputRows.addAll( frameBuffer );
      }
    } else {
      for ( ReservoirSamplingData reservoirSamplingData : data.reservoirSamplers ) {
        sum += reservoirSamplingData.getSample().size();
        rowCounts[index++] = sum;
        flattenedInputRows.addAll( reservoirSamplingData.getSample() );
      }
    }

    index = 0;
    for ( int i = 0; i < outputRows.length; i++ ) {
      if ( i > rowCounts[index] ) {
        index++;
      }
      // get the input row meta corresponding to this row
      IRowMeta associatedRowMeta = data.infoMetas.get( index );
      if ( outputRows[i] != null ) {
        Object[] inputRow;
        if ( data.batchSize == 1 && meta.isDoingReservoirSampling() ) {
          inputRow = flattenedInputRows.get( data.rowByRowReservoirSampleIndex );
        } else {
          inputRow = flattenedInputRows.get( i );
        }
        for ( IValueMeta vm : data.incomingFieldsIncludedInOutputRowMeta.getValueMetaList() ) {
          int outputIndex = data.nonScriptOutputMetaIndexLookup.get( vm.getName() );
          // is this user selected input field present in the current info input row set?
          int inputIndex = associatedRowMeta.indexOfValue( vm.getName() );
          if ( inputIndex >= 0 ) {
            outputRows[i][outputIndex] = inputRow[inputIndex];
          }
        }
        putRow( data.outputRowMeta, outputRows[i] );
      }
    }
  }

  protected void executeScript( PythonSession session, String pyScript ) throws HopException {
    List<String> outAndErr = session.executeScript( resolve( pyScript ) );

    // TODO could add another setting to allow the user to specify if the step
    // should try to continue after a script execution error. Note that ServerUtils
    // already logs warning messages and strips them from the error output.
    if ( !org.apache.hop.core.util.Utils.isEmpty( outAndErr.get( 1 ) ) ) {
      throw new HopException( outAndErr.get( 1 ) );
    }
  }

  protected void rowsToPyDataFrame( PythonSession session, IRowMeta rowMeta, List<Object[]> rows, String pyFrameName )
      throws HopException {
    // Use the new Arrow-enabled method which automatically falls back to CSV if needed
    session.rowsToPythonDataFrameWithArrow( rowMeta, rows, pyFrameName );
  }

  protected PythonSession.PythonVariableType getPythonVariableType( PythonSession session, String varName )
      throws HopException {
    return session.getPythonVariableType( varName );
  }

  protected boolean checkIfPythonVariableIsSet( PythonSession session, String varName ) throws HopException {
    return session.checkIfPythonVariableIsSet( varName );
  }
}
