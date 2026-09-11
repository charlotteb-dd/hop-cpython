package org.phalanxdev.hop.pipeline.transforms.cpython;

import org.apache.hop.metadata.api.HopMetadataProperty;

public class CPythonInputFrame {

  @HopMetadataProperty(key = "step_name")
  private String stepName;

  @HopMetadataProperty(key = "frame_name")
  private String frameName;

  public CPythonInputFrame() {
  }

  public CPythonInputFrame(String stepName, String frameName) {
    this.stepName = stepName;
    this.frameName = frameName;
  }

  public String getStepName() { return stepName; }
  public void setStepName(String stepName) { this.stepName = stepName; }
  public String getFrameName() { return frameName; }
  public void setFrameName(String frameName) { this.frameName = frameName; }
}