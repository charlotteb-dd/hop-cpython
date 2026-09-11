package org.phalanxdev.hop.pipeline.transforms.cpython;

import org.apache.hop.metadata.api.HopMetadataProperty;

public class CPythonOutputField {
  @HopMetadataProperty
  private String name;

  @HopMetadataProperty
  private String type;

  public CPythonOutputField() {
  }

  public CPythonOutputField(String name, String type) {
    this.name = name;
    this.type = type;
  }

  public String getName() { return name; }
  public void setName(String name) { this.name = name; }
  public String getType() { return type; }
  public void setType(String type) { this.type = type; }
}