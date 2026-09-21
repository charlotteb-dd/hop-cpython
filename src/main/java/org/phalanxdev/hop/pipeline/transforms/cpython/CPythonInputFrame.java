package org.phalanxdev.hop.pipeline.transforms.cpython;

import org.apache.hop.metadata.api.HopMetadataProperty;

public class CPythonInputFrame {

	@HopMetadataProperty(key = "transform_name")
	private String transformName;

	@HopMetadataProperty(key = "frame_name")
	private String frameName;

	public CPythonInputFrame() {
	}

	public CPythonInputFrame(String stepName, String frameName) {
		this.transformName = stepName;
		this.frameName = frameName;
	}

	public String getTransformName() {
		return transformName;
	}

	public void setTransformName(String stepName) {
		this.transformName = stepName;
	}

	public String getFrameName() {
		return frameName;
	}

	public void setFrameName(String frameName) {
		this.frameName = frameName;
	}
}