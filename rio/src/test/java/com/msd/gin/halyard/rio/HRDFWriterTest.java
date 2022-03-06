package com.msd.gin.halyard.rio;

import org.eclipse.rdf4j.rio.RDFWriterTest;
import org.eclipse.rdf4j.rio.RioSetting;

public class HRDFWriterTest extends RDFWriterTest {
	public HRDFWriterTest() {
		super(new HRDFWriter.Factory(), new HRDFParser.Factory());
	}

	@Override
	protected RioSetting<?>[] getExpectedSupportedSettings() {
		return new RioSetting<?>[] {};
	}
}
