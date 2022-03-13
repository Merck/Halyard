package com.msd.gin.halyard.rio;

import org.eclipse.rdf4j.rio.RDFFormat;

public final class HRDF {
	public static final RDFFormat FORMAT = new RDFFormat("HRDF", "hrdf", null, "hrdf", RDFFormat.NO_NAMESPACES, RDFFormat.SUPPORTS_CONTEXTS, RDFFormat.SUPPORTS_RDF_STAR);
	static final int TRIPLES = 0;
	static final int QUADS = 4;
	static final int O = 1;
	static final int PO = 2;
	static final int SPO = 3;
	static final int CSPO = 4;

	private HRDF() {}
}
