package com.msd.gin.halyard.rio;

import org.eclipse.rdf4j.rio.RDFFormat;

public final class HRDF {
	public static final RDFFormat FORMAT = new RDFFormat("HRDF", "hrdf", null, "hrdf", RDFFormat.NO_NAMESPACES, RDFFormat.SUPPORTS_CONTEXTS, RDFFormat.SUPPORTS_RDF_STAR);
	static final int TRIPLES = 3;
	static final int QUADS = 4;

	private HRDF() {}
}
