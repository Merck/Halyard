package com.msd.gin.halyard.sail;

import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailException;

public interface BindingSetPipeSail extends Sail {
	@Override
	BindingSetPipeSailConnection getConnection() throws SailException;
}
