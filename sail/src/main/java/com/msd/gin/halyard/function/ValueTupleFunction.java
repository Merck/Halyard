package com.msd.gin.halyard.function;

import com.msd.gin.halyard.vocab.HALYARD;

import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunction;
import org.eclipse.rdf4j.spin.function.InverseMagicProperty;
import org.kohsuke.MetaInfServices;

/**
 * The reverse predicate of {@link IdentifierTupleFunction}.
 */
@MetaInfServices(TupleFunction.class)
public class ValueTupleFunction extends IdentifierTupleFunction implements InverseMagicProperty {

	@Override
	public String getURI() {
		return HALYARD.VALUE_PROPERTY.toString();
	}
}
