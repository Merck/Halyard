package com.msd.gin.halyard.function;

import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.common.Identifier;
import com.msd.gin.halyard.common.RDFFactory;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Table;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunction;
import org.kohsuke.MetaInfServices;

@MetaInfServices(TupleFunction.class)
public final class PredicateTupleFunction extends AbstractReificationTupleFunction {

	@Override
	public String getURI() {
		return RDF.PREDICATE.stringValue();
	}

	@Override
	protected int statementPosition() {
		return 1;
	}

	@Override
	protected Value getValue(Table table, Identifier id, RDFFactory rdfFactory) throws IOException {
		return HalyardTableUtils.getPredicate(table, id, rdfFactory);
	}
}
