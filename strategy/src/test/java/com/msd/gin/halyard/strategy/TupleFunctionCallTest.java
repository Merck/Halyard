package com.msd.gin.halyard.strategy;

import com.google.common.collect.Sets;
import com.msd.gin.halyard.algebra.ExtendedTupleFunctionCall;
import com.msd.gin.halyard.algebra.evaluation.EmptyTripleSource;
import com.msd.gin.halyard.optimizers.HalyardEvaluationStatistics;
import com.msd.gin.halyard.optimizers.SimpleStatementPatternCardinalityCalculator;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.SingletonIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunction;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunctionRegistry;
import org.eclipse.rdf4j.query.impl.SimpleDataset;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TupleFunctionCallTest {
	@Test
	public void testDependency() {
		ValueFactory vf = SimpleValueFactory.getInstance();
		HalyardEvaluationStrategy strategy = new HalyardEvaluationStrategy(new Configuration(),
				new EmptyTripleSource(vf),
				new SimpleDataset(),
				null, new HalyardEvaluationStatistics(SimpleStatementPatternCardinalityCalculator.FACTORY, null));
		QueryBindingSet bs1 = new QueryBindingSet();
		bs1.addBinding("a", vf.createIRI("http://test/1"));
		QueryBindingSet bs2 = new QueryBindingSet();
		bs2.addBinding("a", vf.createIRI("http://test/2"));
		BindingSetAssignment bsa = new BindingSetAssignment();
		bsa.setBindingNames(Sets.newHashSet("a"));
		bsa.setBindingSets(Arrays.asList(bs1, bs2));
		TupleFunction tf = new TupleFunction() {
			@Override
			public String getURI() {
				return "http://tfc/a";
			}

			@Override
			public CloseableIteration<? extends List<? extends Value>, QueryEvaluationException> evaluate(ValueFactory vf, Value... args) throws QueryEvaluationException {
				IRI iri = (IRI) args[0];
				return new SingletonIteration<List<? extends Value>, QueryEvaluationException>(Arrays.asList(vf.createLiteral(iri.getLocalName())));
			}
		};
		ExtendedTupleFunctionCall tfc = new ExtendedTupleFunctionCall(tf.getURI());
		tfc.addArg(new Var("a"));
		tfc.addResultVar(new Var("out"));
		tfc.setDependentExpression(bsa);
		TupleFunctionRegistry.getInstance().add(tf);
		Set<String> outValues = new HashSet<>();
		CloseableIteration<BindingSet,QueryEvaluationException> iter = strategy.evaluate(tfc, new QueryBindingSet());
		while(iter.hasNext()) {
			outValues.add(iter.next().getValue("out").stringValue());
		}
		assertEquals(Sets.newHashSet("1", "2"), outValues);
	}
}
