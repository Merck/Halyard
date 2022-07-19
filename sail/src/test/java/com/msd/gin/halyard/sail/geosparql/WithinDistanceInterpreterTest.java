package com.msd.gin.halyard.sail.geosparql;

import com.msd.gin.halyard.algebra.Algebra;
import com.msd.gin.halyard.vocab.HALYARD;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.TupleFunctionCall;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class WithinDistanceInterpreterTest {
	@Test
	public void testSimpleQuery() {
		String query = "PREFIX geo: <http://www.opengis.net/ont/geosparql#> " + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> " + "PREFIX uom: <http://www.opengis.net/def/uom/OGC/1.0/> "
				+ "select * { ?s geo:asWKT ?to. filter(geof:distance('POINT (0 0)'^^geo:wktLiteral, ?to, uom:metre) < 250) }";
		testQuery(query);
	}

	@Test
	public void testComplexQuery() {
		String query = "PREFIX geo: <http://www.opengis.net/ont/geosparql#> " + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> " + "PREFIX uom: <http://www.opengis.net/def/uom/OGC/1.0/> "
				+ "select * { <http://from> geo:asWKT ?from. <http://whatever> geo:asWKT ?to. values (?units ?limit) {(uom:degree 150) (uom:radian 250)} bind(geof:distance(?from, ?to, ?units) as ?distance) filter(?distance < ?limit) }";
		testQuery(query);
	}

	private void testQuery(String query) {
		ParsedTupleQuery q = QueryParserUtil.parseTupleQuery(QueryLanguage.SPARQL, query, null);
		TupleExpr expr = Algebra.ensureRooted(q.getTupleExpr());
		new WithinDistanceInterpreter().optimize(expr, null, null);
		List<TupleFunctionCall> tfcs = new ArrayList<>();
		expr.visit(new AbstractQueryModelVisitor<RuntimeException>() {
			@Override
			public void meetOther(QueryModelNode node) {
				if (node instanceof TupleFunctionCall) {
					meet((TupleFunctionCall) node);
				} else {
					super.meetOther(node);
				}
			}

			public void meet(TupleFunctionCall tfc) {
				tfcs.add(tfc);
			}
		});
		assertEquals(1, tfcs.size(), expr.toString());
		assertEquals(HALYARD.WITHIN_DISTANCE.stringValue(), tfcs.get(0).getURI());
	}
}
