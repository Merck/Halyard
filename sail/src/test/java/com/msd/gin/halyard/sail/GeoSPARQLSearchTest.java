package com.msd.gin.halyard.sail;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.GEO;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;

public class GeoSPARQLSearchTest extends AbstractSearchTest {
	private final ValueFactory vf = SimpleValueFactory.getInstance();
	private final Literal origin = vf.createLiteral("POINT (0 0)", org.eclipse.rdf4j.model.base.CoreDatatype.GEO.WKT_LITERAL);
	private final Literal pos1 = vf.createLiteral("POINT (1 0)", org.eclipse.rdf4j.model.base.CoreDatatype.GEO.WKT_LITERAL);
	private final Literal pos2 = vf.createLiteral("POINT (2 0)", org.eclipse.rdf4j.model.base.CoreDatatype.GEO.WKT_LITERAL);
	private final Literal pos3 = vf.createLiteral("POINT (3 0)", org.eclipse.rdf4j.model.base.CoreDatatype.GEO.WKT_LITERAL);

    @Test
	public void literalWithinDistance() throws Exception {
		try (ServerSocket server = startElasticsearch(pos1, pos2)) {
			IRI whatever = vf.createIRI("http://whatever");
			Repository hbaseRepo = createRepo("literalWithinDistance", server);
			try (RepositoryConnection conn = hbaseRepo.getConnection()) {
				conn.add(whatever, GEO.AS_WKT, pos1);
				conn.add(whatever, GEO.AS_WKT, pos2);
				conn.add(whatever, GEO.AS_WKT, pos3);
				TupleQuery q = conn.prepareTupleQuery("PREFIX geo: <http://www.opengis.net/ont/geosparql#> " + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> " + "PREFIX uom: <http://www.opengis.net/def/uom/OGC/1.0/> "
						+ "select * { ?s geo:asWKT ?to. filter(geof:distance('POINT (0 0)'^^geo:wktLiteral, ?to, uom:degree) < 2.5) }");
				List<Value> actual = new ArrayList<>();
				try (TupleQueryResult iter = q.evaluate()) {
					while (iter.hasNext()) {
						BindingSet bs = iter.next();
						actual.add(bs.getValue("to"));
					}
				}
				assertThat(actual).containsExactlyInAnyOrder(pos1, pos2);
			}
			hbaseRepo.shutDown();
		}
	}

	@Test
	public void varWithinDistance() throws Exception {
		try (ServerSocket server = startElasticsearch(Arrays.asList(new Literal[] { pos1 }, new Literal[] { pos2 }))) {
			IRI from = vf.createIRI("http://from");
			IRI whatever = vf.createIRI("http://whatever");
			Repository hbaseRepo = createRepo("varWithinDistance", server);
			try (RepositoryConnection conn = hbaseRepo.getConnection()) {
				conn.add(from, GEO.AS_WKT, origin);
				conn.add(whatever, GEO.AS_WKT, pos1);
				conn.add(whatever, GEO.AS_WKT, pos2);
				conn.add(whatever, GEO.AS_WKT, pos3);
				TupleQuery q = conn.prepareTupleQuery("PREFIX geo: <http://www.opengis.net/ont/geosparql#> " + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> " + "PREFIX uom: <http://www.opengis.net/def/uom/OGC/1.0/> "
						+ "select * { <http://from> geo:asWKT ?from. <http://whatever> geo:asWKT ?to. values (?units ?limit) {(uom:metre 111200) (uom:radian 0.05)} filter(geof:distance(?from, ?to, ?units) < ?limit) }");
				List<Value> actual = new ArrayList<>();
				try (TupleQueryResult iter = q.evaluate()) {
					while (iter.hasNext()) {
						BindingSet bs = iter.next();
						actual.add(bs.getValue("to"));
					}
				}
				assertThat(actual).containsExactlyInAnyOrder(pos1, pos2);
			}
			hbaseRepo.shutDown();
		}
	}

	@Test
	public void bindWithinDistance() throws Exception {
		try (ServerSocket server = startElasticsearch(Arrays.asList(new Literal[] { pos1 }, new Literal[] { pos2 }))) {
			IRI from = vf.createIRI("http://from");
			IRI whatever = vf.createIRI("http://whatever");
			Repository hbaseRepo = createRepo("bindWithinDistance", server);
			try (RepositoryConnection conn = hbaseRepo.getConnection()) {
				conn.add(from, GEO.AS_WKT, origin);
				conn.add(whatever, GEO.AS_WKT, pos1);
				conn.add(whatever, GEO.AS_WKT, pos2);
				conn.add(whatever, GEO.AS_WKT, pos3);
				TupleQuery q = conn.prepareTupleQuery("PREFIX geo: <http://www.opengis.net/ont/geosparql#> " + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> " + "PREFIX uom: <http://www.opengis.net/def/uom/OGC/1.0/> "
						+ "select * { <http://from> geo:asWKT ?from. <http://whatever> geo:asWKT ?to. values (?units ?limit) {(uom:metre 111200) (uom:radian 0.05)} bind(geof:distance(?from, ?to, ?units) as ?distance) filter(?distance < ?limit) }");
				List<List<Value>> actual = new ArrayList<>();
				try (TupleQueryResult iter = q.evaluate()) {
					while (iter.hasNext()) {
						BindingSet bs = iter.next();
						actual.add(Arrays.asList(bs.getValue("to"), bs.getValue("distance")));
					}
				}
				assertThat(actual).containsExactlyInAnyOrder(Arrays.asList(pos1, vf.createLiteral(111195.07973436874)), Arrays.asList(pos2, vf.createLiteral(0.03490658503988659)));
			}
			hbaseRepo.shutDown();
		}
	}
}
