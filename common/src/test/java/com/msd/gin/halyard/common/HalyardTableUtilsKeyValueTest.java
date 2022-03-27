package com.msd.gin.halyard.common;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class HalyardTableUtilsKeyValueTest {
    private static final IdentifiableValueIO valueIO = IdentifiableValueIO.create();
	private static final ValueFactory vf = new TimestampedValueFactory(valueIO);

    private static final IRI SUBJ1 = vf.createIRI("http://whatever/subj1");
    private static final IRI SUBJ2 = RDF.NIL;
    private static final IRI PRED1 = RDF.TYPE;
    private static final IRI PRED2 = vf.createIRI("http://whatever/pred");
    private static final Literal EXPL1 = vf.createLiteral("whatever explicit value1");
    private static final Literal EXPL2 = vf.createLiteral("whatever explicit value2");
    private static final IRI CTX1 = RDF.STATEMENT;
    private static final BNode CTX2 = vf.createBNode();

    @Parameters(name = "{0}, {1}, {2}, {3}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                 {SUBJ1, PRED1, EXPL1, null},
                 {SUBJ2, PRED2, EXPL2, null},
                 {SUBJ1, PRED2, EXPL1, null},
                 {SUBJ2, PRED1, EXPL2, null},
                 {SUBJ1, PRED2, EXPL2, null},
                 {SUBJ2, PRED1, EXPL1, null},
                 {SUBJ1, PRED1, EXPL2, null},
                 {SUBJ2, PRED2, EXPL1, null},
                 {vf.createBNode(), PRED2, vf.createBNode(), null},
                 {SUBJ1, PRED1, EXPL1, CTX1},
                 {SUBJ2, PRED2, EXPL2, CTX1},
                 {SUBJ1, PRED2, EXPL1, CTX1},
                 {SUBJ2, PRED1, EXPL2, CTX1},
                 {SUBJ1, PRED2, EXPL2, CTX1},
                 {SUBJ2, PRED1, EXPL1, CTX1},
                 {SUBJ1, PRED1, EXPL2, CTX1},
                 {SUBJ2, PRED2, EXPL1, CTX1},
                 {SUBJ1, PRED1, EXPL1, CTX2},
                 {SUBJ2, PRED2, EXPL2, CTX2},
                 {SUBJ1, PRED2, EXPL1, CTX2},
                 {SUBJ2, PRED1, EXPL2, CTX2},
                 {SUBJ1, PRED2, EXPL2, CTX2},
                 {SUBJ2, PRED1, EXPL1, CTX2},
                 {SUBJ1, PRED1, EXPL2, CTX2},
                 {SUBJ2, PRED2, EXPL1, CTX2},
                 {vf.createBNode(), PRED2, RDF.NIL, CTX2},
           });
    }

    private final RDFSubject s;
    private final RDFPredicate p;
    private final RDFObject o;
    private final RDFContext c;

	public HalyardTableUtilsKeyValueTest(Resource s, IRI p, Value o, Resource c) {
        this.s = RDFSubject.create(s, valueIO);
        this.p = RDFPredicate.create(p, valueIO);
        this.o = RDFObject.create(o, valueIO);
        this.c = (c != null) ? RDFContext.create(c, valueIO) : null;
	}

	@Test
	public void testKeyValues() throws IOException {
		long ts = 0;
		Resource ctx = c != null ? c.val : null;
		Statement expected = vf.createStatement(s.val, p.val, o.val, ctx);
		List<? extends Cell> kvs = HalyardTableUtils.toKeyValues(s.val, p.val, o.val, ctx, false, ts, valueIO);
		for(Cell kv : kvs) {
			testParseStatement("spoc", expected, s, p, o, c != null ? c : null, kv, ts);
			testParseStatement("_poc", expected, null, p, o, c != null ? c : null, kv, ts);
			testParseStatement("s_oc", expected, s, null, o, c != null ? c : null, kv, ts);
			testParseStatement("sp_c", expected, s, p, null, c != null ? c : null, kv, ts);
			testParseStatement("___c", expected, null, null, null, c != null ? c : null, kv, ts);
		}
	}

	private void testParseStatement(String msg, Statement expected, RDFSubject s, RDFPredicate p, RDFObject o, RDFContext c, Cell kv, long ts) {
		Statement actual = HalyardTableUtils.parseStatement(s, p, o, c, kv, valueIO.createReader(vf, null));
		assertEquals(msg, expected, actual);
		assertEquals(ts, ((Timestamped)actual).getTimestamp());
		if(s == null) {
			assertEquals(valueIO.id(expected.getSubject()), ((Identifiable)actual.getSubject()).getId());
		}
		if(p == null) {
			assertEquals(valueIO.id(expected.getPredicate()), ((Identifiable)actual.getPredicate()).getId());
		}
		if(o == null) {
			assertEquals(valueIO.id(expected.getObject()), ((Identifiable)actual.getObject()).getId());
		}
		if (c == null && (expected.getContext() != null || actual.getContext() != null)) {
			assertEquals(valueIO.id(expected.getContext()), ((Identifiable)actual.getContext()).getId());
		}
	}
}
