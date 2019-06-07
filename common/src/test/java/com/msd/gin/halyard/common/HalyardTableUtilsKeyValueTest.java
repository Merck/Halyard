package com.msd.gin.halyard.common;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.hbase.KeyValue;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class HalyardTableUtilsKeyValueTest {
	private static final ValueFactory vf = SimpleValueFactory.getInstance();

    private static final String SUBJ1 = "http://whatever/subj1";
    private static final String SUBJ2 = RDF.NIL.stringValue();
    private static final String PRED1 = RDF.TYPE.stringValue();
    private static final String PRED2 = "http://whatever/pred";
    private static final String EXPL1 = "whatever explicit value1";
    private static final String EXPL2 = "whatever explicit value2";
    private static final String CTX1 = RDF.STATEMENT.stringValue();
    private static final String CTX2 = "http://whatever/ctx2";

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
           });
    }

    private final RDFSubject s;
    private final RDFPredicate p;
    private final RDFObject o;
    private final RDFContext c;

	public HalyardTableUtilsKeyValueTest(String s, String p, String o, String c) {
        this.s = RDFSubject.create(vf.createIRI(s));
        this.p = RDFPredicate.create(vf.createIRI(p));
        this.o = RDFObject.create(vf.createLiteral(o));
        this.c = (c != null) ? RDFContext.create(vf.createIRI(c)) : null;
	}

	@Test
	public void testKeyValues() {
		Resource ctx = c != null ? c.val : null;
		Statement expected = vf.createStatement(s.val, p.val, o.val, ctx);
		KeyValue[] kvs = HalyardTableUtils.toKeyValues(s.val, p.val, o.val, ctx, false, 0);
		for(KeyValue kv : kvs) {
			Statement stmt = HalyardTableUtils.parseStatement(s, p, o, c != null ? c : null, kv, vf);
			assertEquals("spoc", expected, stmt);
			stmt = HalyardTableUtils.parseStatement(null, p, o, c != null ? c : null, kv, vf);
			assertEquals("_poc", expected, stmt);
			stmt = HalyardTableUtils.parseStatement(s, null, o, c != null ? c : null, kv, vf);
			assertEquals("s_oc", expected, stmt);
			stmt = HalyardTableUtils.parseStatement(s, p, null, c != null ? c : null, kv, vf);
			assertEquals("sp_c", expected, stmt);
			stmt = HalyardTableUtils.parseStatement(null, null, null, c != null ? c : null, kv, vf);
			assertEquals("___c", expected, stmt);
		}
	}
}
