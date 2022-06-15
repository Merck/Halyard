package com.msd.gin.halyard.tools;

import com.msd.gin.halyard.common.HBaseServerTestInstance;
import com.msd.gin.halyard.sail.HBaseSail;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class HalyardSingleBulkUpdateTest extends AbstractHalyardToolTest {
    private static final String TABLE = "singlebulkupdatetesttable";

	@Override
	protected AbstractHalyardTool createTool() {
		return new HalyardSingleBulkUpdate();
	}

    @Test
    public void testSingleBulkUpdate() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        Configuration conf = HBaseServerTestInstance.getInstanceConfig();
        HBaseSail sail = new HBaseSail(conf, TABLE, true, -1, true, 0, null, null);
        sail.init();
		try (SailConnection conn = sail.getConnection()) {
			for (int i = 0; i < 5; i++) {
				for (int j = 0; j < 5; j++) {
					conn.addStatement(vf.createIRI("http://whatever/subj" + i), vf.createIRI("http://whatever/pred"), vf.createIRI("http://whatever/obj" + j));
				}
			}
		}
        sail.shutDown();

        String query = "PREFIX halyard: <http://merck.github.io/Halyard/ns#>\n"
                + "delete {?s <http://whatever/pred> ?o}\n"
                + "insert {?o <http://whatever/reverse> ?s}\n"
                + "where {?s <http://whatever/pred> ?o . FILTER (halyard:forkAndFilterBy(2, ?s, ?o))};"
                + "insert {?s <http://whatever/another> ?o}\n"
                + "where {?s <http://whatever/reverse> ?o . FILTER (halyard:forkAndFilterBy(3, ?s, ?o))}";
        File htableDir = getTempHTableDir("test_htable");

        assertEquals(0, run(new String[]{ "-q", query, "-w", htableDir.toURI().toURL().toString(), "-s", TABLE}));

        sail = new HBaseSail(conf, TABLE, false, 0, true, 0, null, null);
        sail.init();
        try {
			try (SailConnection conn = sail.getConnection()) {
				int count;
				try (CloseableIteration<? extends Statement, SailException> iter = conn.getStatements(null, null, null, true)) {
					count = 0;
					while (iter.hasNext()) {
						iter.next();
						count++;
					}
				}
				Assert.assertEquals(50, count);
				try (CloseableIteration<? extends Statement, SailException> iter = conn.getStatements(null, vf.createIRI("http://whatever/pred"), null, true)) {
					Assert.assertFalse(iter.hasNext());
				}
			}
        } finally {
            sail.shutDown();
        }
    }
}
