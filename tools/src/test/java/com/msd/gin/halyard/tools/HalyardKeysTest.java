package com.msd.gin.halyard.tools;

import com.msd.gin.halyard.common.Config;
import com.msd.gin.halyard.common.HBaseServerTestInstance;
import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.sail.HBaseSail;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.sail.SailConnection;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class HalyardKeysTest extends AbstractHalyardToolTest {
	private static final String TABLE_NAME = "keyStatsTable";

	@Override
	protected AbstractHalyardTool createTool() {
		return new HalyardKeys();
	}

	@BeforeClass
	public static void createData() throws Exception {
    	Configuration conf = HBaseServerTestInstance.getInstanceConfig();
    	conf.setInt(Config.ID_SIZE, 4);
    	conf.setInt(Config.ID_TYPE_INDEX, 0);
    	conf.setInt(Config.KEY_SIZE_SUBJECT, 1);
    	conf.setInt(Config.END_KEY_SIZE_SUBJECT, 1);
    	conf.setInt(Config.KEY_SIZE_PREDICATE, 1);
    	conf.setInt(Config.END_KEY_SIZE_PREDICATE, 1);
    	conf.setInt(Config.KEY_SIZE_OBJECT, 1);
    	conf.setInt(Config.END_KEY_SIZE_OBJECT, 1);
    	conf.setInt(Config.KEY_SIZE_CONTEXT, 1);
        final HBaseSail sail = new HBaseSail(conf, TABLE_NAME, true, -1, true, 0, null, null);
        sail.init();
		try (SailConnection conn = sail.getConnection()) {
			try (InputStream ref = HalyardKeysTest.class.getResourceAsStream("testData.trig")) {
				RDFParser p = Rio.createParser(RDFFormat.TRIG);
				p.setPreserveBNodeIDs(true);
				p.setRDFHandler(new AbstractRDFHandler() {
					@Override
					public void handleStatement(Statement st) throws RDFHandlerException {
						conn.addStatement(st.getSubject(), st.getPredicate(), st.getObject(), st.getContext());
					}
				}).parse(ref, "");
			}
		}
	}

	@Test
    public void testKeyStats() throws Exception {
        File root = createTempDir("test_key_stats");

        assertEquals(0, run(new String[]{"-s", TABLE_NAME, "-t", root.toURI().toURL().toString() + "key-stats.csv", "-d", "0"}));

        Path stats = root.toPath().resolve("key-stats.csv");
        assertStatsFile(stats);
    }

	@Test
    public void testKeyStats_Snapshot() throws Exception {
    	Configuration conf = HBaseServerTestInstance.getInstanceConfig();
    	String snapshot = TABLE_NAME + "Snapshot";
    	try (Connection conn = HalyardTableUtils.getConnection(conf)) {
        	try (Admin admin = conn.getAdmin()) {
        		admin.snapshot(snapshot, TableName.valueOf(TABLE_NAME));
        	}
    	}

    	File root = createTempDir("test_key_stats_snapshot");
        File restoredSnapshot = getTempSnapshotDir("restored_snapshot");
        assertEquals(0, run(new String[]{"-s", snapshot, "-u", restoredSnapshot.toURI().toURL().toString(), "-t", root.toURI().toURL().toString() + "key-stats.csv", "-d", "0"}));

        Path stats = root.toPath().resolve("key-stats.csv");
        assertStatsFile(stats);
    }

    private void assertStatsFile(Path stats) throws IOException {
        assertTrue(Files.isReadable(stats));
        List<String> lines = Files.readAllLines(stats, StandardCharsets.US_ASCII);
        List<String> expectedLines = Arrays.asList(
        	"Index, Keys, Cols, Min cols/key, Max cols/key, Mean cols/key, Col freq",
        	"SPO, 1862, 2001, 1, 4, 1, 1:1737|2:113|3:10|4:2",
        	"POS, 1862, 2001, 1, 4, 1, 1:1737|2:113|3:10|4:2",
        	"OSP, 1862, 2001, 1, 4, 1, 1:1737|2:113|3:10|4:2",
        	"CSPO, 1718, 1801, 1, 3, 1, 1:1642|2:69|3:7",
        	"CPOS, 1718, 1801, 1, 3, 1, 1:1642|2:69|3:7",
        	"COSP, 1718, 1801, 1, 3, 1, 1:1642|2:69|3:7"
        );
        assertEquals(expectedLines.size(), lines.size());
        for (int i=0; i<expectedLines.size(); i++) {
        	assertEquals(expectedLines.get(i), lines.get(i));
        }
    }
}
