package com.msd.gin.halyard.tools;

import com.msd.gin.halyard.common.Config;
import com.msd.gin.halyard.common.HBaseServerTestInstance;
import com.msd.gin.halyard.sail.HBaseSail;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.sail.SailConnection;
import org.junit.Test;

import static org.junit.Assert.*;

public class HalyardKeysTest extends AbstractHalyardToolTest {

	@Override
	protected AbstractHalyardTool createTool() {
		return new HalyardKeys();
	}

    @Test
    public void testKeyStats() throws Exception {
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
        final HBaseSail sail = new HBaseSail(conf, "keyStatsTable", true, -1, true, 0, null, null);
        sail.initialize();
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

        File root = File.createTempFile("test_stats", "");
        root.delete();
        root.mkdirs();

        assertEquals(0, run(new String[]{"-s", "statsTable", "-t", root.toURI().toURL().toString() + "key-stats.csv", "-d", "0"}));

        Path stats = root.toPath().resolve("key-stats.csv");
        assertTrue(Files.isReadable(stats));
        List<String> lines = Files.readAllLines(stats, StandardCharsets.US_ASCII);
        List<String> expectedLines = Arrays.asList(
        	"SPO, 1, 1, 1, 1:2001",
        	"POS, 1, 1, 1, 1:2001",
        	"OSP, 1, 1, 1, 1:2001",
        	"CSPO, 1, 1, 1, 1:1801",
        	"CPOS, 1, 1, 1, 1:1801",
        	"COSP, 1, 1, 1, 1:1801"
        );
        assertEquals(expectedLines.size(), lines.size());
        for (int i=0; i<expectedLines.size(); i++) {
        	assertEquals(expectedLines.get(i), lines.get(i));
        }
    }
}
