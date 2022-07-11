package com.msd.gin.halyard.tools;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class RDFSplitterTest {
	private static Path tmpFile;

	@BeforeClass
	public static void createData() throws IOException {
		tmpFile = Files.createTempFile("rdfsplittertest", ".trig");
		tmpFile.toFile().deleteOnExit();
		try (InputStream in = RDFSplitterTest.class.getResourceAsStream("testData2.trig")) {
			Files.copy(in, tmpFile, StandardCopyOption.REPLACE_EXISTING);
		}
	}

	@Test
	public void splitSequential() throws Exception {
		StatementCollector[] outs = createCollectors(3);
		RDFSplitter splitter = new RDFSplitter(tmpFile, RDFFormat.TRIG, null, outs, null, 1);
		long stmtCount = splitter.call();
		List<Statement> actualStmts = concat(outs);
		assertEquals(stmtCount, actualStmts.size());
		Collection<Statement> expectedStmts = getAllStatements();
		Set<Statement> expectedStmtSet = new HashSet<>(expectedStmts);
		Set<Statement> actualStmtSet = new HashSet<>(actualStmts);
		assertEquals("Missing: "+Sets.difference(expectedStmtSet, actualStmtSet), expectedStmtSet, actualStmtSet);
	}

	@Test
	public void splitParallel() throws Exception {
		StatementCollector[] outs = createCollectors(3);
		StatementCollector bnodeOut = new StatementCollector();
		RDFSplitter splitter = new RDFSplitter(tmpFile, RDFFormat.TRIG, null, outs, bnodeOut, 2);
		long stmtCount = splitter.call();
		List<Statement> actualStmts = concat(outs);
		actualStmts.addAll(bnodeOut.getStatements());
		assertEquals(stmtCount, actualStmts.size());
		Collection<Statement> expectedStmts = getAllStatements();
		Set<Statement> expectedStmtSet = new HashSet<>(expectedStmts);
		Set<Statement> actualStmtSet = new HashSet<>(actualStmts);
		assertEquals("Missing: "+Sets.difference(expectedStmtSet, actualStmtSet), expectedStmtSet, actualStmtSet);
	}

	private Collection<Statement> getAllStatements() throws IOException {
		StatementCollector out = new StatementCollector();
		RDFParser parser = Rio.createParser(RDFFormat.TRIG);
		parser.getParserConfig().set(BasicParserSettings.PRESERVE_BNODE_IDS, true);
		parser.setRDFHandler(out);
		try (Reader reader = Files.newBufferedReader(tmpFile)) {
			parser.parse(reader);
		}
		return out.getStatements();
	}

	private StatementCollector[] createCollectors(int n) {
		StatementCollector[] collectors = new StatementCollector[n];
		for (int i=0; i<n; i++) {
			collectors[i] = new StatementCollector();
		}
		return collectors;
	}

	private List<Statement> concat(StatementCollector... collectors) {
		List<Statement> stmts = new ArrayList<>();
		for (StatementCollector collector : collectors) {
			stmts.addAll(collector.getStatements());
		}
		return stmts;
	}
}
