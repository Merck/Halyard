package com.msd.gin.halyard.tools;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class RDFSplitterTest {
	private static Path tmpFile;

	@BeforeClass
	public static void download() throws IOException {
		URL url = new URL("https://schema.org/version/latest/schemaorg-current-http.ttl");
		URLConnection conn = url.openConnection();
		conn.setRequestProperty("Accept-Encoding", "gzip");
		conn.connect();
		String encoding = conn.getContentEncoding();
		boolean isGzipped = (encoding == "gzip");
		tmpFile = Files.createTempFile("rdfsplittertest", isGzipped ? ".ttl.gz" : ".ttl");
		tmpFile.toFile().deleteOnExit();
		try (InputStream in = conn.getInputStream()) {
			Files.copy(isGzipped ? new GZIPInputStream(in) : in, tmpFile, StandardCopyOption.REPLACE_EXISTING);
		}
	}

	@Test
	public void splitSequential() throws Exception {
		StatementCollector[] outs = createCollectors(3);
		RDFSplitter splitter = new RDFSplitter(tmpFile, RDFFormat.TURTLE, false, outs, null, 1);
		long stmtCount = splitter.call();
		List<Statement> actualStmts = concat(outs);
		assertEquals(stmtCount, actualStmts.size());
		Collection<Statement> expectedStmts = getAllStatements();
		assertEquals(expectedStmts, actualStmts);
	}

	@Test
	public void splitParallel() throws Exception {
		StatementCollector[] outs = createCollectors(3);
		RDFSplitter splitter = new RDFSplitter(tmpFile, RDFFormat.TURTLE, false, outs, null, 2);
		long stmtCount = splitter.call();
		List<Statement> actualStmts = concat(outs);
		assertEquals(stmtCount, actualStmts.size());
		Collection<Statement> expectedStmts = getAllStatements();
		assertNotEquals(expectedStmts, actualStmts);
		assertEquals(new HashSet<>(expectedStmts), new HashSet<>(actualStmts));
	}

	private Collection<Statement> getAllStatements() throws IOException {
		StatementCollector out = new StatementCollector();
		RDFParser parser = Rio.createParser(RDFFormat.TURTLE);
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
