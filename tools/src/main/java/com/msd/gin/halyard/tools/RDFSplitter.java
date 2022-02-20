package com.msd.gin.halyard.tools;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.zip.*;

import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.rio.*;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;

public final class RDFSplitter implements RDFHandler, Callable<Void> {
	public static void main(String[] args) throws Exception {
		Path inputFile = Paths.get(args[0]);
		Path outputDir = Paths.get(args[1]);
		int numParts = Integer.parseInt(args[2]);
		new RDFSplitter(inputFile, outputDir, numParts).call();
	}

	private static final Object END = new Object();

	private final ExecutorCompletionService<Void> executorService = new ExecutorCompletionService<>(Executors.newCachedThreadPool());
	private final Path inputFile;
	private final Path outputDir;
	private final int numParts;
	private WriterTask bnodeTask;
	private WriterTask[] tasks;

	private RDFSplitter(Path inputFile, Path outputDir, int numParts) {
		this.inputFile = inputFile;
		this.outputDir = outputDir;
		this.numParts = numParts;
	}

	private static InputStream decompress(boolean isGzipped, InputStream in) throws IOException {
		return isGzipped ? new GZIPInputStream(in) : in;
	}

	private static OutputStream decompress(boolean isGzipped, OutputStream out) throws IOException {
		return isGzipped ? new GZIPOutputStream(out) : out;
	}

	public Void call() throws Exception {
		String inputFileName = inputFile.getFileName().toString();
		boolean isGzipped = inputFileName.endsWith(".gz");

		RDFFormat rdfFormat = Rio.getParserFormatForFileName(inputFileName).orElseThrow(() -> new UnsupportedRDFormatException(inputFileName));
		RDFParser parser = Rio.createParser(rdfFormat);

		parser.getParserConfig().set(BasicParserSettings.VERIFY_DATATYPE_VALUES, false);
		parser.getParserConfig().set(BasicParserSettings.VERIFY_LANGUAGE_TAGS, false);
		parser.getParserConfig().set(BasicParserSettings.VERIFY_RELATIVE_URIS, false);
		parser.getParserConfig().set(BasicParserSettings.VERIFY_URI_SYNTAX, false);
		parser.getParserConfig().set(BasicParserSettings.FAIL_ON_UNKNOWN_DATATYPES, false);
		parser.getParserConfig().set(BasicParserSettings.FAIL_ON_UNKNOWN_LANGUAGES, false);
		parser.setRDFHandler(this);

		int dotPos = inputFileName.indexOf('.');
		String baseName = inputFileName.substring(0, dotPos);
		String ext = inputFileName.substring(dotPos+1);

		OutputStream bnodeOut = decompress(isGzipped, new BufferedOutputStream(Files.newOutputStream(outputDir.resolve(baseName+"_bnodes"+"."+ext))));
		OutputStream[] outs = new OutputStream[numParts];
		for (int i=0; i<outs.length; i++) {
			outs[i] = decompress(isGzipped, new BufferedOutputStream(Files.newOutputStream(outputDir.resolve(baseName+"_"+(i+1)+"."+ext))));
		}

		try(InputStream in = decompress(isGzipped, new BufferedInputStream(Files.newInputStream(inputFile)))) {
			bnodeTask = new WriterTask(rdfFormat, bnodeOut);
			tasks = new WriterTask[numParts];
			for (int i=0; i<tasks.length; i++) {
				tasks[i] = new WriterTask(rdfFormat, outs[i]);
			}
			parser.parse(in);
		} finally {
			bnodeOut.close();
			for (OutputStream out : outs) {
				out.close();
			}
		}

		for (int i=0; i<tasks.length+1; i++) {
			try {
				executorService.take().get();
			} catch (InterruptedException e) {
				throw e;
			} catch (ExecutionException e) {
				throw (Exception) e.getCause();
			}
		}
		return null;
	}

	@Override
	public void startRDF() throws RDFHandlerException {
		bnodeTask.startRDF();
		for (RDFHandler task : tasks) {
			task.startRDF();
		}
	}

	@Override
	public void endRDF() throws RDFHandlerException {
		bnodeTask.endRDF();
		for (RDFHandler task : tasks) {
			task.endRDF();
		}
	}

	@Override
	public void handleNamespace(String prefix, String uri) throws RDFHandlerException {
		bnodeTask.handleNamespace(prefix, uri);
		for (RDFHandler task : tasks) {
			task.handleNamespace(prefix, uri);
		}
	}

	@Override
	public void handleStatement(Statement st) throws RDFHandlerException {
		if (st.getSubject().isBNode() || st.getObject().isBNode() || (st.getContext() != null && st.getContext().isBNode())) {
			bnodeTask.handleStatement(st);
		} else {
			int idx = (Math.abs(st.hashCode()) % tasks.length);
			tasks[idx].handleStatement(st);
		}
	}

	@Override
	public void handleComment(String comment) throws RDFHandlerException {
	}


	final class WriterTask implements RDFHandler, Callable<Void> {
		private final BlockingQueue<Object> queue;
		private final RDFHandler handler;

		WriterTask(RDFFormat format, OutputStream out) {
			this(new ArrayBlockingQueue<>(100), Rio.createWriter(format, out));
		}

		WriterTask(BlockingQueue<Object> queue, RDFHandler handler) {
			this.queue = queue;
			this.handler = handler;
		}

		private void addToQueue(Object o) throws RDFHandlerException {
			try {
				queue.put(o);
			} catch (InterruptedException e) {
				throw new RDFHandlerException(e);
			}
		}

		@Override
		public void startRDF() throws RDFHandlerException {
			executorService.submit(this);
		}

		@Override
		public void endRDF() throws RDFHandlerException {
			addToQueue(END);
		}

		@Override
		public void handleNamespace(String prefix, String uri) throws RDFHandlerException {
			addToQueue(new SimpleNamespace(prefix, uri));
		}

		@Override
		public void handleStatement(Statement st) throws RDFHandlerException {
			addToQueue(st);
		}

		@Override
		public void handleComment(String comment) throws RDFHandlerException {
		}

		public Void call() throws InterruptedException {
			Object next;
			handler.startRDF();
			while ((next = queue.take()) != END) {
				if (next instanceof Namespace) {
					Namespace ns = (Namespace) next;
					handler.handleNamespace(ns.getPrefix(), ns.getName());
				} else if (next instanceof Statement) {
					Statement st = (Statement) next;
					handler.handleStatement(st);
				} else {
					throw new IllegalStateException("Unexpected object: "+next);
				}
			}
			handler.endRDF();
			return null;
		}
	}
}
