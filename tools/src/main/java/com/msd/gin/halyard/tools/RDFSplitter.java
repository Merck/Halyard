package com.msd.gin.halyard.tools;

import com.google.common.io.CountingInputStream;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.zip.*;

import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.rio.*;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RDFSplitter implements RDFHandler, Callable<Long> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RDFSplitter.class);

    public static void main(String[] args) throws Exception {
		Path inputFile = Paths.get(args[0]);
		Path outputDir = Paths.get(args[1]);
		int numParts = Integer.parseInt(args[2]);
		String outExt = args.length > 3 ? args[3] : null;
		int numThreads = args.length > 4 ? Integer.parseInt(args[4]) : 1;

		String inputFileName = inputFile.getFileName().toString();
		int dotPos = inputFileName.indexOf('.');
		String outBaseName = inputFileName.substring(0, dotPos);
		String inExt = inputFileName.substring(dotPos);
		outExt = outExt != null ? outExt : inExt;

		boolean isInputGzipped = inExt.endsWith(".gz");
		RDFFormat inFormat = getParserFormatForName(inExt);
		boolean gzipOutput = outExt.endsWith(".gz");
		RDFFormat outFormat = getWriterFormatForName(outExt);

		RDFFile[] files = new RDFFile[numParts];
		for (int i=0; i<files.length; i++) {
			files[i] = new RDFFile(outFormat, gzipOutput, outputDir.resolve(outBaseName+"_"+(i+1)+outExt));
		}
		RDFFile bnodeFile;
		if (numParts > 1 || numThreads > 1) {
			bnodeFile = new RDFFile(outFormat, gzipOutput, outputDir.resolve(outBaseName+"_bnodes"+outExt));
		} else {
			bnodeFile = null;
		}

		new RDFSplitter(inputFile, inFormat, isInputGzipped, files, bnodeFile, numThreads).call();
	}

	private static InputStream decompress(boolean isGzipped, InputStream in) throws IOException {
		return isGzipped ? new GZIPInputStream(in) : in;
	}

	private static OutputStream compress(boolean isGzipped, OutputStream out) throws IOException {
		return isGzipped ? new GZIPOutputStream(out, BUFFER_SIZE) : out;
	}

	private static RDFFormat getParserFormatForName(String fileName) {
		return Rio.getParserFormatForFileName(fileName).orElseThrow(Rio.unsupportedFormat(fileName));
	}

	private static RDFFormat getWriterFormatForName(String fileName) {
		return Rio.getWriterFormatForFileName(fileName).orElseThrow(Rio.unsupportedFormat(fileName));
	}

	private static final int BUFFER_SIZE = 128*1024;
	private static final int QUEUE_CAPACITY = 1;
	private static final int MAX_BATCH_SIZE = 10000;
	private static final Object START = new Object();
	private static final Object END = new Object();

	private final ExecutorCompletionService<Long> completionService = new ExecutorCompletionService<>(Executors.newCachedThreadPool());
	private final Path inputFile;
	private final RDFFormat format;
	private final boolean isGzipped;
	private final RDFHandler[] outHandlers;
	private final RDFHandler bnodeOutHandler;
	private WriterTask.StatementBatcher[] batchers;
	private WriterTask.StatementBatcher bnodeBatcher;
	private final WriterTask[] tasks;
	private long totalStmtReadCount;
	private long inputByteSize;
	private CountingInputStream inCounter;
	private long readStartTime;
	private long previousStatusBytesRead;

	RDFSplitter(Path inputFile, RDFFormat format, boolean gzipped, RDFHandler[] outHandlers, RDFHandler bnodeOutHandler, int numThreads) {
		this.inputFile = inputFile;
		this.format = format;
		this.isGzipped = gzipped;
		this.outHandlers = outHandlers;
		this.bnodeOutHandler = bnodeOutHandler;
		this.tasks = new WriterTask[numThreads];
		for (int i=0; i<numThreads; i++) {
			this.tasks[i] = new WriterTask();
		}
	}

	public Long call() throws Exception {
		RDFParser parser = Rio.createParser(format);
		parser.getParserConfig().set(BasicParserSettings.VERIFY_DATATYPE_VALUES, false);
		parser.getParserConfig().set(BasicParserSettings.VERIFY_LANGUAGE_TAGS, false);
		parser.getParserConfig().set(BasicParserSettings.VERIFY_RELATIVE_URIS, false);
		parser.getParserConfig().set(BasicParserSettings.VERIFY_URI_SYNTAX, false);
		parser.getParserConfig().set(BasicParserSettings.FAIL_ON_UNKNOWN_DATATYPES, false);
		parser.getParserConfig().set(BasicParserSettings.FAIL_ON_UNKNOWN_LANGUAGES, false);
		parser.setRDFHandler(this);

		inputByteSize = Files.size(inputFile);
		inCounter = new CountingInputStream(new BufferedInputStream(Files.newInputStream(inputFile), tasks.length*BUFFER_SIZE));
		long totalStmts;
		try(InputStream in = decompress(isGzipped, inCounter)) {
			totalStmts = split(parser, in, outHandlers, bnodeOutHandler);
		}
		LOGGER.info("Finished writing a total of {} statements", totalStmts);

		return totalStmts;
	}

	long split(RDFParser parser, InputStream in, RDFHandler[] handlers, RDFHandler bnodeHandler) throws Exception {
		batchers = new WriterTask.StatementBatcher[handlers.length];
		for (int i=0; i<handlers.length; i++) {
			batchers[i] = tasks[i%tasks.length].createBatcher(handlers[i]);
		}
		if (bnodeHandler != null) {
			bnodeBatcher = tasks[tasks.length-1].createBatcher(bnodeHandler);
		}

		for (WriterTask task : tasks) {
			completionService.submit(task);
		}

		readStartTime = System.currentTimeMillis();
		parser.parse(in);
		LOGGER.info("Finished reading a total of {} statements in {}mins", totalStmtReadCount, TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis()-readStartTime));

		long stmtCount = 0L;
		for (int i=0; i<tasks.length; i++) {
			try {
				stmtCount += completionService.take().get();
			} catch (InterruptedException e) {
				throw e;
			} catch (ExecutionException e) {
				throw (Exception) e.getCause();
			}
		}
		return stmtCount;
	}

	private void logStatus() {
		long currentBytesRead = inCounter.getCount();
		if (currentBytesRead - previousStatusBytesRead > 100*1024*1024) {
			LOGGER.info(String.format("%.2f%% read in %dmins", currentBytesRead*100.0/inputByteSize, TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis()-readStartTime)));
			previousStatusBytesRead = currentBytesRead;
		}
	}

	@Override
	public void startRDF() throws RDFHandlerException {
		logStatus();
		totalStmtReadCount = 0;
		for (RDFHandler batcher : batchers) {
			batcher.startRDF();
		}
		if (bnodeBatcher != null) {
			bnodeBatcher.startRDF();
		}
	}

	@Override
	public void endRDF() throws RDFHandlerException {
		logStatus();
		for (RDFHandler batcher : batchers) {
			batcher.endRDF();
		}
		if (bnodeBatcher != null) {
			bnodeBatcher.endRDF();
		}
	}

	@Override
	public void handleNamespace(String prefix, String uri) throws RDFHandlerException {
		for (RDFHandler batcher : batchers) {
			batcher.handleNamespace(prefix, uri);
		}
		if (bnodeBatcher != null) {
			bnodeBatcher.handleNamespace(prefix, uri);
		}
	}

	@Override
	public void handleStatement(Statement st) throws RDFHandlerException {
		logStatus();
		totalStmtReadCount++;
		if (tasks.length == 1) {
			int idx;
			if (batchers.length > 1) {
				long currentBytesRead = inCounter.getCount() - 1;
				idx = (int) (currentBytesRead*batchers.length/inputByteSize);
			} else {
				idx = 0;
			}
			batchers[idx].handleStatement(st);
		} else {
			if (st.getSubject().isBNode() || st.getObject().isBNode() || (st.getContext() != null && st.getContext().isBNode())) {
				bnodeBatcher.handleStatement(st);
			} else {
				// group by subject for more efficient turtle encoding
				int idx = (Math.abs(st.getSubject().hashCode()) % batchers.length);
				batchers[idx].handleStatement(st);
			}
		}
	}

	@Override
	public void handleComment(String comment) throws RDFHandlerException {
	}

	private static boolean processBatch(RDFHandler writer, List<Object> batch) {
		for (Object next : batch) {
			if (next instanceof Statement) {
				Statement st = (Statement) next;
				writer.handleStatement(st);
			} else if (next instanceof Namespace) {
				Namespace ns = (Namespace) next;
				writer.handleNamespace(ns.getPrefix(), ns.getName());
			} else if (next instanceof String) {
				String comment = (String) next;
				writer.handleComment(comment);
			} else if (next == END) {
				writer.endRDF();
				return false;
			} else if (next == START) {
				writer.startRDF();
			} else {
				throw new AssertionError("Unexpected object: "+next);
			}
		}
		return true;
	}

	final static class WriterTask implements Callable<Long> {
		private final BlockingQueue<StatementBatch> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
		private final List<StatementBatcher> batchers = new ArrayList<>();
		private int remainingFiles;

		public StatementBatcher createBatcher(RDFHandler handler) {
			StatementBatcher batcher = new StatementBatcher(handler);
			batchers.add(batcher);
			remainingFiles++;
			return batcher;
		}

		private boolean addToQueue(RDFHandler handler, List<Object> batch, boolean flush) throws RDFHandlerException {
			boolean enqueued;
			final int batchSize = batch.size();
			if (batchSize >= MAX_BATCH_SIZE || flush) {
				enqueued = false;
				try {
					queue.put(new StatementBatch(handler, batch));
					enqueued = true;
				} catch (InterruptedException e) {
					throw new RDFHandlerException(e);
				}
			} else {
				enqueued = queue.offer(new StatementBatch(handler, batch));
			}
			return enqueued;
		}

		public Long call() throws IOException, InterruptedException {
			while (remainingFiles > 0) {
				List<StatementBatch> nextBatches = new ArrayList<>();
				queue.drainTo(nextBatches);
				for (StatementBatch nextBatch : nextBatches) {
					if(!processBatch(nextBatch.handler, nextBatch.batch)) {
						remainingFiles--;
						break;
					}
				}
			}

			long stmtCount = 0L;
			for (StatementBatcher batcher : batchers) {
				stmtCount += batcher.getStatementCount();
			}
			return stmtCount;
		}

		final class StatementBatcher implements RDFHandler {
			private final RDFHandler handler;
			private List<Object> batch = new ArrayList<>();
			private long stmtCount;

			StatementBatcher(RDFHandler handler) {
				this.handler = handler;
			}

			public void add(Object o, boolean flush) throws RDFHandlerException {
				batch.add(o);
				final int batchSize = batch.size();
				if (addToQueue(handler, batch, flush)) {
					batch = new ArrayList<>(batchSize);
				}
			}

			public long getStatementCount() {
				return stmtCount;
			}

			@Override
			public void startRDF() throws RDFHandlerException {
				add(START, false);
			}

			@Override
			public void endRDF() throws RDFHandlerException {
				add(END, true);
				LOGGER.info("Consumed {} statements", stmtCount);
			}

			@Override
			public void handleNamespace(String prefix, String uri) throws RDFHandlerException {
				add(new SimpleNamespace(prefix, uri), false);
			}

			@Override
			public void handleStatement(Statement st) throws RDFHandlerException {
				add(st, false);
				stmtCount++;
			}

			@Override
			public void handleComment(String comment) throws RDFHandlerException {
				add(comment, false);
			}
		}

		private final static class StatementBatch {
			private final RDFHandler handler;
			private final List<Object> batch;

			private StatementBatch(RDFHandler handler, List<Object> batch) {
				this.handler = handler;
				this.batch = batch;
			}
		}
	}

	final static class RDFFile implements RDFHandler {
		private final RDFFormat format;
		private final boolean isGzipped;
		private final Path filename;
		private OutputStream out;
		private RDFWriter writer;

		RDFFile(RDFFormat format, boolean isGzipped, Path filename) {
			this.format = format;
			this.isGzipped = isGzipped;
			this.filename = filename;
		}

		@Override
		public void startRDF() throws RDFHandlerException {
			try {
				out = compress(isGzipped, Files.newOutputStream(filename));
				writer = Rio.createWriter(format, out);
				LOGGER.info("Started writing to {}", filename);
				writer.startRDF();
			} catch (IOException e) {
				throw new RDFHandlerException(e);
			}
		}

		@Override
		public void endRDF() throws RDFHandlerException {
			writer.endRDF();
			LOGGER.info("Finished writing to {}", filename);
			try {
				out.close();
			} catch (IOException e) {
				throw new RDFHandlerException(e);
			}
		}

		@Override
		public void handleNamespace(String prefix, String uri) throws RDFHandlerException {
			writer.handleNamespace(prefix, uri);
		}

		@Override
		public void handleStatement(Statement st) throws RDFHandlerException {
			writer.handleStatement(st);
		}

		@Override
		public void handleComment(String comment) throws RDFHandlerException {
			writer.handleComment(comment);
		}
	}

}
