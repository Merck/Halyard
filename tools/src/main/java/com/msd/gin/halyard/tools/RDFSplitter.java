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

public final class RDFSplitter implements RDFHandler, Callable<Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RDFSplitter.class);

    public static void main(String[] args) throws Exception {
		Path inputFile = Paths.get(args[0]);
		Path outputDir = Paths.get(args[1]);
		int numParts = Integer.parseInt(args[2]);
		String outExt = args.length > 3 ? args[3] : null;
		new RDFSplitter(inputFile, outputDir, numParts, outExt).call();
	}

	private static final int BUFFER_SIZE = 128*1024;
	private static final int QUEUE_CAPACITY = 1;
	private static final int MAX_BATCH_SIZE = 10000;
	private static final Object START = new Object();
	private static final Object END = new Object();

	private final ExecutorCompletionService<Long> executorService = new ExecutorCompletionService<>(Executors.newCachedThreadPool());
	private final Path inputFile;
	private final String inExt;
	private final Path outputDir;
	private final String outBaseName;
	private final String outExt;
	private final WriterTask[] tasks;
	private WriterTask bnodeTask;
	private long totalStmtReadCount;
	private long inputByteSize;
	private CountingInputStream inCounter;
	private long readStartTime;
	private long previousStatusBytesRead;

	private RDFSplitter(Path inputFile, Path outputDir, int numParts, String outExt) {
		this.inputFile = inputFile;
		this.outputDir = outputDir;
		this.tasks = new WriterTask[numParts];
		String inputFileName = inputFile.getFileName().toString();
		int dotPos = inputFileName.indexOf('.');
		this.outBaseName = inputFileName.substring(0, dotPos);
		this.inExt = inputFileName.substring(dotPos);
		this.outExt = outExt != null ? outExt : inExt;
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

	public Void call() throws Exception {
		boolean isInputGzipped = inExt.endsWith(".gz");
		RDFFormat inFormat = getParserFormatForName(inExt);
		boolean gzipOutput = outExt.endsWith(".gz");
		RDFFormat outFormat =getWriterFormatForName(outExt);

		RDFParser parser = Rio.createParser(inFormat);

		parser.getParserConfig().set(BasicParserSettings.VERIFY_DATATYPE_VALUES, false);
		parser.getParserConfig().set(BasicParserSettings.VERIFY_LANGUAGE_TAGS, false);
		parser.getParserConfig().set(BasicParserSettings.VERIFY_RELATIVE_URIS, false);
		parser.getParserConfig().set(BasicParserSettings.VERIFY_URI_SYNTAX, false);
		parser.getParserConfig().set(BasicParserSettings.FAIL_ON_UNKNOWN_DATATYPES, false);
		parser.getParserConfig().set(BasicParserSettings.FAIL_ON_UNKNOWN_LANGUAGES, false);
		parser.setRDFHandler(this);

		long totalStmtWrittenCount = 0;
		inputByteSize = Files.size(inputFile);
		inCounter = new CountingInputStream(new BufferedInputStream(Files.newInputStream(inputFile), tasks.length*BUFFER_SIZE));
		try(InputStream in = decompress(isInputGzipped, inCounter)) {
			bnodeTask = new WriterTask(outFormat, gzipOutput, outputDir.resolve(outBaseName+"_bnodes"+outExt));
			for (int i=0; i<tasks.length; i++) {
				tasks[i] = new WriterTask(outFormat, gzipOutput, outputDir.resolve(outBaseName+"_"+(i+1)+outExt));
			}

			readStartTime = System.currentTimeMillis();
			parser.parse(in);
			LOGGER.info("Finished reading {} statements in {}mins", totalStmtReadCount, TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis()-readStartTime));

			for (int i=0; i<tasks.length+1; i++) {
				try {
					totalStmtWrittenCount += executorService.take().get();
				} catch (InterruptedException e) {
					throw e;
				} catch (ExecutionException e) {
					throw (Exception) e.getCause();
				}
			}
		}
		LOGGER.info("Finished writing {} statements", totalStmtWrittenCount);

		return null;
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
		bnodeTask.startRDF();
		for (RDFHandler task : tasks) {
			task.startRDF();
		}
	}

	@Override
	public void endRDF() throws RDFHandlerException {
		logStatus();
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
		logStatus();
		totalStmtReadCount++;
		if (st.getSubject().isBNode() || st.getObject().isBNode() || (st.getContext() != null && st.getContext().isBNode())) {
			bnodeTask.handleStatement(st);
		} else {
			// group by subject for more efficient turtle encoding
			int idx = (Math.abs(st.getSubject().hashCode()) % tasks.length);
			tasks[idx].handleStatement(st);
		}
	}

	@Override
	public void handleComment(String comment) throws RDFHandlerException {
	}

	private static boolean processBatch(RDFStreamWriter writer, List<Object> batch) {
		for (Object next : batch) {
			if (next instanceof Statement) {
				Statement st = (Statement) next;
				writer.handleStatement(st);
			} else if (next instanceof Namespace) {
				Namespace ns = (Namespace) next;
				writer.handleNamespace(ns.getPrefix(), ns.getName());
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


	final class WriterTask implements RDFHandler, Callable<Long> {
		private final BlockingQueue<List<Object>> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
		private final RDFFormat format;
		private final boolean isGzipped;
		private final Path filename;
		private List<Object> batch = new ArrayList<>();

		WriterTask(RDFFormat format, boolean isGzipped, Path filename) {
			this.format = format;
			this.isGzipped = isGzipped;
			this.filename = filename;
		}

		private void addToQueue(Object o, boolean flush) throws RDFHandlerException {
			batch.add(o);
			final int batchSize = batch.size();
			boolean enqueued;
			if (batchSize >= MAX_BATCH_SIZE || flush) {
				enqueued = false;
				try {
					queue.put(batch);
					enqueued = true;
				} catch (InterruptedException e) {
					throw new RDFHandlerException(e);
				}
			} else {
				enqueued = queue.offer(batch);
			}
			if (enqueued) {
				batch = new ArrayList<>(batchSize);
			}
		}

		@Override
		public void startRDF() throws RDFHandlerException {
			addToQueue(START, false);
			executorService.submit(this);
		}

		@Override
		public void endRDF() throws RDFHandlerException {
			addToQueue(END, true);
		}

		@Override
		public void handleNamespace(String prefix, String uri) throws RDFHandlerException {
			addToQueue(new SimpleNamespace(prefix, uri), false);
		}

		@Override
		public void handleStatement(Statement st) throws RDFHandlerException {
			addToQueue(st, false);
		}

		@Override
		public void handleComment(String comment) throws RDFHandlerException {
		}

		public Long call() throws IOException, InterruptedException {
			LOGGER.info("Started writing to {}", filename);
			long stmtCount;
			try (RDFStreamWriter writer = new RDFStreamWriter(format, compress(isGzipped, Files.newOutputStream(filename)))) {
				boolean finished = false;
				while (!finished) {
					List<List<Object>> nextBatches = new ArrayList<>();
					queue.drainTo(nextBatches);
					for (List<Object> nextBatch : nextBatches) {
						if(!processBatch(writer, nextBatch)) {
							finished = true;
							break;
						}
					}
				}
				stmtCount = writer.getStatementsWritten();
			}
			LOGGER.info("Finished writing {} statements to {}", stmtCount, filename);
			return stmtCount;
		}
	}


	final static class RDFStreamWriter implements RDFHandler, Closeable {
		private final OutputStream out;
		private final RDFWriter writer;
		private long stmtCount;

		RDFStreamWriter(RDFFormat format, OutputStream out) {
			this.out = out;
			this.writer = Rio.createWriter(format, out);
		}

		public long getStatementsWritten() {
			return stmtCount;
		}

		public void startRDF() throws RDFHandlerException {
			writer.startRDF();
		}

		public void endRDF() throws RDFHandlerException {
			writer.endRDF();
		}

		public void handleNamespace(String prefix, String uri) throws RDFHandlerException {
			writer.handleNamespace(prefix, uri);
		}

		public void handleStatement(Statement st) throws RDFHandlerException {
			writer.handleStatement(st);
			stmtCount++;
		}

		public void handleComment(String comment) throws RDFHandlerException {
			writer.handleComment(comment);
		}

		public void close() throws IOException {
			out.close();
		}
	}
}
