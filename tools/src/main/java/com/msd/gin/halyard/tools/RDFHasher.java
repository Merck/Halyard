package com.msd.gin.halyard.tools;

import com.msd.gin.halyard.common.Hashes;
import com.msd.gin.halyard.common.IdentifiableValueIO;
import com.msd.gin.halyard.common.Identifier;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RDFHasher {
    private static final Logger LOGGER = LoggerFactory.getLogger(RDFHasher.class);
    private static final IdentifiableValueIO valueIO = IdentifiableValueIO.create();

	public static void main(String[] args) throws Exception {
		Path inputFile = Paths.get(args[0]);
		int numThreads = args.length > 1 ? Integer.parseInt(args[1]) : 1;
		String inputFileName = inputFile.getFileName().toString();
		int dotPos = inputFileName.indexOf('.');
		String outBaseName = inputFileName.substring(0, dotPos);
		String inExt = inputFileName.substring(dotPos);
		String inCompression = RDFSplitter.getCompression(inExt);
		RDFFormat inFormat = RDFSplitter.getParserFormatForName(inExt);

		DB db = DBMaker.newFileDB(new File(outBaseName+".db"))
			.asyncWriteEnable()
			.closeOnJvmShutdown()
			.commitFileSyncDisable()
			.transactionDisable()
			.make();
		Map<byte[],byte[]> mapOfHashes = db.createHashMap("hashes").make();
		Hasher[] hashers = new Hasher[numThreads];
		for (int i=0; i<hashers.length; i++) {
			hashers[i] = new Hasher(mapOfHashes);
		}
		Hasher bnodeHasher = new Hasher(mapOfHashes);
		try {
			new RDFSplitter(inputFile, inFormat, inCompression, hashers, bnodeHasher, numThreads).call();
		} finally {
			db.close();
		}
	}

	final static class Hasher implements RDFHandler {
		private final Map<byte[],byte[]> hashes;

		Hasher(Map<byte[],byte[]> hashes) {
			this.hashes =  hashes;
		}

		private void add(Value v) {
			Identifier id = valueIO.id(v);
			byte[] ser = valueIO.STREAM_WRITER.toBytes(v);
			byte[] hash = new byte[valueIO.getIdSize()];
			id.writeTo(ByteBuffer.wrap(hash));
			byte[] prev = hashes.put(hash, ser);
			if (prev != null) {
				Value prevValue = valueIO.STREAM_READER.readValue(ByteBuffer.wrap(prev));
				LOGGER.warn("Hash collision! {} and {} both have hash {}", v, prevValue, Hashes.encode(hash));
			}
		}

		@Override
		public void startRDF() throws RDFHandlerException {
		}

		@Override
		public void handleNamespace(String prefix, String uri) throws RDFHandlerException {
		}

		@Override
		public void handleStatement(Statement st) throws RDFHandlerException {
			add(st.getSubject());
			add(st.getPredicate());
			add(st.getObject());
			if (st.getContext() != null) {
				add(st.getContext());
			}
		}

		@Override
		public void handleComment(String comment) throws RDFHandlerException {
		}

		@Override
		public void endRDF() throws RDFHandlerException {
		}
	}
}
