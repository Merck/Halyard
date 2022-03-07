package com.msd.gin.halyard.rio;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.ByteBuffer;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFParserFactory;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFParser;

import com.msd.gin.halyard.common.TripleFactory;
import com.msd.gin.halyard.common.ValueIO;

public final class HRDFParser extends AbstractRDFParser {

    public static final class Factory implements RDFParserFactory {

        @Override
        public RDFFormat getRDFFormat() {
            return HRDF.FORMAT;
        }

        @Override
        public RDFParser getParser() {
            return new HRDFParser();
        }

    }

	@Override
	public RDFFormat getRDFFormat() {
		return com.msd.gin.halyard.rio.HRDF.FORMAT;
	}

	@Override
	public void parse(InputStream in, String baseURI)
		throws IOException,
		RDFParseException,
		RDFHandlerException
	{
		try {
			byte[] buffer = new byte[1024];
			DataInputStream dataIn = new DataInputStream(in);
			if (rdfHandler != null) {
				rdfHandler.startRDF();
			}
			int numValues;
			while((numValues = dataIn.read()) != -1) {
				int len = dataIn.readShort();
				buffer = ensureCapacity(buffer, len);
				dataIn.readFully(buffer, 0, len);
				Resource s = (Resource) readValue(ByteBuffer.wrap(buffer, 0, len));
	
				len = dataIn.readShort();
				buffer = ensureCapacity(buffer, len);
				dataIn.readFully(buffer, 0, len);
				IRI p = (IRI) readValue(ByteBuffer.wrap(buffer, 0, len));
	
				len = dataIn.readInt();
				buffer = ensureCapacity(buffer, len);
				dataIn.readFully(buffer, 0, len);
				Value o = readValue(ByteBuffer.wrap(buffer, 0, len));
	
				Statement stmt;
				if (numValues == HRDF.TRIPLES) {
					stmt = createStatement(s, p, o);
				} else if (numValues == HRDF.QUADS) {
					len = dataIn.readShort();
					buffer = ensureCapacity(buffer, len);
					dataIn.readFully(buffer, 0, len);
					Resource c = (Resource) readValue(ByteBuffer.wrap(buffer, 0, len));
	
					stmt = createStatement(s, p, o, c);
				} else {
					throw new RDFParseException("Invalid number of values: "+numValues);
				}
	
				if (rdfHandler != null) {
					rdfHandler.handleStatement(stmt);
				}
			}
			if (rdfHandler != null) {
				rdfHandler.endRDF();
			}
		} finally {
			clear();
		}
	}

	private byte[] ensureCapacity(byte[] buffer, int requiredSize) {
		if (buffer.length < requiredSize) {
			return new byte[requiredSize];
		} else {
			return buffer;
		}
	}

	@Override
	public void parse(Reader reader, String baseURI)
		throws IOException,
		RDFParseException,
		RDFHandlerException
	{
		throw new UnsupportedOperationException();
	}

	private Value readValue(ByteBuffer b) throws IOException {
		Value v = ValueIO.readValue(b, valueFactory, TRIPLE_FACTORY);
		if (v.isBNode()) {
			v = createNode(((BNode)v).getID());
		}
		return v;
	}


	private final TripleFactory TRIPLE_FACTORY = new TripleFactory() {
		@Override
		public Triple readTriple(ByteBuffer b, ValueFactory vf) throws IOException {
			int originalLimit = b.limit();
			int len = b.getShort();
			b.limit(b.position()+len);
			Resource s = (Resource) readValue(b);
			b.limit(originalLimit);

			len = b.getShort();
			b.limit(b.position()+len);
			IRI p = (IRI) readValue(b);
			b.limit(originalLimit);

			len = b.getInt();
			b.limit(b.position()+len);
			Value o = readValue(b);
			b.limit(originalLimit);

			return vf.createTriple(s, p, o);
		}
	};
}
