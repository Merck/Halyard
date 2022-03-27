package com.msd.gin.halyard.rio;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.ByteBuffer;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFParserFactory;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFParser;

import com.msd.gin.halyard.common.ValueIO;
import com.msd.gin.halyard.common.ValueIO.StreamTripleReader;

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

	private static final ValueIO valueIO = ValueIO.create();

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
		clear();
		ValueIO.Reader valueReader = valueIO.createReader(valueFactory, new StreamTripleReader(), (id,vf) -> createNode(id));
		try {
			byte[] buffer = new byte[1024];
			DataInputStream dataIn = new DataInputStream(in);
			if (rdfHandler != null) {
				rdfHandler.startRDF();
			}
			Resource prevContext = null;
			Resource prevSubject = null;
			IRI prevPredicate = null;
			int type;
			while((type = dataIn.read()) != -1) {
				int numValues = (type > HRDF.QUADS) ? type-HRDF.QUADS : type;
				Resource c;
				if (numValues >= HRDF.CSPO) {
					int len = dataIn.readShort();
					buffer = ensureCapacity(buffer, len);
					dataIn.readFully(buffer, 0, len);
					c = (Resource) valueReader.readValue(ByteBuffer.wrap(buffer, 0, len));
				} else if (type > HRDF.QUADS) {
					c = prevContext;
				} else {
					c = null;
				}
				Resource s;
				if (numValues >= HRDF.SPO) { 
					int len = dataIn.readShort();
					buffer = ensureCapacity(buffer, len);
					dataIn.readFully(buffer, 0, len);
					s = (Resource) valueReader.readValue(ByteBuffer.wrap(buffer, 0, len));
				} else {
					s = prevSubject;
				}
				IRI p;
				if (numValues >= HRDF.PO) { 
					int len = dataIn.readShort();
					buffer = ensureCapacity(buffer, len);
					dataIn.readFully(buffer, 0, len);
					p = (IRI) valueReader.readValue(ByteBuffer.wrap(buffer, 0, len));
				} else {
					p = prevPredicate;
				}
				int len = dataIn.readInt();
				buffer = ensureCapacity(buffer, len);
				dataIn.readFully(buffer, 0, len);
				Value o = valueReader.readValue(ByteBuffer.wrap(buffer, 0, len));

				Statement stmt;
				if (c != null) {
					stmt = createStatement(s, p, o, c);
				} else {
					stmt = createStatement(s, p, o);
				}

				if (rdfHandler != null) {
					rdfHandler.handleStatement(stmt);
				}

				prevContext = c;
				prevSubject = s;
				prevPredicate = p;
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
}
