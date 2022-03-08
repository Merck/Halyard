package com.msd.gin.halyard.rio;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.RDFWriterFactory;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFWriter;

import com.msd.gin.halyard.common.TripleWriter;
import com.msd.gin.halyard.common.ValueIO;

public final class HRDFWriter extends AbstractRDFWriter {

    public static final class Factory implements RDFWriterFactory {

        @Override
        public RDFFormat getRDFFormat() {
            return HRDF.FORMAT;
        }

        @Override
        public RDFWriter getWriter(OutputStream out) {
            return new HRDFWriter(out);
        }

		@Override
		public RDFWriter getWriter(OutputStream out, String baseURI)
			throws URISyntaxException
		{
			return getWriter(out);
		}

		@Override
		public RDFWriter getWriter(Writer writer) {
			throw new UnsupportedOperationException();
		}

		@Override
		public RDFWriter getWriter(Writer writer, String baseURI)
			throws URISyntaxException
		{
			throw new UnsupportedOperationException();
		}
	}

	private final DataOutputStream out;

	public HRDFWriter(OutputStream out) {
		this.out = new DataOutputStream(out);
	}

	@Override
	protected void consumeStatement(Statement st) {
		Resource c = st.getContext();
		ByteBuffer buf = ByteBuffer.allocate(256);
		int numValues = (c != null) ? HRDF.QUADS : HRDF.TRIPLES;
		try {
			out.write(numValues);
			buf = writeValue(st.getSubject(), buf, 2);
			buf = writeValue(st.getPredicate(), buf, 2);
			buf = writeValue(st.getObject(), buf, 4);
			if (c != null) {
				buf = writeValue(c, buf, 2);
			}
			out.write(buf.array(), buf.arrayOffset(), buf.position());
		} catch(IOException e) {
			throw new RDFHandlerException(e);
		}
	}

	private ByteBuffer writeValue(Value v, ByteBuffer buf, int sizeBytes) {
		buf = ValueIO.ensureCapacity(buf, sizeBytes);
		int sizePos = buf.position();
		int startPos = buf.position() + sizeBytes;
		buf.position(startPos);
		buf = ValueIO.writeBytes(v, buf, TRIPLE_WRITER);
		int endPos = buf.position();
		int len = endPos - startPos;
		buf.position(sizePos);
		if (sizeBytes == 2) {
			buf.putShort((short) len);
		} else if (sizeBytes == 4) {
			buf.putInt(len);
		} else {
			throw new AssertionError();
		}
		buf.position(endPos);
		return buf;
	}

	@Override
	public RDFFormat getRDFFormat() {
		return HRDF.FORMAT;
	}

	@Override
	public void handleComment(String comment)
		throws RDFHandlerException
	{
	}

	@Override
	public void endRDF()
		throws RDFHandlerException
	{
	}


	private final TripleWriter TRIPLE_WRITER = new TripleWriter() {
		@Override
		public ByteBuffer writeTriple(Resource subj, IRI pred, Value obj, ByteBuffer buf) {
			buf = writeValue(subj, buf, 2);
			buf = writeValue(pred, buf, 2);
			buf = writeValue(obj, buf, 4);
			return buf;
		}
	};
}
