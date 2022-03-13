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

	private Resource prevContext;
	private Resource prevSubject;
	private IRI prevPredicate;

	@Override
	public RDFFormat getRDFFormat() {
		return HRDF.FORMAT;
	}

	@Override
	protected void consumeStatement(Statement st) {
		Resource subj = st.getSubject();
		IRI pred = st.getPredicate();
		Resource c = st.getContext();
		ByteBuffer buf = ByteBuffer.allocate(256);
		int type = HRDF.CSPO;
		if (c == null || c.equals(prevContext)) {
			type--;
			if (subj.equals(prevSubject)) {
				type--;
				if (pred.equals(prevPredicate)) {
					type--;
				}
			}
		}
		if (c != null) {
			type += HRDF.QUADS;
		}
		buf.put((byte) type);
		boolean skip = (c == null) || c.equals(prevContext);
		if (!skip) {
			buf = writeValue(c, buf, 2);
		}
		skip = subj.equals(prevSubject) && skip;
		if (!skip) {
			buf = writeValue(subj, buf, 2);
		}
		skip = pred.equals(prevPredicate) && skip;
		if (!skip) {
			buf = writeValue(pred, buf, 2);
		}
		buf = writeValue(st.getObject(), buf, 4);
		try {
			out.write(buf.array(), buf.arrayOffset(), buf.position());
		} catch(IOException e) {
			throw new RDFHandlerException(e);
		}
		prevContext = c;
		prevSubject = subj;
		prevPredicate = pred;
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
	public void handleComment(String comment)
		throws RDFHandlerException
	{
	}

	@Override
	public void endRDF()
		throws RDFHandlerException
	{
		prevContext = null;
		prevSubject = null;
		prevPredicate = null;
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
