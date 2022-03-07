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
		int numValues = (c != null) ? HRDF.QUADS : HRDF.TRIPLES;
		try {
			out.write(numValues);
			byte[] b = ValueIO.writeBytes(st.getSubject(), TRIPLE_WRITER);
			out.writeShort(b.length);
			out.write(b);
			b = ValueIO.writeBytes(st.getPredicate(), TRIPLE_WRITER);
			out.writeShort(b.length);
			out.write(b);
			b = ValueIO.writeBytes(st.getObject(), TRIPLE_WRITER);
			out.writeInt(b.length);
			out.write(b);
			if (c != null) {
				b = ValueIO.writeBytes(c, TRIPLE_WRITER);
				out.writeShort(b.length);
				out.write(b);
			}
		} catch(IOException e) {
			throw new RDFHandlerException(e);
		}
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
		public byte[] writeTriple(Resource subj, IRI pred, Value obj) {
			byte[] sbytes = ValueIO.writeBytes(subj, this);
			byte[] pbytes = ValueIO.writeBytes(pred, this);
			byte[] obytes = ValueIO.writeBytes(obj, this);
			byte[] b = new byte[2+sbytes.length+2+pbytes.length+4+obytes.length];
			ByteBuffer buf = ByteBuffer.wrap(b);
			buf.putShort((short) sbytes.length);
			buf.put(sbytes);
			buf.putShort((short) pbytes.length);
			buf.put(pbytes);
			buf.putInt(obytes.length);
			buf.put(obytes);
			return b;
		}
	};
}
