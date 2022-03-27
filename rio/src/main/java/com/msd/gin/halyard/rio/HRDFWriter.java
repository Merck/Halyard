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
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.RDFWriterFactory;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFWriter;

import com.msd.gin.halyard.common.ValueIO;
import com.msd.gin.halyard.common.ValueIO.StreamTripleWriter;

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
	private static final ValueIO valueIO = ValueIO.create();
	private static final ValueIO.Writer valueWriter = valueIO.createWriter(new StreamTripleWriter());

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
			buf = ValueIO.writeValue(c, valueWriter, buf, 2);
		}
		skip = subj.equals(prevSubject) && skip;
		if (!skip) {
			buf = ValueIO.writeValue(subj, valueWriter, buf, 2);
		}
		skip = pred.equals(prevPredicate) && skip;
		if (!skip) {
			buf = ValueIO.writeValue(pred, valueWriter, buf, 2);
		}
		buf = ValueIO.writeValue(st.getObject(), valueWriter, buf, 4);
		try {
			out.write(buf.array(), buf.arrayOffset(), buf.position());
		} catch(IOException e) {
			throw new RDFHandlerException(e);
		}
		prevContext = c;
		prevSubject = subj;
		prevPredicate = pred;
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
}
