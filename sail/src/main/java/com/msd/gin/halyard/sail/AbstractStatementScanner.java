package com.msd.gin.halyard.sail;

import com.msd.gin.halyard.common.HalyardTableUtils;
import com.msd.gin.halyard.common.RDFContext;
import com.msd.gin.halyard.common.RDFObject;
import com.msd.gin.halyard.common.RDFPredicate;
import com.msd.gin.halyard.common.RDFSubject;
import com.msd.gin.halyard.common.ValueIO;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.hbase.client.Result;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;

public abstract class AbstractStatementScanner implements CloseableIteration<Statement, IOException> {
	private final ValueIO.Reader reader;
	protected final ValueFactory vf;
	protected RDFSubject subj;
	protected RDFPredicate pred;
	protected RDFObject obj;
	protected RDFContext ctx;
	private Statement next = null;
	private Iterator<Statement> iter = null;

	protected AbstractStatementScanner(ValueIO.Reader reader) {
		this.reader = reader;
		this.vf = reader.getValueFactory();
	}

	protected abstract Result nextResult() throws IOException;

	@Override
	public final synchronized boolean hasNext() throws IOException {
		if (next == null) {
			while (true) {
				if (iter == null) {
					Result res = nextResult();
					if (res == null) {
						return false; // no more Results
					}
					iter = HalyardTableUtils.parseStatements(subj, pred, obj, ctx, res, reader).iterator();
				}
				if (iter.hasNext()) {
					Statement s = iter.next();
					next = s; // cache the next statement which will be returned with a call to next().
					return true; // there is another statement
				}
				iter = null;
			}
		} else {
			return true;
		}
	}

	@Override
	public final synchronized Statement next() throws IOException {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}
		// return the next statement and set next to null so it can be refilled by the next call to hasNext()
		Statement st = next;
		next = null;
		return st;
	}

	@Override
	public final void remove() {
		throw new UnsupportedOperationException();
	}
}
