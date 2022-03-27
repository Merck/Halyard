package com.msd.gin.halyard.function;

import com.msd.gin.halyard.common.Hashes;
import com.msd.gin.halyard.common.IdentifiableValueIO;
import com.msd.gin.halyard.common.Identifier;
import com.msd.gin.halyard.sail.HBaseSailConnection;
import com.msd.gin.halyard.vocab.HALYARD;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Table;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.common.iteration.SingletonIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryContext;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunction;

public abstract class AbstractReificationTupleFunction implements TupleFunction {

	protected abstract int statementPosition();

	protected abstract Value getValue(Table t, Identifier id, ValueFactory vf, IdentifiableValueIO valueIO) throws IOException;

	@Override
	public final CloseableIteration<? extends List<? extends Value>, QueryEvaluationException> evaluate(ValueFactory vf,
			Value... args)
		throws ValueExprEvaluationException
	{
		if (args.length != 1 || !(args[0] instanceof IRI)) {
			throw new ValueExprEvaluationException(String.format("%s requires an identifier IRI", getURI()));
		}

		IdentifiableValueIO valueIO = (IdentifiableValueIO) QueryContext.getQueryContext().getAttribute(HBaseSailConnection.QUERY_CONTEXT_VALUEIO_ATTRIBUTE);

		IRI idIri = (IRI) args[0];
		Identifier id;
		if (HALYARD.STATEMENT_ID_NS.getName().equals(idIri.getNamespace())) {
			int idSize = valueIO.getIdSize();
			byte[] stmtId = Hashes.decode(idIri.getLocalName());
			byte[] idBytes = new byte[idSize];
			System.arraycopy(stmtId, statementPosition() * idSize, idBytes, 0, idSize);
			id = valueIO.id(idBytes);
		} else if (HALYARD.VALUE_ID_NS.getName().equals(idIri.getNamespace())) {
			id = valueIO.id(Hashes.decode(idIri.getLocalName()));
		} else {
			throw new ValueExprEvaluationException(String.format("%s requires an identifier IRI", getURI()));
		}

		Table table = (Table) QueryContext.getQueryContext().getAttribute(HBaseSailConnection.QUERY_CONTEXT_TABLE_ATTRIBUTE);
		Value v;
		try {
			v = getValue(table, id, vf, valueIO);
		} catch (IOException e) {
			throw new ValueExprEvaluationException(e);
		}
		if (v != null) {
			return new SingletonIteration<>(Collections.singletonList(v));
		} else {
			return new EmptyIteration<>();
		}
	}
}
