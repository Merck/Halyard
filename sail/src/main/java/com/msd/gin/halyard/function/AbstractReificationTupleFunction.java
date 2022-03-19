package com.msd.gin.halyard.function;

import com.msd.gin.halyard.common.Hashes;
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

	protected abstract Value getValue(Table t, Identifier id, ValueFactory vf) throws IOException;

	@Override
	public final CloseableIteration<? extends List<? extends Value>, QueryEvaluationException> evaluate(ValueFactory vf,
			Value... args)
		throws ValueExprEvaluationException
	{
		if (args.length != 1 || !(args[0] instanceof IRI)) {
			throw new ValueExprEvaluationException(String.format("%s requires an identifier IRI", getURI()));
		}

		IRI idIri = (IRI) args[0];
		Identifier id;
		if (HALYARD.STATEMENT_ID_NS.getName().equals(idIri.getNamespace())) {
			byte[] stmtId = Hashes.decode(idIri.getLocalName());
			byte[] idBytes = new byte[Identifier.ID_SIZE];
			System.arraycopy(stmtId, statementPosition() * Identifier.ID_SIZE, idBytes, 0, Identifier.ID_SIZE);
			id = new Identifier(idBytes);
		} else if (HALYARD.VALUE_ID_NS.getName().equals(idIri.getNamespace())) {
			id = new Identifier(Hashes.decode(idIri.getLocalName()));
		} else {
			throw new ValueExprEvaluationException(String.format("%s requires an identifier IRI", getURI()));
		}

		Table table = (Table) QueryContext.getQueryContext().getAttribute(HBaseSailConnection.QUERY_CONTEXT_TABLE_ATTRIBUTE);
		Value v;
		try {
			v = getValue(table, id, vf);
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
