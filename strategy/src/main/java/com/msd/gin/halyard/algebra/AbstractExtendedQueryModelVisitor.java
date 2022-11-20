package com.msd.gin.halyard.algebra;

import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.TupleFunctionCall;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

public abstract class AbstractExtendedQueryModelVisitor<X extends Exception> extends AbstractQueryModelVisitor<X> {
	@Override
	public void meetOther(QueryModelNode node) throws X {
		if (node instanceof TupleFunctionCall) {
			// all TupleFunctionCalls are expected to be ExtendedTupleFunctionCalls
			meet((ExtendedTupleFunctionCall)node);
		} else if (node instanceof StarJoin) {
			meet((StarJoin)node);
		} else {
			super.meetOther(node);
		}
	}

	public void meet(ExtendedTupleFunctionCall node) throws X {
		meetNode(node);
	}

	protected void meetNAryTupleOperator(NAryTupleOperator node) throws X {
		meetNode(node);
	}

	public void meet(StarJoin node) throws X {
		meetNAryTupleOperator(node);
	}

}
