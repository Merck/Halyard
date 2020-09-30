package com.msd.gin.halyard.repository;

import com.msd.gin.halyard.sail.HBaseSail;
import com.msd.gin.halyard.sail.TimestampedUpdateContext;
import com.msd.gin.halyard.vocab.HALYARD;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.rdf4j.RDF4JException;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.ConvertingIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.common.iteration.TimeLimitIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF4J;
import org.eclipse.rdf4j.model.vocabulary.SESAME;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryInterruptedException;
import org.eclipse.rdf4j.query.UpdateExecutionException;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.Modify;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.SingletonSet;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.TupleFunctionCall;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.UpdateExpr;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunctionRegistry;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.query.parser.ParsedUpdate;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailUpdate;
import org.eclipse.rdf4j.repository.sail.helpers.SailUpdateExecutor;
import org.eclipse.rdf4j.rio.ParserConfig;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.UpdateContext;
import org.eclipse.rdf4j.sail.spin.SpinMagicPropertyInterpreter;
import org.eclipse.rdf4j.spin.SpinParser;
import org.slf4j.LoggerFactory;

public class HBaseUpdate extends SailUpdate {
	private final HBaseSail sail;

	public HBaseUpdate(ParsedUpdate parsedUpdate, HBaseSail sail, SailRepositoryConnection con) {
		super(parsedUpdate, con);
		this.sail = sail;
	}

	@Override
	public void execute() throws UpdateExecutionException {
		ParsedUpdate parsedUpdate = getParsedUpdate();
		List<UpdateExpr> updateExprs = parsedUpdate.getUpdateExprs();
		Map<UpdateExpr, Dataset> datasetMapping = parsedUpdate.getDatasetMapping();

		SailRepositoryConnection con = getConnection();
		HBaseUpdateExecutor executor = new HBaseUpdateExecutor(sail, con.getSailConnection(), con.getValueFactory(), con.getParserConfig());

		boolean localTransaction = false;
		try {
			if (!getConnection().isActive()) {
				localTransaction = true;
				beginLocalTransaction();
			}
			for (UpdateExpr updateExpr : updateExprs) {

				Dataset activeDataset = getMergedDataset(datasetMapping.get(updateExpr));

				try {
					executor.executeUpdate(updateExpr, activeDataset, getBindings(), getIncludeInferred(), getMaxExecutionTime());
				} catch (RDF4JException | IOException e) {
					LoggerFactory.getLogger(getClass()).warn("exception during update execution: ", e);
					if (!updateExpr.isSilent()) {
						throw new UpdateExecutionException(e);
					}
				}
			}

			if (localTransaction) {
				commitLocalTransaction();
				localTransaction = false;
			}
		} finally {
			if (localTransaction) {
				rollbackLocalTransaction();
			}
		}
	}

	private void beginLocalTransaction() throws RepositoryException {
		getConnection().begin();
	}

	private void commitLocalTransaction() throws RepositoryException {
		getConnection().commit();

	}

	private void rollbackLocalTransaction() throws RepositoryException {
		getConnection().rollback();

	}

	static class HBaseUpdateExecutor extends SailUpdateExecutor {
		final HBaseSail sail;
		final SailConnection con;
		final ValueFactory vf;

		public HBaseUpdateExecutor(HBaseSail sail, SailConnection con, ValueFactory vf, ParserConfig loadConfig) {
			super(con, vf, loadConfig);
			this.sail = sail;
			this.con = con;
			this.vf = vf;
		}

		protected void executeModify(Modify modify, UpdateContext uc, int maxExecutionTime) throws SailException {
			try {
				ModifyInfo insertInfo = null;
				TupleExpr insertClause = modify.getInsertExpr();
				if (insertClause != null) {
					// for inserts, TupleFunctions are expected in the insert clause
					insertClause = optimize(insertClause, uc.getDataset(), uc.getBindingSet());
					insertInfo = InsertCollector.process(insertClause);
				}

				ModifyInfo deleteInfo = null;
				TupleExpr deleteClause = modify.getDeleteExpr();
				TupleExpr whereClause = modify.getWhereExpr();
				if (deleteClause != null) {
					// for deletes, TupleFunctions are expected in the where clause
					whereClause = optimize(whereClause, uc.getDataset(), uc.getBindingSet());
					deleteInfo = new ModifyInfo(StatementPatternCollector.process(deleteClause), WhereCollector.process(whereClause));
				}

				if (!(whereClause instanceof QueryRoot)) {
					whereClause = new QueryRoot(whereClause);
				}

				CloseableIteration<? extends BindingSet, QueryEvaluationException> sourceBindings = null;
				try {
					sourceBindings = evaluateWhereClause(whereClause, uc, maxExecutionTime);
					TimestampedUpdateContext tsUc = new TimestampedUpdateContext(uc.getUpdateExpr(), uc.getDataset(), uc.getBindingSet(), uc.isIncludeInferred());
					while (sourceBindings.hasNext()) {
						BindingSet sourceBinding = sourceBindings.next();
						deleteBoundTriples(sourceBinding, deleteInfo, tsUc);

						insertBoundTriples(sourceBinding, insertInfo, tsUc);
					}
				} finally {
					if (sourceBindings != null) {
						sourceBindings.close();
					}
				}
			} catch (QueryEvaluationException e) {
				throw new SailException(e);
			}
		}

		private IRI[] getDefaultRemoveGraphs(Dataset dataset) {
			if (dataset == null) {
				return new IRI[0];
			}
			Set<IRI> set = new HashSet<>(dataset.getDefaultRemoveGraphs());
			if (set.isEmpty()) {
				return new IRI[0];
			}
			if (set.remove(RDF4J.NIL) | set.remove(SESAME.NIL)) {
				set.add(null);
			}

			return set.toArray(new IRI[set.size()]);
		}

		private TupleExpr optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
			SpinParser spinParser = sail.getSpinParser();
			TripleSource source = new TripleSource() {
				@Override
				public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(Resource subj, IRI pred, Value obj, Resource... contexts) throws QueryEvaluationException {
					return new EmptyIteration<>();
				}

				@Override
				public ValueFactory getValueFactory() {
					return vf;
				}
			};
			TupleFunctionRegistry tupleFunctionRegistry = sail.getTupleFunctionRegistry();
			new SpinMagicPropertyInterpreter(spinParser, source, tupleFunctionRegistry, null).optimize(tupleExpr, dataset, bindings);
			return tupleExpr;
		}

		private CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluateWhereClause(final TupleExpr whereClause, final UpdateContext uc, final int maxExecutionTime) throws SailException, QueryEvaluationException {
			CloseableIteration<? extends BindingSet, QueryEvaluationException> sourceBindings1 = null;
			CloseableIteration<? extends BindingSet, QueryEvaluationException> sourceBindings2 = null;
			ConvertingIteration<BindingSet, BindingSet, QueryEvaluationException> result = null;
			boolean allGood = false;
			try {
				sourceBindings1 = con.evaluate(whereClause, uc.getDataset(), uc.getBindingSet(), uc.isIncludeInferred());

				if (maxExecutionTime > 0) {
					sourceBindings2 = new TimeLimitIteration<BindingSet, QueryEvaluationException>(sourceBindings1, 1000L * maxExecutionTime) {

						@Override
						protected void throwInterruptedException() throws QueryEvaluationException {
							throw new QueryInterruptedException("execution took too long");
						}
					};
				} else {
					sourceBindings2 = sourceBindings1;
				}

				result = new ConvertingIteration<BindingSet, BindingSet, QueryEvaluationException>(sourceBindings2) {

					protected BindingSet convert(BindingSet sourceBinding) throws QueryEvaluationException {
						if (whereClause instanceof SingletonSet && sourceBinding instanceof EmptyBindingSet && uc.getBindingSet() != null) {
							// in the case of an empty WHERE clause, we use the
							// supplied
							// bindings to produce triples to DELETE/INSERT
							return uc.getBindingSet();
						} else {
							// check if any supplied bindings do not occur in the
							// bindingset
							// produced by the WHERE clause. If so, merge.
							Set<String> uniqueBindings = new HashSet<String>(uc.getBindingSet().getBindingNames());
							uniqueBindings.removeAll(sourceBinding.getBindingNames());
							if (uniqueBindings.size() > 0) {
								MapBindingSet mergedSet = new MapBindingSet();
								for (String bindingName : sourceBinding.getBindingNames()) {
									mergedSet.addBinding(sourceBinding.getBinding(bindingName));
								}
								for (String bindingName : uniqueBindings) {
									mergedSet.addBinding(uc.getBindingSet().getBinding(bindingName));
								}
								return mergedSet;
							}
							return sourceBinding;
						}
					}
				};
				allGood = true;
				return result;
			} finally {
				if (!allGood) {
					try {
						if (result != null) {
							result.close();
						}
					} finally {
						try {
							if (sourceBindings2 != null) {
								sourceBindings2.close();
							}
						} finally {
							if (sourceBindings1 != null) {
								sourceBindings1.close();
							}
						}
					}
				}
			}
		}

		private void deleteBoundTriples(BindingSet whereBinding, ModifyInfo deleteClause, TimestampedUpdateContext uc) throws SailException {
			if (deleteClause != null) {
				List<StatementPattern> deletePatterns = deleteClause.getStatementPatterns();

				Value patternValue;
				for (StatementPattern deletePattern : deletePatterns) {

					patternValue = getValueForVar(deletePattern.getSubjectVar(), whereBinding);
					Resource subject = patternValue instanceof Resource ? (Resource) patternValue : null;

					patternValue = getValueForVar(deletePattern.getPredicateVar(), whereBinding);
					IRI predicate = patternValue instanceof IRI ? (IRI) patternValue : null;

					Value object = getValueForVar(deletePattern.getObjectVar(), whereBinding);

					Resource context = null;
					if (deletePattern.getContextVar() != null) {
						patternValue = getValueForVar(deletePattern.getContextVar(), whereBinding);
						context = patternValue instanceof Resource ? (Resource) patternValue : null;
					}

					if (subject == null || predicate == null || object == null) {
						/*
						 * skip removal of triple if any variable is unbound (may happen with optional patterns or if triple pattern forms illegal triple). See SES-1047 and #610.
						 */
						continue;
					}

					Statement toBeDeleted = (context != null) ? vf.createStatement(subject, predicate, object, context) : vf.createStatement(subject, predicate, object);
					setTimestamp(uc, toBeDeleted, deleteClause.getTupleFunctionCalls(), whereBinding);

					if (context != null) {
						if (RDF4J.NIL.equals(context) || SESAME.NIL.equals(context)) {
							con.removeStatement(uc, subject, predicate, object, (Resource) null);
						} else {
							con.removeStatement(uc, subject, predicate, object, context);
						}
					} else {
						IRI[] remove = getDefaultRemoveGraphs(uc.getDataset());
						con.removeStatement(uc, subject, predicate, object, remove);
					}
				}
			}
		}

		private void insertBoundTriples(BindingSet whereBinding, ModifyInfo insertClause, TimestampedUpdateContext uc) throws SailException {
			if (insertClause != null) {
				List<StatementPattern> insertPatterns = insertClause.getStatementPatterns();

				// bnodes in the insert pattern are locally scoped for each
				// individual source binding.
				MapBindingSet bnodeMapping = new MapBindingSet();
				for (StatementPattern insertPattern : insertPatterns) {
					Statement toBeInserted = createStatementFromPattern(insertPattern, whereBinding, bnodeMapping);

					if (toBeInserted != null) {
						setTimestamp(uc, toBeInserted, insertClause.getTupleFunctionCalls(), whereBinding);

						IRI with = uc.getDataset().getDefaultInsertGraph();
						if (with == null && toBeInserted.getContext() == null) {
							con.addStatement(uc, toBeInserted.getSubject(), toBeInserted.getPredicate(), toBeInserted.getObject());
						} else if (toBeInserted.getContext() == null) {
							con.addStatement(uc, toBeInserted.getSubject(), toBeInserted.getPredicate(), toBeInserted.getObject(), with);
						} else {
							con.addStatement(uc, toBeInserted.getSubject(), toBeInserted.getPredicate(), toBeInserted.getObject(), toBeInserted.getContext());
						}
					}
				}
			}
		}

		private void setTimestamp(TimestampedUpdateContext uc, Statement stmt, List<TupleFunctionCall> tupleFunctionCalls, BindingSet bindings) {
			for (TupleFunctionCall tfc : tupleFunctionCalls) {
				if (HALYARD.TIMESTAMP_PROPERTY.stringValue().equals(tfc.getURI())) {
					List<ValueExpr> args = tfc.getArgs();
					Resource tsSubj = (Resource) getValueForVar((Var) args.get(0), bindings);
					IRI tsPred = (IRI) getValueForVar((Var) args.get(1), bindings);
					Value tsObj = getValueForVar((Var) args.get(2), bindings);
					Statement tsStmt;
					if (args.size() == 3) {
						tsStmt = vf.createStatement(tsSubj, tsPred, tsObj);
					} else if (args.size() == 4) {
						Resource tsCtx = (Resource) getValueForVar((Var) args.get(3), bindings);
						tsStmt = vf.createStatement(tsSubj, tsPred, tsObj, tsCtx);
					} else {
						tsStmt = null;
					}
					if (stmt.equals(tsStmt)) {
						Literal ts = (Literal) getValueForVar(tfc.getResultVars().get(0), bindings);
						if (XSD.DATETIME.equals(ts.getDatatype())) {
							uc.setTimestamp(ts.calendarValue().toGregorianCalendar().getTimeInMillis());
						} else {
							uc.setTimestamp(ts.longValue());
						}
						return;
					}
				}
			}
		}

		private Statement createStatementFromPattern(StatementPattern pattern, BindingSet sourceBinding, MapBindingSet bnodeMapping) throws SailException {

			Resource subject = null;
			IRI predicate = null;
			Value object = null;
			Resource context = null;

			Value patternValue;
			if (pattern.getSubjectVar().hasValue()) {
				patternValue = pattern.getSubjectVar().getValue();
				if (patternValue instanceof Resource) {
					subject = (Resource) patternValue;
				}
			} else {
				patternValue = sourceBinding.getValue(pattern.getSubjectVar().getName());
				if (patternValue instanceof Resource) {
					subject = (Resource) patternValue;
				}

				if (subject == null && pattern.getSubjectVar().isAnonymous()) {
					Binding mappedSubject = bnodeMapping.getBinding(pattern.getSubjectVar().getName());

					if (mappedSubject != null) {
						patternValue = mappedSubject.getValue();
						if (patternValue instanceof Resource) {
							subject = (Resource) patternValue;
						}
					} else {
						subject = vf.createBNode();
						bnodeMapping.addBinding(pattern.getSubjectVar().getName(), subject);
					}
				}
			}

			if (subject == null) {
				return null;
			}

			if (pattern.getPredicateVar().hasValue()) {
				patternValue = pattern.getPredicateVar().getValue();
				if (patternValue instanceof IRI) {
					predicate = (IRI) patternValue;
				}
			} else {
				patternValue = sourceBinding.getValue(pattern.getPredicateVar().getName());
				if (patternValue instanceof IRI) {
					predicate = (IRI) patternValue;
				}
			}

			if (predicate == null) {
				return null;
			}

			if (pattern.getObjectVar().hasValue()) {
				object = pattern.getObjectVar().getValue();
			} else {
				object = sourceBinding.getValue(pattern.getObjectVar().getName());

				if (object == null && pattern.getObjectVar().isAnonymous()) {
					Binding mappedObject = bnodeMapping.getBinding(pattern.getObjectVar().getName());

					if (mappedObject != null) {
						patternValue = mappedObject.getValue();
						if (patternValue instanceof Resource) {
							object = (Resource) patternValue;
						}
					} else {
						object = vf.createBNode();
						bnodeMapping.addBinding(pattern.getObjectVar().getName(), object);
					}
				}
			}

			if (object == null) {
				return null;
			}

			if (pattern.getContextVar() != null) {
				if (pattern.getContextVar().hasValue()) {
					patternValue = pattern.getContextVar().getValue();
					if (patternValue instanceof Resource) {
						context = (Resource) patternValue;
					}
				} else {
					patternValue = sourceBinding.getValue(pattern.getContextVar().getName());
					if (patternValue instanceof Resource) {
						context = (Resource) patternValue;
					}
				}
			}

			Statement st;
			if (context != null) {
				st = vf.createStatement(subject, predicate, object, context);
			} else {
				st = vf.createStatement(subject, predicate, object);
			}
			return st;
		}

		private Value getValueForVar(Var var, BindingSet bindings) throws SailException {
			Value value = null;
			if (var.hasValue()) {
				value = var.getValue();
			} else {
				value = bindings.getValue(var.getName());
			}
			return value;
		}

	}

	static class ModifyInfo {
		private final List<StatementPattern> stPatterns;
		private final List<TupleFunctionCall> tupleFunctionCalls;

		ModifyInfo(List<StatementPattern> stPatterns, List<TupleFunctionCall> tupleFunctionCalls) {
			this.stPatterns = stPatterns;
			this.tupleFunctionCalls = tupleFunctionCalls;
		}

		public List<StatementPattern> getStatementPatterns() {
			return stPatterns;
		}

		public List<TupleFunctionCall> getTupleFunctionCalls() {
			return tupleFunctionCalls;
		}
	}

	static class InsertCollector extends StatementPatternCollector {
		private final List<TupleFunctionCall> tupleFunctionCalls = new ArrayList<>();

		static ModifyInfo process(TupleExpr expr) {
			InsertCollector insertCollector = new InsertCollector();
			expr.visit(insertCollector);
			return new ModifyInfo(insertCollector.getStatementPatterns(), insertCollector.getTupleFunctionCalls());
		}

		public List<TupleFunctionCall> getTupleFunctionCalls() {
			return tupleFunctionCalls;
		}

		public void meet(Union node) {
			if (node.getRightArg() instanceof TupleFunctionCall) {
				tupleFunctionCalls.add((TupleFunctionCall) node.getRightArg());
			}
		}
	}

	static class WhereCollector extends AbstractQueryModelVisitor<RuntimeException> {
		private final List<TupleFunctionCall> tupleFunctionCalls = new ArrayList<>();

		static List<TupleFunctionCall> process(TupleExpr expr) {
			WhereCollector whereCollector = new WhereCollector();
			expr.visit(whereCollector);
			return whereCollector.getTupleFunctionCalls();
		}

		public List<TupleFunctionCall> getTupleFunctionCalls() {
			return tupleFunctionCalls;
		}

		@Override
		public void meet(Filter node) {
			// Skip boolean constraints
			node.getArg().visit(this);
		}

		@Override
		public void meet(StatementPattern node) {
			// skip
		}

		public void meet(Union node) {
			if (node.getRightArg() instanceof TupleFunctionCall) {
				TupleFunctionCall tfc = (TupleFunctionCall) node.getRightArg();
				tupleFunctionCalls.add(tfc);
				if (HALYARD.TIMESTAMP_PROPERTY.stringValue().equals(tfc.getURI())) {
					node.replaceWith(new SingletonSet());
				}
			}
		}
	}

}
