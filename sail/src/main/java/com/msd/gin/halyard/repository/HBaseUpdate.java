package com.msd.gin.halyard.repository;

import com.google.common.base.Stopwatch;
import com.msd.gin.halyard.algebra.AbstractExtendedQueryModelVisitor;
import com.msd.gin.halyard.algebra.Algebra;
import com.msd.gin.halyard.algebra.evaluation.EmptyTripleSource;
import com.msd.gin.halyard.query.BindingSetPipe;
import com.msd.gin.halyard.query.QueueingBindingSetPipe;
import com.msd.gin.halyard.sail.HBaseSail;
import com.msd.gin.halyard.sail.HBaseSailConnection;
import com.msd.gin.halyard.sail.TimestampedUpdateContext;
import com.msd.gin.halyard.spin.SpinMagicPropertyInterpreter;
import com.msd.gin.halyard.spin.SpinParser;
import com.msd.gin.halyard.vocab.HALYARD;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.eclipse.rdf4j.common.exception.RDF4JException;
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
import org.eclipse.rdf4j.query.UpdateExecutionException;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.Modify;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.Reduced;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.TupleFunctionCall;
import org.eclipse.rdf4j.query.algebra.UpdateExpr;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunctionRegistry;
import org.eclipse.rdf4j.query.algebra.helpers.collectors.StatementPatternCollector;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.query.parser.ParsedUpdate;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailUpdate;
import org.eclipse.rdf4j.repository.sail.helpers.SailUpdateExecutor;
import org.eclipse.rdf4j.rio.ParserConfig;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.UpdateContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseUpdate extends SailUpdate {
	private static final Logger LOGGER = LoggerFactory.getLogger(HBaseUpdate.class);

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
		ValueFactory vf = con.getValueFactory();
		HBaseUpdateExecutor executor = new HBaseUpdateExecutor(sail, (HBaseSailConnection) con.getSailConnection(), vf, con.getParserConfig());

		boolean localTransaction = false;
		try {
			if (!getConnection().isActive()) {
				localTransaction = true;
				beginLocalTransaction();
			}
			for (int i = 0; i < updateExprs.size(); i++) {
				UpdateExpr updateExpr = updateExprs.get(i);
				Dataset activeDataset = getMergedDataset(datasetMapping.get(updateExpr));

				QueryBindingSet updateBindings = new QueryBindingSet(getBindings());
				updateBindings.addBinding(HBaseSailConnection.UPDATE_PART_BINDING, vf.createLiteral(i));
				try {
					executor.executeUpdate(updateExpr, activeDataset, updateBindings, getIncludeInferred(), getMaxExecutionTime());
				} catch (RDF4JException | IOException e) {
					LOGGER.warn("exception during update execution: ", e);
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
		final HBaseSailConnection con;
		final ValueFactory vf;

		public HBaseUpdateExecutor(HBaseSail sail, HBaseSailConnection con, ValueFactory vf, ParserConfig loadConfig) {
			super(con, vf, loadConfig);
			this.sail = sail;
			this.con = con;
			this.vf = vf;
		}

		@Override
		protected void executeModify(Modify modify, UpdateContext uc, int maxExecutionTime) throws SailException {
			try {
				ModifyInfo insertInfo;
				TupleExpr insertClause = modify.getInsertExpr();
				if (insertClause != null) {
					// for inserts, TupleFunctions are expected in the insert clause
					insertClause = Algebra.ensureRooted(insertClause);
					insertClause = optimize(insertClause, uc.getDataset(), uc.getBindingSet(), false);
					insertInfo = InsertCollector.process(insertClause);
				} else {
					insertInfo = null;
				}

				ModifyInfo deleteInfo;
				TupleExpr deleteClause = modify.getDeleteExpr();
				TupleExpr whereClause = new Reduced(modify.getWhereExpr());
				whereClause = Algebra.ensureRooted(whereClause);
				if (deleteClause != null) {
					// for deletes, TupleFunctions are expected in the where clause
					whereClause = optimize(whereClause, uc.getDataset(), uc.getBindingSet(), true);
					deleteInfo = new ModifyInfo(deleteClause, StatementPatternCollector.process(deleteClause), WhereCollector.process(whereClause));
				} else {
					deleteInfo = null;
				}

				TimestampedUpdateContext tsUc = new TimestampedUpdateContext(uc.getUpdateExpr(), uc.getDataset(), uc.getBindingSet(), uc.isIncludeInferred());
				long timeout = maxExecutionTime > 0 ? maxExecutionTime : Integer.MAX_VALUE;
				QueueingBindingSetPipe pipe = new QueueingBindingSetPipe(sail.getExecutor().getMaxQueueSize(), timeout, TimeUnit.SECONDS);
				evaluateWhereClause(pipe, whereClause, uc);
				if (con.isTrackResultSize()) {
					if (deleteInfo != null) {
						deleteInfo.getClause().setResultSizeActual(0);
					}
					if (insertInfo != null) {
						insertInfo.getClause().setResultSizeActual(0);
					}
				}
				pipe.collect(next -> {
					if (deleteInfo != null) {
						deleteBoundTriples(next, deleteInfo, tsUc);
					}
					if (insertInfo != null) {
						insertBoundTriples(next, insertInfo, tsUc);
					}
				});

				if (con.isTrackResultSize()) {
					// copy results back from cloned expressions
					if (deleteInfo != null) {
						modify.getDeleteExpr().setResultSizeActual(deleteInfo.getClause().getResultSizeActual());
					}
					if (insertInfo != null) {
						modify.getInsertExpr().setResultSizeActual(insertInfo.getClause().getResultSizeActual());
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

		private TupleExpr optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeMatchingTriples) {
			LOGGER.debug("Update TupleExpr before interpretation:\n{}", tupleExpr);
			SpinParser spinParser = sail.getSpinParser();
			TripleSource source = new EmptyTripleSource(vf);
			TupleFunctionRegistry tupleFunctionRegistry = sail.getTupleFunctionRegistry();
			new SpinMagicPropertyInterpreter(spinParser, source, tupleFunctionRegistry, null, includeMatchingTriples).optimize(tupleExpr, dataset, bindings);
			LOGGER.debug("Update TupleExpr after interpretation:\n{}", tupleExpr);
			return tupleExpr;
		}

		private void evaluateWhereClause(BindingSetPipe handler, final TupleExpr whereClause, final UpdateContext uc) {
			handler = new BindingSetPipe(handler) {
				private final boolean isEmptyWhere = Algebra.isEmpty(whereClause);
				private final BindingSet ucBinding = uc.getBindingSet();
				@Override
				protected boolean next(BindingSet sourceBinding) {
					if (isEmptyWhere && sourceBinding.isEmpty() && ucBinding != null) {
						// in the case of an empty WHERE clause, we use the
						// supplied
						// bindings to produce triples to DELETE/INSERT
						return super.next(ucBinding);
					} else {
						// check if any supplied bindings do not occur in the
						// bindingset
						// produced by the WHERE clause. If so, merge.
						Set<String> uniqueBindings = new HashSet<String>(ucBinding.getBindingNames());
						uniqueBindings.removeAll(sourceBinding.getBindingNames());
						if (!uniqueBindings.isEmpty()) {
							MapBindingSet mergedSet = new MapBindingSet(sourceBinding.size() + uniqueBindings.size());
							for (String bindingName : sourceBinding.getBindingNames()) {
								mergedSet.addBinding(sourceBinding.getBinding(bindingName));
							}
							for (String bindingName : uniqueBindings) {
								mergedSet.addBinding(ucBinding.getBinding(bindingName));
							}
							return super.next(mergedSet);
						} else {
							return super.next(sourceBinding);
						}
					}
				}
			};
			con.evaluate(handler, whereClause, uc.getDataset(), uc.getBindingSet(), uc.isIncludeInferred());
		}

		private void deleteBoundTriples(BindingSet whereBinding, ModifyInfo deleteInfo, TimestampedUpdateContext uc) throws SailException {
			List<StatementPattern> deletePatterns = deleteInfo.getStatementPatterns();

			TupleExpr clause = deleteInfo.getClause();
			int deleteCount = 0;
			Stopwatch stopwatch;
			if (con.isTrackResultTime()) {
				clause.setTotalTimeNanosActual(Math.max(0, clause.getTotalTimeNanosActual()));
				stopwatch = Stopwatch.createStarted();
			} else {
				stopwatch = null;
			}
			Value patternValue;
			for (StatementPattern deletePattern : deletePatterns) {

				patternValue = Algebra.getVarValue(deletePattern.getSubjectVar(), whereBinding);
				Resource subject = patternValue instanceof Resource ? (Resource) patternValue : null;

				patternValue = Algebra.getVarValue(deletePattern.getPredicateVar(), whereBinding);
				IRI predicate = patternValue instanceof IRI ? (IRI) patternValue : null;

				Value object = Algebra.getVarValue(deletePattern.getObjectVar(), whereBinding);

				Resource context = null;
				if (deletePattern.getContextVar() != null) {
					patternValue = Algebra.getVarValue(deletePattern.getContextVar(), whereBinding);
					context = patternValue instanceof Resource ? (Resource) patternValue : null;
				}

				if (subject == null || predicate == null || object == null) {
					/*
					 * skip removal of triple if any variable is unbound (may happen with optional patterns or if triple pattern forms illegal triple). See SES-1047 and #610.
					 */
					continue;
				}

				Statement toBeDeleted = (context != null) ? vf.createStatement(subject, predicate, object, context) : vf.createStatement(subject, predicate, object);
				setTimestamp(uc, toBeDeleted, deleteInfo.getTupleFunctionCalls(), whereBinding);

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
				deleteCount++;
			}
			if (con.isTrackResultTime()) {
				stopwatch.stop();
				clause.setTotalTimeNanosActual(clause.getTotalTimeNanosActual() + stopwatch.elapsed(TimeUnit.NANOSECONDS));
			}
			if (con.isTrackResultSize()) {
				clause.setResultSizeActual(clause.getResultSizeActual() + deleteCount);
			}
		}

		private void insertBoundTriples(BindingSet whereBinding, ModifyInfo insertInfo, TimestampedUpdateContext uc) throws SailException {
			List<StatementPattern> insertPatterns = insertInfo.getStatementPatterns();

			TupleExpr clause = insertInfo.getClause();
			int insertCount = 0;
			Stopwatch stopwatch;
			if (con.isTrackResultTime()) {
				clause.setTotalTimeNanosActual(Math.max(0, clause.getTotalTimeNanosActual()));
				stopwatch = Stopwatch.createStarted();
			} else {
				stopwatch = null;
			}
			// bnodes in the insert pattern are locally scoped for each
			// individual source binding.
			MapBindingSet bnodeMapping = new MapBindingSet();
			for (StatementPattern insertPattern : insertPatterns) {
				Statement toBeInserted = createStatementFromPattern(insertPattern, whereBinding, bnodeMapping);

				if (toBeInserted != null) {
					setTimestamp(uc, toBeInserted, insertInfo.getTupleFunctionCalls(), whereBinding);

					IRI with = uc.getDataset().getDefaultInsertGraph();
					if (with == null && toBeInserted.getContext() == null) {
						con.addStatement(uc, toBeInserted.getSubject(), toBeInserted.getPredicate(), toBeInserted.getObject());
					} else if (toBeInserted.getContext() == null) {
						con.addStatement(uc, toBeInserted.getSubject(), toBeInserted.getPredicate(), toBeInserted.getObject(), with);
					} else {
						con.addStatement(uc, toBeInserted.getSubject(), toBeInserted.getPredicate(), toBeInserted.getObject(), toBeInserted.getContext());
					}
					insertCount++;
				}
			}
			if (con.isTrackResultTime()) {
				stopwatch.stop();
				clause.setTotalTimeNanosActual(clause.getTotalTimeNanosActual() + stopwatch.elapsed(TimeUnit.NANOSECONDS));
			}
			if (con.isTrackResultSize()) {
				clause.setResultSizeActual(clause.getResultSizeActual() + insertCount);
			}
		}

		private void setTimestamp(TimestampedUpdateContext uc, Statement stmt, List<TupleFunctionCall> tupleFunctionCalls, BindingSet bindings) {
			for (TupleFunctionCall tfc : tupleFunctionCalls) {
				if (HALYARD.TIMESTAMP_PROPERTY.stringValue().equals(tfc.getURI())) {
					List<ValueExpr> args = tfc.getArgs();
					Resource tsSubj = (Resource) Algebra.getVarValue((Var) args.get(0), bindings);
					IRI tsPred = (IRI) Algebra.getVarValue((Var) args.get(1), bindings);
					Value tsObj = Algebra.getVarValue((Var) args.get(2), bindings);
					Statement tsStmt;
					if (args.size() == 3) {
						tsStmt = vf.createStatement(tsSubj, tsPred, tsObj);
					} else if (args.size() == 4) {
						Resource tsCtx = (Resource) Algebra.getVarValue((Var) args.get(3), bindings);
						tsStmt = vf.createStatement(tsSubj, tsPred, tsObj, tsCtx);
					} else {
						tsStmt = null;
					}
					if (stmt.equals(tsStmt)) {
						Literal ts = (Literal) Algebra.getVarValue(tfc.getResultVars().get(0), bindings);
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
	}

	static final class ModifyInfo {
		private final TupleExpr clause;
		private final List<StatementPattern> stPatterns;
		private final List<TupleFunctionCall> tupleFunctionCalls;

		ModifyInfo(TupleExpr clause, List<StatementPattern> stPatterns, List<TupleFunctionCall> tupleFunctionCalls) {
			this.clause = clause;
			this.stPatterns = stPatterns;
			this.tupleFunctionCalls = tupleFunctionCalls;
		}

		public TupleExpr getClause() {
			return clause;
		}

		public List<StatementPattern> getStatementPatterns() {
			return stPatterns;
		}

		public List<TupleFunctionCall> getTupleFunctionCalls() {
			return tupleFunctionCalls;
		}
	}

	static final class InsertCollector extends StatementPatternCollector {
		private final List<TupleFunctionCall> tupleFunctionCalls = new ArrayList<>();

		static ModifyInfo process(TupleExpr clause) {
			InsertCollector insertCollector = new InsertCollector();
			clause.visit(insertCollector);
			return new ModifyInfo(clause, insertCollector.getStatementPatterns(), insertCollector.getTupleFunctionCalls());
		}

		public List<TupleFunctionCall> getTupleFunctionCalls() {
			return tupleFunctionCalls;
		}

		@Override
		public void meetOther(QueryModelNode node) {
			if (node instanceof TupleFunctionCall) {
				meet((TupleFunctionCall) node);
			} else {
				super.meetOther(node);
			}
		}

		public void meet(TupleFunctionCall tfc) {
			tupleFunctionCalls.add(tfc);
		}
	}

	static final class WhereCollector extends AbstractExtendedQueryModelVisitor<RuntimeException> {
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

		@Override
		public void meetOther(QueryModelNode node) {
			if (node instanceof TupleFunctionCall) {
				meet((TupleFunctionCall) node);
			} else {
				super.meetOther(node);
			}
		}

		public void meet(TupleFunctionCall tfc) {
			tupleFunctionCalls.add(tfc);
			if (HALYARD.TIMESTAMP_PROPERTY.stringValue().equals(tfc.getURI())) {
				Algebra.remove(tfc);
			}
		}
	}

}
