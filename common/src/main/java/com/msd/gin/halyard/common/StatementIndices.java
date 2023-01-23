package com.msd.gin.halyard.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.ValueFactory;

import com.msd.gin.halyard.vocab.HALYARD;
import static com.msd.gin.halyard.common.StatementIndex.VAR_CARDINALITY;

public final class StatementIndices {
	private final int maxCaching;
	private final RDFFactory rdfFactory;
	private final StatementIndex<SPOC.S,SPOC.P,SPOC.O,SPOC.C> spo;
	private final StatementIndex<SPOC.P,SPOC.O,SPOC.S,SPOC.C> pos;
	private final StatementIndex<SPOC.O,SPOC.S,SPOC.P,SPOC.C> osp;
	private final StatementIndex<SPOC.C,SPOC.S,SPOC.P,SPOC.O> cspo;
	private final StatementIndex<SPOC.C,SPOC.P,SPOC.O,SPOC.S> cpos;
	private final StatementIndex<SPOC.C,SPOC.O,SPOC.S,SPOC.P> cosp;

	public static StatementIndices create() {
		Configuration conf = HBaseConfiguration.create();
		return new StatementIndices(conf, RDFFactory.create(conf));
	}

	public StatementIndices(Configuration conf, RDFFactory rdfFactory) {
        this.maxCaching = conf.getInt(HConstants.HBASE_CLIENT_SCANNER_CACHING, HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING);
		this.rdfFactory = rdfFactory;

		RDFRole<SPOC.S> subject = rdfFactory.getSubjectRole();
		RDFRole<SPOC.P> predicate = rdfFactory.getPredicateRole();
		RDFRole<SPOC.O> object = rdfFactory.getObjectRole();
		RDFRole<SPOC.C> context = rdfFactory.getContextRole();

		this.spo = new StatementIndex<>(
			StatementIndex.Name.SPO, 0, false,
			subject, predicate, object, context,
			rdfFactory, conf
		);
		this.pos = new StatementIndex<>(
			StatementIndex.Name.POS, 1, false,
			predicate, object, subject, context,
			rdfFactory, conf
		);
		this.osp = new StatementIndex<>(
			StatementIndex.Name.OSP, 2, false,
			object, subject, predicate, context,
			rdfFactory, conf
		);
		this.cspo = new StatementIndex<>(
			StatementIndex.Name.CSPO, 3, true,
			context, subject, predicate, object,
			rdfFactory, conf
		);
		this.cpos = new StatementIndex<>(
			StatementIndex.Name.CPOS, 4, true,
			context, predicate, object, subject,
			rdfFactory, conf
		);
		this.cosp = new StatementIndex<>(
			StatementIndex.Name.COSP, 5, true,
			context, object, subject, predicate,
			rdfFactory, conf
		);
	}

	public RDFFactory getRDFFactory() {
		return rdfFactory;
	}

	public StatementIndex<SPOC.S,SPOC.P,SPOC.O,SPOC.C> getSPOIndex() {
		return spo;
	}

	public StatementIndex<SPOC.P,SPOC.O,SPOC.S,SPOC.C> getPOSIndex() {
		return pos;
	}

	public StatementIndex<SPOC.O,SPOC.S,SPOC.P,SPOC.C> getOSPIndex() {
		return osp;
	}

	public StatementIndex<SPOC.C,SPOC.S,SPOC.P,SPOC.O> getCSPOIndex() {
		return cspo;
	}

	public StatementIndex<SPOC.C,SPOC.P,SPOC.O,SPOC.S> getCPOSIndex() {
		return cpos;
	}

	public StatementIndex<SPOC.C,SPOC.O,SPOC.S,SPOC.P> getCOSPIndex() {
		return cosp;
	}

	public StatementIndex<?,?,?,?> toIndex(byte prefix) {
		switch(prefix) {
			case 0: return spo;
			case 1: return pos;
			case 2: return osp;
			case 3: return cspo;
			case 4: return cpos;
			case 5: return cosp;
			default: throw new AssertionError(String.format("Invalid prefix: %s", prefix));
		}
	}

	public Scan scanAll() {
		int cardinality = VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY;
        int rowBatchSize = Math.min(maxCaching, cardinality);
		return HalyardTableUtils.scan(
			spo.concat(false, spo.role1.startKey(), spo.role2.startKey(), spo.get3StartKey(), spo.get4StartKey()),
			cosp.concat(true, cosp.role1.stopKey(), cosp.role2.stopKey(), cosp.get3StopKey(), cosp.role4.endStopKey()),
			rowBatchSize,
			true
		);
	}

	public Scan scanDefaultIndices() {
		int cardinality = VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY;
        int rowBatchSize = Math.min(maxCaching, cardinality);
		return HalyardTableUtils.scan(
			spo.concat(false, spo.role1.startKey(), spo.role2.startKey(), spo.get3StartKey(), spo.get4StartKey()),
			osp.concat(true, osp.role1.stopKey(), osp.role2.stopKey(), osp.get3StopKey(), osp.role4.endStopKey()),
			rowBatchSize,
			true
		);
	}

	public List<Scan> scanContextIndices(Resource graph) {
		List<Scan> scans = new ArrayList<>(3);
		RDFContext ctx = rdfFactory.createContext(graph);
		List<StatementIndex<SPOC.C,?,?,?>> ctxIndices = Arrays.asList(cspo, cpos, cosp);
		for (StatementIndex<SPOC.C,?,?,?> index : ctxIndices) {
			scans.add(index.scan(ctx));
		}
		return scans;
	}

	public Scan scanLiterals(IRI predicate, Resource graph) {
		if (predicate != null && graph != null) {
			return scanPredicateGraphLiterals(predicate, graph);
		} else if (predicate != null && graph == null) {
			return scanPredicateLiterals(predicate);
		} else if (predicate == null && graph != null) {
			return scanGraphLiterals(graph);
		} else {
			return scanAllLiterals();
		}
	}

	private Scan scanAllLiterals() {
		StatementIndex<SPOC.O,SPOC.S,SPOC.P,SPOC.C> index = osp;
		int typeSaltSize = rdfFactory.typeSaltSize;
		if (typeSaltSize == 1) {
			int cardinality = VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY;
			return index.scan(
				rdfFactory.writeSaltAndType(0, ValueType.LITERAL, null, index.role1.startKey()), index.role2.startKey(), index.get3StartKey(), index.get4StartKey(),
				rdfFactory.writeSaltAndType(0, ValueType.LITERAL, null, index.role1.stopKey()), index.role2.stopKey(), index.get3StopKey(), index.role4.endStopKey(),
				cardinality,
				true
			);
		} else {
			return index.scanWithConstraint(new ValueConstraint(ValueType.LITERAL));
		}
	}

	private Scan scanGraphLiterals(Resource graph) {
		RDFContext ctx = rdfFactory.createContext(graph);
		StatementIndex<SPOC.C,SPOC.O,SPOC.S,SPOC.P> index = cosp;
		int typeSaltSize = rdfFactory.typeSaltSize;
		if (typeSaltSize == 1) {
			ByteSequence ctxb = new ByteArray(ctx.getKeyHash(index));
			int cardinality = VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY;
			return index.scan(
				ctxb, rdfFactory.writeSaltAndType(0, ValueType.LITERAL, null, index.role2.startKey()), index.get3StartKey(), index.get4StartKey(),
				ctxb, rdfFactory.writeSaltAndType(0, ValueType.LITERAL, null, index.role2.stopKey()), index.get3StopKey(), index.role4.endStopKey(),
				cardinality,
				true
			);
		} else {
			return index.scanWithConstraint(ctx, new ValueConstraint(ValueType.LITERAL));
		}
	}

	private Scan scanPredicateLiterals(IRI predicate) {
		RDFPredicate pred = rdfFactory.createPredicate(predicate);
		StatementIndex<SPOC.P,SPOC.O,SPOC.S,SPOC.C> index = pos;
		int typeSaltSize = rdfFactory.typeSaltSize;
		if (typeSaltSize == 1) {
			ByteSequence predb = new ByteArray(pred.getKeyHash(index));
			int cardinality = VAR_CARDINALITY*VAR_CARDINALITY*VAR_CARDINALITY;
			return index.scan(
				predb, rdfFactory.writeSaltAndType(0, ValueType.LITERAL, null, index.role2.startKey()), index.get3StartKey(), index.get4StartKey(),
				predb, rdfFactory.writeSaltAndType(0, ValueType.LITERAL, null, index.role2.stopKey()), index.get3StopKey(), index.role4.endStopKey(),
				cardinality,
				true
			);
		} else {
			return index.scanWithConstraint(pred, new ValueConstraint(ValueType.LITERAL));
		}
	}

	private Scan scanPredicateGraphLiterals(IRI predicate, Resource graph) {
		RDFPredicate pred = rdfFactory.createPredicate(predicate);
		RDFContext ctx = rdfFactory.createContext(graph);
		StatementIndex<SPOC.C,SPOC.P,SPOC.O,SPOC.S> index = cpos;
		int typeSaltSize = rdfFactory.typeSaltSize;
		if (typeSaltSize == 1) {
			ByteSequence predb = new ByteArray(pred.getKeyHash(index));
			ByteSequence ctxb = new ByteArray(ctx.getKeyHash(index));
			int cardinality = VAR_CARDINALITY*VAR_CARDINALITY;
			return index.scan(
				ctxb, predb, rdfFactory.writeSaltAndType(0, ValueType.LITERAL, null, index.role3.startKey()), index.get4StartKey(),
				ctxb, predb, rdfFactory.writeSaltAndType(0, ValueType.LITERAL, null, index.role3.stopKey()), index.role4.endStopKey(),
				cardinality,
				true
			);
		} else {
			return index.scanWithConstraint(ctx, pred, new ValueConstraint(ValueType.LITERAL));
		}
	}

	/**
	 * Performs a scan using any suitable index.
	 */
	public Scan scan(@Nonnull RDFIdentifier<? extends SPOC.S> s, @Nonnull RDFIdentifier<? extends SPOC.P> p, @Nonnull RDFIdentifier<? extends SPOC.O> o, @Nullable RDFIdentifier<? extends SPOC.C> c) {
		Scan scan;
		if (c == null) {
			int h = Math.floorMod(Objects.hash(s, p, o), 3);
			switch (h) {
				case 0:
					scan = spo.scan(s, p, o);
					break;
				case 1:
					scan = pos.scan(p, o, s);
					break;
				case 2:
					scan = osp.scan(o, s, p);
					break;
				default:
					throw new AssertionError();
			}
		} else {
			int h = Math.floorMod(Objects.hash(s, p, o, c), 3);
			switch (h) {
				case 0:
					scan = cspo.scan(c, s, p, o);
					break;
				case 1:
					scan = cpos.scan(c, p, o, s);
					break;
				case 2:
					scan = cosp.scan(c, o, s, p);
					break;
				default:
					throw new AssertionError();
			}
			scan = HalyardTableUtils.scanSingle(scan);
		}
		return scan;
	}

	public ValueIO.Reader createTableReader(ValueFactory vf, KeyspaceConnection conn) {
		return rdfFactory.valueIO.createReader(vf, new TableTripleReader(conn));
	}



	private final class TableTripleReader implements TripleReader {
		private final KeyspaceConnection conn;
	
		public TableTripleReader(KeyspaceConnection conn) {
			this.conn = conn;
		}
	
		@Override
		public Triple readTriple(ByteBuffer b, ValueIO.Reader valueReader) {
			int idSize = rdfFactory.getIdSize();
			byte[] sid = new byte[idSize];
			byte[] pid = new byte[idSize];
			byte[] oid = new byte[idSize];
			b.get(sid).get(pid).get(oid);
	
			RDFContext ckey = rdfFactory.createContext(HALYARD.TRIPLE_GRAPH_CONTEXT);
			RDFIdentifier<SPOC.S> skey = rdfFactory.createSubjectId(rdfFactory.id(sid));
			RDFIdentifier<SPOC.P> pkey = rdfFactory.createPredicateId(rdfFactory.id(pid));
			RDFIdentifier<SPOC.O> okey = rdfFactory.createObjectId(rdfFactory.id(oid));
			Scan scan = scan(skey, pkey, okey, ckey);
			try {
				Result result;
				try (ResultScanner scanner = conn.getScanner(scan)) {
					result = scanner.next();
				}
				if (result == null) {
					throw new IOException("Triple not found (no result)");
				}
				if (result.isEmpty()) {
					throw new IOException("Triple not found (no cells)");
				}
				Cell[] cells = result.rawCells();
				Statement stmt = HalyardTableUtils.parseStatement(null, null, null, ckey, cells[0], valueReader, StatementIndices.this);
				return valueReader.getValueFactory().createTriple(stmt.getSubject(), stmt.getPredicate(), stmt.getObject());
			} catch (IOException ioe) {
				throw new RuntimeException(ioe);
			}
		}
	}
}
