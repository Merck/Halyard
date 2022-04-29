package com.msd.gin.halyard.common;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.msd.gin.halyard.common.Hashes.HashFunction;
import com.msd.gin.halyard.common.ValueIO.StreamTripleReader;
import com.msd.gin.halyard.common.ValueIO.StreamTripleWriter;
import com.msd.gin.halyard.vocab.HALYARD;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RDFFactory {
	private static final Logger LOGGER = LoggerFactory.getLogger(RDFFactory.class);

	public final ValueIO.Writer idTripleWriter;
	public final ValueIO.Writer streamWriter;
	public final ValueIO.Reader streamReader;
	private final BiMap<Identifier, IRI> wellKnownIriIds = HashBiMap.create(256);
	private final ThreadLocal<HashFunction> idHash;
	private final int idSize;
	private final int typeIndex;
	private final int typeSaltSize;

	private final ValueIO valueIO;
	private final ValueFactory idValueFactory;
	private final ValueFactory tsValueFactory;

	final RDFRole<SPOC.S> subject;
	final RDFRole<SPOC.P> predicate;
	final RDFRole<SPOC.O> object;
	final RDFRole<SPOC.C> context;

	final StatementIndex<SPOC.S,SPOC.P,SPOC.O,SPOC.C> spo;
	final StatementIndex<SPOC.P,SPOC.O,SPOC.S,SPOC.C> pos;
	final StatementIndex<SPOC.O,SPOC.S,SPOC.P,SPOC.C> osp;
	final StatementIndex<SPOC.C,SPOC.S,SPOC.P,SPOC.O> cspo;
	final StatementIndex<SPOC.C,SPOC.P,SPOC.O,SPOC.S> cpos;
	final StatementIndex<SPOC.C,SPOC.O,SPOC.S,SPOC.P> cosp;

	public static RDFFactory create() {
		Configuration conf = HBaseConfiguration.create();
		return create(conf);
	}

	public static RDFFactory create(Configuration config) {
		return new RDFFactory(config);
	}

	public static RDFFactory create(Table table) throws IOException {
		Get getConfig = new Get(HalyardTableUtils.CONFIG_ROW_KEY)
				.addColumn(HalyardTableUtils.CF_NAME, HalyardTableUtils.CONFIG_COL);
		Result res = table.get(getConfig);
		Cell[] cells = res.rawCells();
		if (cells == null || cells.length == 0) {
			throw new IOException("No config found");
		}
		Cell cell = cells[0];
		Configuration halyardConf = new Configuration(false);
		ByteArrayInputStream bin = new ByteArrayInputStream(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
		halyardConf.addResource(bin);
		return create(halyardConf);
	}

	private static int lessThan(int x, int upperLimit) {
		if (x >= upperLimit) {
			throw new IllegalArgumentException(String.format("%d must be less than %d", x, upperLimit));
		}
		return x;
	}

	private static int lessThanOrEqual(int x, int upperLimit) {
		if (x > upperLimit) {
			throw new IllegalArgumentException(String.format("%d must be less than or equal to %d", x, upperLimit));
		}
		return x;
	}

	private static int greaterThanOrEqual(int x, int lowerLimit) {
		if (x < lowerLimit) {
			throw new IllegalArgumentException(String.format("%d must be greater than or equal to %d", x, lowerLimit));
		}
		return x;
	}

	private RDFFactory(Configuration config) {
		valueIO = new ValueIO(
			Config.getBoolean(config, Config.VOCAB, true),
			Config.getBoolean(config, Config.LANG, true),
			Config.getInteger(config, Config.STRING_COMPRESSION, 200)
		);
		String confIdAlgo = Config.getString(config, Config.ID_HASH, "SHA-1");
		int confIdSize = Config.getInteger(config, Config.ID_SIZE, 0);

		idHash = new ThreadLocal<HashFunction>() {
			@Override
			protected HashFunction initialValue() {
				return Hashes.getHash(confIdAlgo, confIdSize);
			}
		};
		HashFunction hashInstance = idHash.get();
		idSize = hashInstance.size();
		LOGGER.info("Identifier hash: {} {}-bit ({} bytes)", hashInstance.getName(), idSize*Byte.SIZE, idSize);

		typeIndex = lessThan(lessThanOrEqual(Config.getInteger(config, Config.ID_TYPE_INDEX, 1), Short.BYTES), idSize);
		typeSaltSize = 1 << (8*typeIndex);

		int minKeySize = typeIndex + 2;
		int subjectKeySize = lessThanOrEqual(greaterThanOrEqual(Config.getInteger(config, Config.KEY_SIZE_SUBJECT, 8), minKeySize), idSize);
		int subjectEndKeySize = lessThanOrEqual(greaterThanOrEqual(Config.getInteger(config, Config.END_KEY_SIZE_SUBJECT, 8), minKeySize), idSize);
		int predicateKeySize = lessThanOrEqual(greaterThanOrEqual(Config.getInteger(config, Config.KEY_SIZE_PREDICATE, 4), minKeySize), idSize);
		int predicateEndKeySize = lessThanOrEqual(greaterThanOrEqual(Config.getInteger(config, Config.END_KEY_SIZE_PREDICATE, 2), minKeySize), idSize);
		int objectKeySize = lessThanOrEqual(greaterThanOrEqual(Config.getInteger(config, Config.KEY_SIZE_OBJECT, 8), minKeySize), idSize);
		int objectEndKeySize = lessThanOrEqual(greaterThanOrEqual(Config.getInteger(config, Config.END_KEY_SIZE_OBJECT, 2), minKeySize), idSize);
		int contextKeySize = lessThanOrEqual(greaterThanOrEqual(Config.getInteger(config, Config.KEY_SIZE_CONTEXT, 6), minKeySize), idSize);

		idValueFactory = new IdValueFactory(this);
		tsValueFactory = new TimestampedValueFactory(this);
		idTripleWriter = valueIO.createWriter(new IdTripleWriter());
		streamWriter = valueIO.createWriter(new StreamTripleWriter());
		streamReader = valueIO.createReader(idValueFactory, new StreamTripleReader());

		for (IRI iri : valueIO.wellKnownIris.values()) {
			IdentifiableIRI idIri = new IdentifiableIRI(iri.stringValue(), this);
			Identifier id = idIri.getId();
			if (wellKnownIriIds.putIfAbsent(id, idIri) != null) {
				throw new AssertionError(String.format("Hash collision between %s and %s",
						wellKnownIriIds.get(id), idIri));
			}
		}

		this.subject = new RDFRole<>(
			RDFRole.Name.SUBJECT,
			idSize,
			subjectKeySize, subjectEndKeySize,
			0, 2, 1, typeIndex, Short.BYTES
		);
		this.predicate = new RDFRole<>(
			RDFRole.Name.PREDICATE,
			idSize,
			predicateKeySize, predicateEndKeySize,
			1, 0, 2, typeIndex, Short.BYTES
		);
		this.object = new RDFRole<>(
			RDFRole.Name.OBJECT,
			idSize,
			objectKeySize, objectEndKeySize,
			2, 1, 0, typeIndex, Integer.BYTES
		);
		this.context = new RDFRole<>(
			RDFRole.Name.CONTEXT,
			idSize,
			contextKeySize, -1,
			0, 0, 0, typeIndex, Short.BYTES
		);

		this.spo = new StatementIndex<>(
			StatementIndex.Name.SPO, 0, IndexType.TRIPLE,
			subject, predicate, object, context,
			this
		);
		this.pos = new StatementIndex<>(
			StatementIndex.Name.POS, 1, IndexType.TRIPLE,
			predicate, object, subject, context,
			this
		);
		this.osp = new StatementIndex<>(
			StatementIndex.Name.OSP, 2, IndexType.TRIPLE,
			object, subject, predicate, context,
			this
		);
		this.cspo = new StatementIndex<>(
			StatementIndex.Name.CSPO, 3, IndexType.QUAD,
			context, subject, predicate, object,
			this
		);
		this.cpos = new StatementIndex<>(
			StatementIndex.Name.CPOS, 4, IndexType.QUAD,
			context, predicate, object, subject,
			this
		);
		this.cosp = new StatementIndex<>(
			StatementIndex.Name.COSP, 5, IndexType.QUAD,
			context, object, subject, predicate,
			this
		);
	}

	public ValueFactory getIdValueFactory() {
		return idValueFactory;
	}

	public ValueFactory getTimestampedValueFactory() {
		return tsValueFactory;
	}

	public Collection<Namespace> getWellKnownNamespaces() {
		return valueIO.getWellKnownNamespaces();
	}
	Identifier wellKnownId(IRI iri) {
		return wellKnownIriIds.inverse().get(iri);
	}

	IRI getWellKnownIRI(Identifier id) {
		return wellKnownIriIds.get(id);
	}

	boolean isWellKnownIRI(Value v) {
		return wellKnownIriIds.containsValue(v);
	}

	public int getIdSize() {
		return idSize;
	}

	int getTypeSaltSize() {
		return typeSaltSize;
	}

	byte[] createTypeSalt(int salt, byte typeBits) {
		byte[] b = new byte[typeIndex + 1];
		b[typeIndex] = typeBits;
		for (int i=typeIndex-1; i>=0; i--) {
			b[i] = (byte) salt;
			salt >>= 8;
		}
		return b;
	}
	public RDFRole<SPOC.S> getSubjectRole() {
		return subject;
	}

	public RDFRole<SPOC.P> getPredicateRole() {
		return predicate;
	}

	public RDFRole<SPOC.O> getObjectRole() {
		return object;
	}

	public RDFRole<SPOC.C> getContextRole() {
		return context;
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

	public Identifier id(Value v) {
		if (v instanceof Identifiable) {
			return ((Identifiable) v).getId();
		}

		Identifier id = wellKnownIriIds.inverse().get(v);
		if (id != null) {
			return id;
		}

		ByteBuffer ser = ByteBuffer.allocate(ValueIO.DEFAULT_BUFFER_SIZE);
		ser = idTripleWriter.writeTo(v, ser);
		ser.flip();
		return id(v, ser);
	}

	Identifier id(Value v, ByteBuffer ser) {
		byte[] hash = idHash.get().apply(ser);
		return Identifier.create(v, hash, typeIndex);
	}

	public Identifier id(byte[] idBytes) {
		if (idBytes.length != idSize) {
			throw new IllegalArgumentException("Byte array has incorrect length");
		}
		return new Identifier(idBytes, typeIndex);
	}

	public byte[] statementId(Resource subj, IRI pred, Value obj) {
		byte[] id = new byte[3 * idSize];
		ByteBuffer buf = ByteBuffer.wrap(id);
		buf = writeStatementId(subj, pred, obj, buf);
		buf.flip();
		buf.get(id);
		return id;
	}

	public ByteBuffer writeStatementId(Resource subj, IRI pred, Value obj, ByteBuffer buf) {
		buf = ValueIO.ensureCapacity(buf, 3*idSize);
		id(subj).writeTo(buf);
		id(pred).writeTo(buf);
		id(obj).writeTo(buf);
		return buf;
	}

	public RDFIdentifier<SPOC.S> createSubjectId(Identifier id) {
		return new RDFIdentifier<>(subject, id);
	}

	public RDFIdentifier<SPOC.P> createPredicateId(Identifier id) {
		return new RDFIdentifier<>(predicate, id);
	}

	public RDFIdentifier<SPOC.O> createObjectId(Identifier id) {
		return new RDFIdentifier<>(object, id);
	}

	public RDFSubject createSubject(Resource val) {
		return RDFSubject.create(subject, val, this);
	}

	public RDFPredicate createPredicate(IRI val) {
		return RDFPredicate.create(predicate, val, this);
	}

	public RDFObject createObject(Value val) {
		return RDFObject.create(object, val, this);
	}

	public RDFContext createContext(Resource val) {
		return RDFContext.create(context, val, this);
	}

	ValueIO.Writer createWriter() {
		return valueIO.createWriter(null);
	}

	ValueIO.Reader createReader(ValueFactory vf) {
		return valueIO.createReader(vf, null);
	}

	public ValueIO.Reader createTableReader(ValueFactory vf, Table table) {
		return valueIO.createReader(vf, new TableTripleReader(table));
	}


	private final class IdTripleWriter implements TripleWriter {
		@Override
		public ByteBuffer writeTriple(Resource subj, IRI pred, Value obj, ValueIO.Writer writer, ByteBuffer buf) {
			return writeStatementId(subj, pred, obj, buf);
		}
	}


	private final class TableTripleReader implements TripleReader {
		private final Table table;
	
		public TableTripleReader(Table table) {
			this.table = table;
		}
	
		@Override
		public Triple readTriple(ByteBuffer b, ValueIO.Reader valueReader) {
			int idSize = getIdSize();
			byte[] sid = new byte[idSize];
			byte[] pid = new byte[idSize];
			byte[] oid = new byte[idSize];
			b.get(sid).get(pid).get(oid);
	
			RDFContext ckey = createContext(HALYARD.TRIPLE_GRAPH_CONTEXT);
			RDFIdentifier<SPOC.S> skey = createSubjectId(id(sid));
			RDFIdentifier<SPOC.P> pkey = createPredicateId(id(pid));
			RDFIdentifier<SPOC.O> okey = createObjectId(id(oid));
			Scan scan = cspo.scan(ckey, skey, pkey, okey);
			Get get = new Get(scan.getStartRow())
				.setFilter(new FilterList(scan.getFilter(), new FirstKeyOnlyFilter()));
			get.addFamily(HalyardTableUtils.CF_NAME);
			try {
				get.readVersions(HalyardTableUtils.READ_VERSIONS);
				Result result = table.get(get);
				assert result.rawCells().length == 1;
				Statement stmt = HalyardTableUtils.parseStatement(null, null, null, ckey, result.rawCells()[0], valueReader, RDFFactory.this);
				return valueReader.getValueFactory().createTriple(stmt.getSubject(), stmt.getPredicate(), stmt.getObject());
			} catch (IOException ioe) {
				throw new RuntimeException(ioe);
			}
		}
	}
}
