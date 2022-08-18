package com.msd.gin.halyard.common;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.msd.gin.halyard.vocab.HALYARD;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class RDFFactory {
	private static final Logger LOGGER = LoggerFactory.getLogger(RDFFactory.class);
	private static final int MIN_KEY_SIZE = 1;
	private static final Map<HalyardConfiguration,RDFFactory> FACTORIES = Collections.synchronizedMap(new HashMap<>());

	public final ValueIO.Writer idTripleWriter;
	public final ValueIO.Writer streamWriter;
	public final ValueIO.Reader streamReader;
	private final BiMap<ValueIdentifier, IRI> wellKnownIriIds = HashBiMap.create(256);
	private final ValueIdentifier.Format idFormat;
	final int typeSaltSize;

	private final ValueIO valueIO;

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
		HalyardConfiguration halyardConfig = new HalyardConfiguration(config);
		return FACTORIES.computeIfAbsent(halyardConfig, c -> new RDFFactory(halyardConfig));
	}

	public static RDFFactory create(KeyspaceConnection conn) throws IOException {
		Get getConfig = new Get(HalyardTableUtils.CONFIG_ROW_KEY)
				.addColumn(HalyardTableUtils.CF_NAME, HalyardTableUtils.CONFIG_COL);
		Result res = conn.get(getConfig);
		if (res == null) {
			throw new IOException("No config found");
		}
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

	private RDFFactory(HalyardConfiguration halyardConfig) {
		valueIO = new ValueIO(
			halyardConfig.getBoolean(Config.VOCAB),
			halyardConfig.getBoolean(Config.LANG),
			halyardConfig.getInteger(Config.STRING_COMPRESSION)
		);
		String confIdAlgo = halyardConfig.getString(Config.ID_HASH);
		int confIdSize = halyardConfig.getInteger(Config.ID_SIZE);
		int idSize = Hashes.getHash(confIdAlgo, confIdSize).size();
		LOGGER.info("Identifier hash: {} {}-bit ({} bytes)", confIdAlgo, idSize*Byte.SIZE, idSize);

		int typeIndex = lessThan(lessThanOrEqual(halyardConfig.getInteger(Config.ID_TYPE_INDEX), Short.BYTES), idSize);
		ValueIdentifier.TypeNibble typeNibble = halyardConfig.getBoolean(Config.ID_TYPE_NIBBLE) ? ValueIdentifier.TypeNibble.LITTLE_NIBBLE : ValueIdentifier.TypeNibble.BIG_NIBBLE;
		idFormat = new ValueIdentifier.Format(confIdAlgo, idSize, typeIndex, typeNibble);
		typeSaltSize = idFormat.getSaltSize();

		int subjectKeySize = lessThanOrEqual(greaterThanOrEqual(halyardConfig.getInteger(Config.KEY_SIZE_SUBJECT), MIN_KEY_SIZE), idSize);
		int subjectEndKeySize = lessThanOrEqual(greaterThanOrEqual(halyardConfig.getInteger(Config.END_KEY_SIZE_SUBJECT), MIN_KEY_SIZE), idSize);
		int predicateKeySize = lessThanOrEqual(greaterThanOrEqual(halyardConfig.getInteger(Config.KEY_SIZE_PREDICATE), MIN_KEY_SIZE), idSize);
		int predicateEndKeySize = lessThanOrEqual(greaterThanOrEqual(halyardConfig.getInteger(Config.END_KEY_SIZE_PREDICATE), MIN_KEY_SIZE), idSize);
		int objectKeySize = lessThanOrEqual(greaterThanOrEqual(halyardConfig.getInteger(Config.KEY_SIZE_OBJECT), MIN_KEY_SIZE), idSize);
		int objectEndKeySize = lessThanOrEqual(greaterThanOrEqual(halyardConfig.getInteger(Config.END_KEY_SIZE_OBJECT), MIN_KEY_SIZE), idSize);
		int contextKeySize = lessThanOrEqual(greaterThanOrEqual(halyardConfig.getInteger(Config.KEY_SIZE_CONTEXT), MIN_KEY_SIZE), idSize);
		int contextEndKeySize = lessThanOrEqual(greaterThanOrEqual(halyardConfig.getInteger(Config.END_KEY_SIZE_CONTEXT), 0), idSize);

		idTripleWriter = valueIO.createWriter(new IdTripleWriter());
		streamWriter = valueIO.createStreamWriter();
		streamReader = valueIO.createStreamReader(IdValueFactory.INSTANCE);

		for (IRI iri : valueIO.wellKnownIris.values()) {
			IdentifiableIRI idIri = new IdentifiableIRI(iri.stringValue());
			ValueIdentifier id = idIri.getId(this);
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
			contextKeySize, contextEndKeySize,
			0, 0, 0, typeIndex, Short.BYTES
		);

		this.spo = new StatementIndex<>(
			StatementIndex.Name.SPO, 0, false,
			subject, predicate, object, context,
			this
		);
		this.pos = new StatementIndex<>(
			StatementIndex.Name.POS, 1, false,
			predicate, object, subject, context,
			this
		);
		this.osp = new StatementIndex<>(
			StatementIndex.Name.OSP, 2, false,
			object, subject, predicate, context,
			this
		);
		this.cspo = new StatementIndex<>(
			StatementIndex.Name.CSPO, 3, true,
			context, subject, predicate, object,
			this
		);
		this.cpos = new StatementIndex<>(
			StatementIndex.Name.CPOS, 4, true,
			context, predicate, object, subject,
			this
		);
		this.cosp = new StatementIndex<>(
			StatementIndex.Name.COSP, 5, true,
			context, object, subject, predicate,
			this
		);
	}

	public Collection<Namespace> getWellKnownNamespaces() {
		return valueIO.getWellKnownNamespaces();
	}

	IRI getWellKnownIRI(ValueIdentifier id) {
		return wellKnownIriIds.get(id);
	}

	boolean isWellKnownIRI(Value v) {
		return v.isIRI() && wellKnownIriIds.containsValue(v);
	}

	public int getIdSize() {
		return idFormat.size;
	}

	public String getIdAlgorithm() {
		return idFormat.algorithm;
	}

	ByteSequence writeSaltAndType(final int salt, ValueType type, IRI datatype, ByteSequence seq) {
		if (salt >= typeSaltSize) {
			throw new IllegalArgumentException(String.format("Salt must be between 0 (inclusive) and %d (exclusive): %d", typeSaltSize, salt));
		}
		return new ByteSequence() {
			@Override
			public ByteBuffer writeTo(ByteBuffer bb) {
				int saltBits = salt;
				byte[] arr = bb.array();
				int offset = bb.arrayOffset() + bb.position();
				seq.writeTo(bb);
				// overwrite type bits
				idFormat.writeType(type, datatype, arr, offset);
				if (idFormat.typeNibble == ValueIdentifier.TypeNibble.LITTLE_NIBBLE) {
					arr[offset+idFormat.typeIndex] = (byte) ((arr[offset+idFormat.typeIndex] & 0x0F) | ((saltBits&0x0F) << 4));
					saltBits >>= 4;
				}
				for (int i=idFormat.typeIndex-1; i>=0; i--) {
					arr[offset+i] = (byte) saltBits;
					saltBits >>= 8;
				}
				return bb;
			}

			@Override
			public int size() {
				return seq.size();
			}
		};
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

	public ValueIdentifier id(Value v) {
		ValueIdentifier id = v.isIRI() ? wellKnownIriIds.inverse().get(v) : null;
		if (id == null) {
			if (v instanceof IdentifiableValue) {
				IdentifiableValue idv = (IdentifiableValue) v;
				id = idv.getId(this);
			} else {
				ByteBuffer ser = ByteBuffer.allocate(ValueIO.DEFAULT_BUFFER_SIZE);
				ser = idTripleWriter.writeTo(v, ser);
				ser.flip();
				id = id(v, ser);
			}
		}
		return id;
	}

	public ValueIdentifier id(byte[] idBytes) {
		if (idBytes.length != idFormat.size) {
			throw new IllegalArgumentException("Byte array has incorrect length");
		}
		return new ValueIdentifier(idBytes, idFormat);
	}

	ValueIdentifier id(Value v, ByteBuffer ser) {
		ValueType type = ValueType.valueOf(v);
		return idFormat.id(type, v.isLiteral() ? ((Literal)v).getDatatype() : null, ser);
	}

	public ValueIdentifier idFromString(String s) {
		return new ValueIdentifier(Hashes.decode(s), idFormat);
	}

	ByteBuffer getSerializedForm(Value v) {
		byte[] b = idTripleWriter.toBytes(v);
		return ByteBuffer.wrap(b).asReadOnlyBuffer();
	}

	public byte[] statementId(Resource subj, IRI pred, Value obj) {
		byte[] id = new byte[3 * idFormat.size];
		ByteBuffer buf = ByteBuffer.wrap(id);
		buf = writeStatementId(subj, pred, obj, buf);
		buf.flip();
		buf.get(id);
		return id;
	}

	public ByteBuffer writeStatementId(Resource subj, IRI pred, Value obj, ByteBuffer buf) {
		buf = ValueIO.ensureCapacity(buf, 3*idFormat.size);
		id(subj).writeTo(buf);
		id(pred).writeTo(buf);
		id(obj).writeTo(buf);
		return buf;
	}

	public RDFIdentifier<SPOC.S> createSubjectId(ValueIdentifier id) {
		return new RDFIdentifier<>(subject, id);
	}

	public RDFIdentifier<SPOC.P> createPredicateId(ValueIdentifier id) {
		return new RDFIdentifier<>(predicate, id);
	}

	public RDFIdentifier<SPOC.O> createObjectId(ValueIdentifier id) {
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

	public ValueIO.Reader createTableReader(ValueFactory vf, KeyspaceConnection conn) {
		return valueIO.createReader(vf, new TableTripleReader(conn));
	}


	private final class IdTripleWriter implements TripleWriter {
		@Override
		public ByteBuffer writeTriple(Resource subj, IRI pred, Value obj, ValueIO.Writer writer, ByteBuffer buf) {
			return writeStatementId(subj, pred, obj, buf);
		}
	}


	private final class TableTripleReader implements TripleReader {
		private final KeyspaceConnection conn;
	
		public TableTripleReader(KeyspaceConnection conn) {
			this.conn = conn;
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
			Scan scan = StatementIndex.scan(skey, pkey, okey, ckey, RDFFactory.this);
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
				Statement stmt = HalyardTableUtils.parseStatement(null, null, null, ckey, cells[0], valueReader, RDFFactory.this);
				return valueReader.getValueFactory().createTriple(stmt.getSubject(), stmt.getPredicate(), stmt.getObject());
			} catch (IOException ioe) {
				throw new RuntimeException(ioe);
			}
		}
	}
}
