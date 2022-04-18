package com.msd.gin.halyard.common;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.msd.gin.halyard.common.Hashes.HashFunction;
import com.msd.gin.halyard.common.ValueIO.StreamTripleReader;
import com.msd.gin.halyard.common.ValueIO.StreamTripleWriter;
import com.msd.gin.halyard.vocab.HALYARD;

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
import org.apache.hadoop.hbase.util.Bytes;
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
	private final ValueFactory valueFactory;
	private final ValueFactory tsValueFactory;
	final RDFRole subject;
	final RDFRole predicate;
	final RDFRole object;
	final RDFRole context;

	public static RDFFactory create() {
		Configuration conf = HBaseConfiguration.create();
		return create(conf);
	}

	public static RDFFactory create(Configuration config) {
		return new RDFFactory(config);
	}

	public static RDFFactory create(Table table) throws IOException {
		Get getConfig = new Get(HalyardTableUtils.CONFIG_ROW_KEY)
				.addColumn(HalyardTableUtils.CF_NAME, HalyardTableUtils.ID_HASH_CONFIG_COL)
				.addColumn(HalyardTableUtils.CF_NAME, HalyardTableUtils.ID_SIZE_CONFIG_COL);
			Result res = table.get(getConfig);
			Cell[] cells = res.rawCells();
			if (cells == null) {
				throw new IOException("No config found");
			}
			String idAlgo = null;
			int idSize = -1; 
			for (Cell cell : cells) {
				ByteBuffer q = ByteBuffer.wrap(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
				if (ByteBuffer.wrap(HalyardTableUtils.ID_HASH_CONFIG_COL).equals(q)) {
					idAlgo = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
				} else if (ByteBuffer.wrap(HalyardTableUtils.ID_SIZE_CONFIG_COL).equals(q)) {
					idSize = Bytes.toInt(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
				}
			}
			if (idAlgo == null) {
				throw new IOException("Identifier hash missing from table");
			}
			if (idSize == -1) {
				throw new IOException("Identifier size missing from table");
			}
			Configuration conf = table.getConfiguration();

			String confIdAlgo = conf.get(Config.ID_HASH_CONFIG);
			if (confIdAlgo != null && !confIdAlgo.equals(idAlgo)) {
				throw new IOException(String.format("Identifier hash mismatch: table contains %s but configuration says %s", idAlgo, confIdAlgo));
			}
			conf.set(Config.ID_HASH_CONFIG, idAlgo);

			int confIdSize = conf.getInt(Config.ID_SIZE_CONFIG, -1);
			if (confIdSize != -1 && confIdSize != idSize) {
				throw new IOException(String.format("Identifier size mismatch: table contains %s but configuration says %s", idSize, confIdSize));
			}
			conf.setInt(Config.ID_SIZE_CONFIG, idSize);
			return create(conf);
	}

	private RDFFactory(Configuration config) {
		valueIO = new ValueIO(
			Config.getBoolean(config, "halyard.vocabularies", true),
			Config.getBoolean(config, "halyard.languages", true),
			Config.getInteger(config, "halyard.string.compressionThreshold", 200)
		);
		String confIdAlgo = Config.getString(config, Config.ID_HASH_CONFIG, "SHA-1");
		int confIdSize = Config.getInteger(config, Config.ID_SIZE_CONFIG, 0);

		idHash = new ThreadLocal<HashFunction>() {
			@Override
			protected HashFunction initialValue() {
				return Hashes.getHash(confIdAlgo, confIdSize);
			}
		};
		HashFunction hashInstance = idHash.get();
		idSize = hashInstance.size();
		LOGGER.info("Identifier hash: {} {}-bit ({} bytes)", hashInstance.getName(), idSize*Byte.SIZE, idSize);

		typeIndex = (idSize > 1) ? 1 : 0;
		typeSaltSize = 1 << (8*typeIndex);

		valueFactory = new IdValueFactory(this);
		tsValueFactory = new TimestampedValueFactory(this);
		idTripleWriter = valueIO.createWriter(new IdTripleWriter());
		streamWriter = valueIO.createWriter(new StreamTripleWriter());
		streamReader = valueIO.createReader(valueFactory, new StreamTripleReader());

		for (IRI iri : valueIO.wellKnownIris.values()) {
			IdentifiableIRI idIri = new IdentifiableIRI(iri.stringValue(), this);
			Identifier id = idIri.getId();
			if (wellKnownIriIds.putIfAbsent(id, idIri) != null) {
				throw new AssertionError(String.format("Hash collision between %s and %s",
						wellKnownIriIds.get(id), idIri));
			}
		}

		this.subject = new RDFRole(
			RDFSubject.KEY_SIZE,
			RDFSubject.END_KEY_SIZE,
			0, 2, 1
		);
		this.predicate = new RDFRole(
			RDFPredicate.KEY_SIZE,
			RDFPredicate.END_KEY_SIZE,
			1, 0, 2
		);
		this.object = new RDFRole(
			RDFObject.KEY_SIZE,
			RDFObject.END_KEY_SIZE,
			2, 1,
			// NB: preserve type flags (isLiteral) for literal scanning
			0
		);
		this.context = new RDFRole(
			RDFContext.KEY_SIZE,
			-1,
			0, 0, 0
		);
	}

	public ValueFactory getValueFactory() {
		return valueFactory;
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

	public RDFIdentifier createSubjectId(Identifier id) {
		return new RDFIdentifier(subject, id);
	}

	public RDFIdentifier createPredicateId(Identifier id) {
		return new RDFIdentifier(predicate, id);
	}

	public RDFIdentifier createObjectId(Identifier id) {
		return new RDFIdentifier(object, id);
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

	public ValueIO.Reader createTableReader(Table table) {
		return valueIO.createReader(valueFactory, new TableTripleReader(table));
	}

	public ValueIO.Reader createTimestampedTableReader(Table table) {
		return valueIO.createReader(tsValueFactory, new TableTripleReader(table));
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
			RDFIdentifier skey = createSubjectId(id(sid));
			RDFIdentifier pkey = createPredicateId(id(pid));
			RDFIdentifier okey = createObjectId(id(oid));
			Scan scan = StatementIndex.CSPO.scan(ckey, skey, pkey, okey);
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
