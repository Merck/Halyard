package com.msd.gin.halyard.common;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.msd.gin.halyard.common.Hashes.HashFunction;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdentifiableValueIO extends ValueIO {
	private static final Logger LOGGER = LoggerFactory.getLogger(IdentifiableValueIO.class);

	public final ValueIO.Writer ID_TRIPLE_WRITER;
	public final ValueIO.Writer STREAM_WRITER;
	public final ValueIO.Reader STREAM_READER;
	private final BiMap<Identifier, IRI> WELL_KNOWN_IRI_IDS = HashBiMap.create(256);
	private final ThreadLocal<HashFunction> idHash;
	private final int idSize;
	private final int typeIndex;
	private final int typeSaltSize;

	public static IdentifiableValueIO create() {
		Configuration conf = HBaseConfiguration.create();
		return create(conf);
	}

	public static IdentifiableValueIO create(Configuration config) {
		return new IdentifiableValueIO(config);
	}

	public static IdentifiableValueIO create(Table table) throws IOException {
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

	private IdentifiableValueIO(Configuration config) {
		super(
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

		ID_TRIPLE_WRITER = createWriter(new IdTripleWriter());
		STREAM_WRITER = createWriter(new StreamTripleWriter());
		STREAM_READER = createReader(new IdValueFactory(this), new StreamTripleReader());

		for (IRI iri : WELL_KNOWN_IRIS.values()) {
			IdentifiableIRI idIri = (IdentifiableIRI) iri;
			Identifier id = idIri.getId();
			if (WELL_KNOWN_IRI_IDS.putIfAbsent(id, idIri) != null) {
				throw new AssertionError(String.format("Hash collision between %s and %s",
						WELL_KNOWN_IRI_IDS.get(id), idIri));
			}
		}
	}

	@Override
	protected void addIRI(IRI iri) {
		IdentifiableIRI idIri = new IdentifiableIRI(iri.stringValue(), this);
		super.addIRI(idIri);
	}

	Identifier wellKnownId(IRI iri) {
		return WELL_KNOWN_IRI_IDS.inverse().get(iri);
	}

	IRI getWellKnownIRI(Identifier id) {
		return WELL_KNOWN_IRI_IDS.get(id);
	}

	public int getIdSize() {
		return idSize;
	}

	int getTypeSaltSize() {
		return typeSaltSize;
	}

	byte[] hash(ByteBuffer bb) {
		return idHash.get().apply(bb);
	}

	public Identifier id(Value v) {
		if (v instanceof Identifiable) {
			return ((Identifiable) v).getId();
		}

		Identifier id = WELL_KNOWN_IRI_IDS.inverse().get(v);
		if (id != null) {
			return id;
		}

		return Identifier.create(v, typeIndex, this);
	}

	Identifier id(Value v, ByteBuffer ser) {
		return Identifier.create(v, ser, typeIndex, this);
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
		buf = ensureCapacity(buf, 3*idSize);
		id(subj).writeTo(buf);
		id(pred).writeTo(buf);
		id(obj).writeTo(buf);
		return buf;
	}


	private static final class IdTripleWriter implements TripleWriter {
		@Override
		public ByteBuffer writeTriple(Resource subj, IRI pred, Value obj, ValueIO.Writer writer, ByteBuffer buf) {
			return ((IdentifiableValueIO)writer.getValueIO()).writeStatementId(subj, pred, obj, buf);
		}
	}
}
