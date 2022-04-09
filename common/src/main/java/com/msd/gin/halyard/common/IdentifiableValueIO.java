package com.msd.gin.halyard.common;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.msd.gin.halyard.common.Hashes.HashFunction;

import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
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

	public IdentifiableValueIO(Configuration config) {
		super(
			Config.getBoolean(config, "halyard.vocabularies", true),
			Config.getBoolean(config, "halyard.languages", true),
			Config.getInteger(config, "halyard.string.compressionThreshold", 200)
		);
		String confIdAlgo = Config.getString(config, "halyard.id.hash", "SHA-1");
		int confIdSize = Config.getInteger(config, "halyard.id.size", 0);
		LOGGER.info("Identifier hash configuration: {} {}", confIdAlgo, confIdSize);

		idHash = new ThreadLocal<HashFunction>() {
			@Override
			protected HashFunction initialValue() {
				return Hashes.getHash(confIdAlgo, confIdSize);
			}
		};
		idSize = idHash.get().size();
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
