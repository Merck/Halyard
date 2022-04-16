package com.msd.gin.halyard.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Table;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;

public class RDFFactory {
	private final IdentifiableValueIO valueIO;
	final RDFRole subject;
	final RDFRole predicate;
	final RDFRole object;
	final RDFRole context;

	public static RDFFactory create() {
		Configuration conf = HBaseConfiguration.create();
		return create(conf);
	}

	public static RDFFactory create(Configuration config) {
		IdentifiableValueIO valueIO = IdentifiableValueIO.create(config);
		return new RDFFactory(valueIO);
	}

	public static RDFFactory create(Table table) throws IOException {
		IdentifiableValueIO valueIO = IdentifiableValueIO.create(table);
		return new RDFFactory(valueIO);
	}

	private RDFFactory(IdentifiableValueIO valueIO) {
		this.valueIO = valueIO;
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

	public IdentifiableValueIO getValueIO() {
		return valueIO;
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
		return RDFSubject.create(subject, val, valueIO);
	}

	public RDFPredicate createPredicate(IRI val) {
		return RDFPredicate.create(predicate, val, valueIO);
	}

	public RDFObject createObject(Value val) {
		return RDFObject.create(object, val, valueIO);
	}

	public RDFContext createContext(Resource val) {
		return RDFContext.create(context, val, valueIO);
	}
}
