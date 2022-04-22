package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;

public class SPOC<V extends Value> {
	private SPOC() {}
	public static final class S extends SPOC<Resource> {private S() {}};
	public static final class P extends SPOC<IRI> {private P() {}};
	public static final class O extends SPOC<Value> {private O() {}};
	public static final class C extends SPOC<Resource> {private C() {}};
}
