package com.msd.gin.halyard.function;

import com.msd.gin.halyard.common.XMLLiteral;

import java.util.List;

import javax.xml.xpath.XPathConstants;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.junit.Test;

import static org.junit.Assert.*;

public class XPathTupleFunctionTest {
	@Test
	public void testXPathNodeSet() throws Exception {
		ValueFactory vf = SimpleValueFactory.getInstance();
		String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><test attr=\"foo\"><desc>bar</desc><part id=\"1\"/><part id=\"2\"/></test>";
		XMLLiteral l = new XMLLiteral(xml);
		IRI dt = vf.createIRI(XPathConstants.NODESET.getNamespaceURI() + "#", XPathConstants.NODESET.getLocalPart());
		CloseableIteration<? extends List<? extends Value>, QueryEvaluationException> iter = new XPathTupleFunction().evaluate(vf, vf.createLiteral("/test/part"), l, dt);
		assertTrue(iter.hasNext());
		assertEquals("<part id=\"1\"/>", iter.next().get(0).stringValue());
		assertTrue(iter.hasNext());
		assertEquals("<part id=\"2\"/>", iter.next().get(0).stringValue());
		assertFalse(iter.hasNext());
	}

	@Test
	public void testXPathNodeSetOfAttributes() throws Exception {
		ValueFactory vf = SimpleValueFactory.getInstance();
		String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><test attr=\"foo\"><desc>bar</desc><part id=\"1\"/><part id=\"2\"/></test>";
		XMLLiteral l = new XMLLiteral(xml);
		IRI dt = vf.createIRI(XPathConstants.NODESET.getNamespaceURI() + "#", XPathConstants.NODESET.getLocalPart());
		CloseableIteration<? extends List<? extends Value>, QueryEvaluationException> iter = new XPathTupleFunction().evaluate(vf, vf.createLiteral("/test/part/@id"), l, dt);
		assertTrue(iter.hasNext());
		assertEquals("1", iter.next().get(0).stringValue());
		assertTrue(iter.hasNext());
		assertEquals("2", iter.next().get(0).stringValue());
		assertFalse(iter.hasNext());
	}

	@Test
	public void testXPathNode() throws Exception {
		ValueFactory vf = SimpleValueFactory.getInstance();
		String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><test attr=\"foo\"><desc>bar</desc><part id=\"1\"/><part id=\"2\"/></test>";
		XMLLiteral l = new XMLLiteral(xml);
		IRI dt = vf.createIRI(XPathConstants.NODE.getNamespaceURI() + "#", XPathConstants.NODE.getLocalPart());
		CloseableIteration<? extends List<? extends Value>, QueryEvaluationException> iter = new XPathTupleFunction().evaluate(vf, vf.createLiteral("/test"), l, dt);
		assertTrue(iter.hasNext());
		assertEquals("<test attr=\"foo\"><desc>bar</desc><part id=\"1\"/><part id=\"2\"/></test>", iter.next().get(0).stringValue());
		assertFalse(iter.hasNext());
	}

	@Test
	public void testXPathString() throws Exception {
		ValueFactory vf = SimpleValueFactory.getInstance();
		String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><test attr=\"foo\"><desc>bar</desc><part id=\"1\"/><part id=\"2\"/></test>";
		XMLLiteral l = new XMLLiteral(xml);
		CloseableIteration<? extends List<? extends Value>, QueryEvaluationException> iter = new XPathTupleFunction().evaluate(vf, vf.createLiteral("/test/@attr"), l);
		assertTrue(iter.hasNext());
		assertEquals("foo", iter.next().get(0).stringValue());
		assertFalse(iter.hasNext());
	}

}
