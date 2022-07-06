package com.msd.gin.halyard.common;

import org.eclipse.rdf4j.model.Literal;
import org.junit.jupiter.api.Test;

public class XMLLiteralTest extends AbstractCustomLiteralTest {
	@Override
	protected Literal createLiteral() throws Exception {
		String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><test attr=\"foo\">bar</test>";
		return new XMLLiteral(xml);
	}

	@Override
	protected Literal createOtherLiteral() throws Exception {
		String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><test>foobar</test>";
		return new XMLLiteral(xml);
	}

	@Test
	public void testDocument() throws Exception {
		String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><test attr=\"foo\">bar</test>";
		XMLLiteral l = new XMLLiteral(xml);
		l.objectValue();
	}
}
