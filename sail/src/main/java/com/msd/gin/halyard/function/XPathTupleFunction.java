package com.msd.gin.halyard.function;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.msd.gin.halyard.common.XMLLiteral;
import com.msd.gin.halyard.vocab.HALYARD;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.xml.namespace.QName;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.SingletonIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunction;
import org.eclipse.rdf4j.spin.function.InverseMagicProperty;
import org.kohsuke.MetaInfServices;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.ls.DOMImplementationLS;
import org.w3c.dom.ls.LSSerializer;

@MetaInfServices(TupleFunction.class)
public class XPathTupleFunction implements TupleFunction, InverseMagicProperty {
	private static final Cache<String, XPathExpression> XPATH_CACHE = CacheBuilder.newBuilder().maximumSize(100).expireAfterAccess(10, TimeUnit.SECONDS).concurrencyLevel(1).build();

	private static final ThreadLocal<XPathFactory> XPATH_FACTORY = new ThreadLocal<XPathFactory>() {
		@Override
		protected XPathFactory initialValue() {
			return XPathFactory.newInstance();
		}
	};

	@Override
	public String getURI() {
		return HALYARD.XPATH_PROPERTY.stringValue();
	}

	public CloseableIteration<? extends List<? extends Value>, QueryEvaluationException> evaluate(ValueFactory vf, Value... args) throws ValueExprEvaluationException {
		if (args.length < 2 || args.length > 3) {
			throw new ValueExprEvaluationException(String.format("%s requires 2 or 3 arguments, got %d", getURI(), args.length));
		}

		if (!(args[0] instanceof Literal)) {
			throw new ValueExprEvaluationException("First argument must be an XPath string");
		}
		if (!(args[1] instanceof Literal) || !RDF.XMLLITERAL.equals(((Literal) args[1]).getDatatype())) {
			throw new ValueExprEvaluationException("Second argument must be an XML literal");
		}

		QName returnType;
		if (args.length == 3) {
			if (!(args[2] instanceof IRI)) {
				throw new ValueExprEvaluationException("Third argument must be an XPath data type");
			}
			IRI iri = (IRI) args[2];
			returnType = new QName(iri.getNamespace().substring(0, iri.getNamespace().length() - 1), iri.getLocalName());
		} else {
			returnType = XPathConstants.STRING;
		}

		String query = ((Literal) args[0]).stringValue();
		try {
			Document doc;
			if (args[1] instanceof XMLLiteral) {
				doc = ((XMLLiteral) args[1]).objectValue();
			} else {
				String xml = ((Literal) args[1]).getLabel();
				doc = XMLLiteral.parseXml(xml);
			}
			XPathExpression xpe = XPATH_CACHE.get(query, () -> XPATH_FACTORY.get().newXPath().compile(query));
			Object result = xpe.evaluate(doc, returnType);
			LSSerializer serializer;
			if (result instanceof Node || result instanceof NodeList) {
				DOMImplementationLS ls = ((DOMImplementationLS) doc.getImplementation());
				serializer = ls.createLSSerializer();
				serializer.getDomConfig().setParameter("xml-declaration", false);
			} else {
				serializer = null;
			}

			if (result instanceof Node) {
				String s = serializeNode((Node) result, serializer);
				return new SingletonIteration<>(Collections.singletonList(vf.createLiteral(s)));
			} else if (result instanceof NodeList) {
				NodeList nl = (NodeList) result;
				return new CloseableIteration<List<? extends Value>, QueryEvaluationException>() {
					int pos = 0;

					@Override
					public boolean hasNext() throws QueryEvaluationException {
						return pos < nl.getLength();
					}

					@Override
					public List<? extends Value> next() throws QueryEvaluationException {
						Node n = nl.item(pos++);
						String s = serializeNode(n, serializer);
						return Collections.singletonList(vf.createLiteral(s));
					}

					@Override
					public void remove() throws QueryEvaluationException {
						throw new UnsupportedOperationException();
					}

					@Override
					public void close() throws QueryEvaluationException {
					}

				};
			} else {
				return new SingletonIteration<>(Collections.singletonList(Values.literal(vf, result, true)));
			}
		} catch (Exception e) {
			throw new QueryEvaluationException(e);
		}
	}

	private static String serializeNode(Node n, LSSerializer serializer) {
		if (n instanceof Attr) {
			return ((Attr) n).getNodeValue();
		} else {
			return serializer.writeToString(n);
		}
	}
}
