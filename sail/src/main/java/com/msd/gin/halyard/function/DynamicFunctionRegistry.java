package com.msd.gin.halyard.function;

import java.math.BigInteger;
import java.net.URI;
import java.time.Duration;
import java.time.Period;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.xml.namespace.QName;
import javax.xml.xpath.XPathFunction;
import javax.xml.xpath.XPathFunctionException;
import javax.xml.xpath.XPathFunctionResolver;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.util.Literals;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.eclipse.rdf4j.query.algebra.evaluation.function.FunctionRegistry;

import net.sf.saxon.Configuration;
import net.sf.saxon.expr.Expression;
import net.sf.saxon.expr.JPConverter;
import net.sf.saxon.expr.PJConverter;
import net.sf.saxon.expr.StaticContext;
import net.sf.saxon.expr.XPathContext;
import net.sf.saxon.functions.FunctionLibrary;
import net.sf.saxon.functions.FunctionLibraryList;
import net.sf.saxon.functions.registry.ConstructorFunctionLibrary;
import net.sf.saxon.om.GroundedValue;
import net.sf.saxon.om.StructuredQName;
import net.sf.saxon.sxpath.IndependentContext;
import net.sf.saxon.trans.SymbolicName;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.value.BigIntegerValue;
import net.sf.saxon.value.DayTimeDurationValue;
import net.sf.saxon.value.DurationValue;
import net.sf.saxon.value.YearMonthDurationValue;

public class DynamicFunctionRegistry extends FunctionRegistry {
	private final FunctionResolver resolver;

	public DynamicFunctionRegistry() {
		this(new XPathSparqlFunctionResolver(new SaxonXPathFunctionResolver()));
	}

	public DynamicFunctionRegistry(FunctionResolver resolver) {
		this.resolver = resolver;
	}

	@Override
	public boolean has(String iri) {
		return get(iri).isPresent();
	}

	@Override
	public Optional<Function> get(String iri) {
		return Optional.ofNullable(services.computeIfAbsent(iri, resolver::resolveFunction));
	}


	public static final class XPathSparqlFunctionResolver implements FunctionResolver {
		private final XPathFunctionResolver resolver;

		XPathSparqlFunctionResolver(XPathFunctionResolver resolver) {
			this.resolver = resolver;
		}

		@Override
		public Function resolveFunction(String iri) {
			int sep = iri.lastIndexOf('#');
			if (sep != -1) {
				QName qname = new QName(iri.substring(0, sep), iri.substring(sep + 1));
				Map<Integer, XPathFunction> arityMap = null;
				for (int i = 0; i < 20; i++) {
					XPathFunction xpathFunc = resolver.resolveFunction(qname, i);
					if (xpathFunc != null) {
						if (arityMap == null) {
							arityMap = new HashMap<>(7);
						}
						arityMap.put(i, xpathFunc);
					}
				}

				if (arityMap != null && !arityMap.isEmpty()) {
					return new XPathSparqlFunction(iri, arityMap);
				}
			}
			return null;
		}
	}

	static final class XPathSparqlFunction implements Function {
		private static final Map<IRI, java.util.function.Function<Literal, Object>> LITERAL_CONVERTERS = new HashMap<>(63);
		private static final java.util.function.Function<Literal, Object> DEFAULT_CONVERTER = Literal::stringValue;
		private final String name;
		private final Map<Integer, XPathFunction> arityMap;

		static {
			LITERAL_CONVERTERS.put(XSD.STRING, Literal::getLabel);
			LITERAL_CONVERTERS.put(XSD.FLOAT, Literal::floatValue);
			LITERAL_CONVERTERS.put(XSD.DOUBLE, Literal::doubleValue);
			LITERAL_CONVERTERS.put(XSD.DECIMAL, Literal::decimalValue);
			LITERAL_CONVERTERS.put(XSD.INTEGER, l -> {
				BigInteger v = l.integerValue();
				if (v.compareTo(BigIntegerValue.MIN_LONG) < 0 || v.compareTo(BigIntegerValue.MAX_LONG) > 0) {
					return v;
				} else {
					long lv = v.longValue();
					if (lv < Integer.MIN_VALUE || lv > Integer.MAX_VALUE) {
						return lv;
					} else {
						return (int) lv;
					}
				}
			});
			LITERAL_CONVERTERS.put(XSD.INT, Literal::intValue);
			LITERAL_CONVERTERS.put(XSD.SHORT, Literal::shortValue);
			LITERAL_CONVERTERS.put(XSD.BYTE, Literal::byteValue);
			LITERAL_CONVERTERS.put(XSD.DATETIME, l -> l.calendarValue().toGregorianCalendar().getTime());
			LITERAL_CONVERTERS.put(XSD.BOOLEAN, Literal::booleanValue);
			LITERAL_CONVERTERS.put(XSD.DAYTIMEDURATION, l -> Duration.parse(l.getLabel()));
			LITERAL_CONVERTERS.put(XSD.YEARMONTHDURATION, l -> Period.parse(l.getLabel()));
			LITERAL_CONVERTERS.put(XSD.DURATION, l -> DurationValue.makeDuration(l.getLabel()));
		}

		XPathSparqlFunction(String name, Map<Integer, XPathFunction> arityMap) {
			this.name = name;
			this.arityMap = arityMap;
		}

		@Override
		public String getURI() {
			return name;
		}

		@Override
		public Value evaluate(ValueFactory vf, Value... args) throws ValueExprEvaluationException {
			XPathFunction f = arityMap.get(args.length);
			if (f == null) {
				throw new ValueExprEvaluationException("Incorrect number of arguments");
			}

			List<Object> xargs = new ArrayList<>(args.length);
			for (Value arg : args) {
				Object xarg;
				if (arg instanceof Literal) {
					Literal l = (Literal) arg;
					IRI dt = l.getDatatype();
					xarg = LITERAL_CONVERTERS.getOrDefault(dt, DEFAULT_CONVERTER).apply(l);
				} else if (arg instanceof IRI) {
					xarg = URI.create(arg.stringValue());
				} else {
					xarg = arg.stringValue();
				}
				xargs.add(xarg);
			}
			try {
				return Literals.createLiteral(vf, f.evaluate(xargs));
			} catch (XPathFunctionException ex) {
				throw new ValueExprEvaluationException(ex);
			}
		}
	}

	static final class SaxonXPathFunctionResolver implements XPathFunctionResolver {
		private final FunctionLibrary lib;
		private final StaticContext ctx;

		SaxonXPathFunctionResolver() {
			Configuration config = new Configuration();
			FunctionLibraryList lib = new FunctionLibraryList();
			lib.addFunctionLibrary(config.getXPath31FunctionSet());
			lib.addFunctionLibrary(config.getBuiltInExtensionLibraryList());
			lib.addFunctionLibrary(new ConstructorFunctionLibrary(config));
			lib.addFunctionLibrary(config.getIntegratedFunctionLibrary());
			this.lib = lib;
			this.ctx = new IndependentContext(config);
		}

		@Override
		public XPathFunction resolveFunction(QName functionName, int arity) {
			SymbolicName.F name = new SymbolicName.F(new StructuredQName("", functionName.getNamespaceURI(), functionName.getLocalPart()), arity);
			return lib.isAvailable(name) ? new SaxonXPathFunction(name, lib, ctx) : null;
		}
	}

	static final class SaxonXPathFunction implements XPathFunction {
		private final SymbolicName.F name;
		private final FunctionLibrary lib;
		private final StaticContext ctx;

		SaxonXPathFunction(SymbolicName.F name, FunctionLibrary lib, StaticContext ctx) {
			this.name = name;
			this.lib = lib;
			this.ctx = ctx;
		}

		@Override
		public Object evaluate(@SuppressWarnings("rawtypes") List args) throws XPathFunctionException {
			XPathContext xctx = ctx.makeEarlyEvaluationContext();
			Expression[] staticArgs = new Expression[args.size()];
			for (int i = 0; i < args.size(); i++) {
				Object arg = args.get(i);
				GroundedValue<?> v;
				if (arg instanceof Period) {
					v = YearMonthDurationValue.fromMonths((int) ((Period) arg).toTotalMonths());
				} else if (arg instanceof Duration) {
					v = DayTimeDurationValue.fromJavaDuration((Duration) arg);
				} else {
					JPConverter converter = JPConverter.allocate(arg.getClass(), null, ctx.getConfiguration());
					try {
						v = converter.convert(arg, xctx).materialize();
					} catch (XPathException ex) {
						throw new XPathFunctionException(ex);
					}
				}
				staticArgs[i] = net.sf.saxon.expr.Literal.makeLiteral(v);
			}
			List<String> reasons = new ArrayList<>(0);
			Expression expr = lib.bind(name, staticArgs, ctx, reasons);
			if (expr == null) {
				throw new XPathFunctionException(String.format("No such function %s: %s", name, reasons));
			}
			try {
				GroundedValue<?> result = expr.iterate(xctx).materialize();
				PJConverter converter = PJConverter.allocate(ctx.getConfiguration(), expr.getItemType(), expr.getCardinality(), Object.class);
				return converter.convert(result, Object.class, xctx);
			} catch (XPathException ex) {
				throw new XPathFunctionException(ex);
			}
		}
	}
}
