package com.msd.gin.halyard.sail.search;

import com.msd.gin.halyard.vocab.HALYARD;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Function.class)
public class EscapeTermFunction implements Function {
	private static final Pattern RESERVED_CHARACTERS = Pattern.compile("[\\<\\>\\+\\-\\=\\!\\(\\)\\{\\}\\[\\]\\^\\\"\\~\\*\\?\\:\\\\\\/]|(\\&\\&)|(\\|\\|)");

	@Override
	public String getURI() {
		return HALYARD.ESCAPE_TERM_FUNCTION.stringValue();
	}

	@Override
	public Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {
		if (args.length != 1) {
			throw new QueryEvaluationException("Missing arguments");
		}

		if (!args[0].isLiteral()) {
			throw new QueryEvaluationException("Invalid value");
		}
		String s = ((Literal) args[0]).stringValue();
		s = s.toLowerCase(Locale.ROOT);
		StringBuilder buf = new StringBuilder(s.length());
		buf.append("(");
		int end = 0;
		Matcher matcher = RESERVED_CHARACTERS.matcher(s);
		while (matcher.find()) {
			int start = matcher.start();
			buf.append(s.substring(end, start));
			end = matcher.end();
			String reserved = s.substring(start, end);
			if (!"<".equals(reserved) && !">".equals(reserved)) {
				buf.append("\\");
				buf.append(reserved);
			}
		}
		buf.append(s.substring(end));
		buf.append(")");
		return valueFactory.createLiteral(buf.toString());
	}

}
