/*
 * Copyright 2016 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
 * Inc., Kenilworth, NJ, USA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.msd.gin.halyard.strategy;

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Pattern;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.net.ParsedIRI;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
//if URI is removed from RDF4J it should be replaceable with org.eclipse.rdf4j.model.IRI
import org.eclipse.rdf4j.model.URI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;
import org.eclipse.rdf4j.model.impl.BooleanLiteral;
import org.eclipse.rdf4j.model.util.URIUtil;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.And;
import org.eclipse.rdf4j.query.algebra.BNodeGenerator;
import org.eclipse.rdf4j.query.algebra.Bound;
import org.eclipse.rdf4j.query.algebra.Coalesce;
import org.eclipse.rdf4j.query.algebra.Compare;
import org.eclipse.rdf4j.query.algebra.CompareAll;
import org.eclipse.rdf4j.query.algebra.CompareAny;
import org.eclipse.rdf4j.query.algebra.Datatype;
import org.eclipse.rdf4j.query.algebra.Exists;
import org.eclipse.rdf4j.query.algebra.FunctionCall;
import org.eclipse.rdf4j.query.algebra.IRIFunction;
import org.eclipse.rdf4j.query.algebra.If;
import org.eclipse.rdf4j.query.algebra.In;
import org.eclipse.rdf4j.query.algebra.IsBNode;
import org.eclipse.rdf4j.query.algebra.IsLiteral;
import org.eclipse.rdf4j.query.algebra.IsNumeric;
import org.eclipse.rdf4j.query.algebra.IsResource;
import org.eclipse.rdf4j.query.algebra.IsURI;
import org.eclipse.rdf4j.query.algebra.Label;
import org.eclipse.rdf4j.query.algebra.Lang;
import org.eclipse.rdf4j.query.algebra.LangMatches;
import org.eclipse.rdf4j.query.algebra.Like;
import org.eclipse.rdf4j.query.algebra.ListMemberOperator;
import org.eclipse.rdf4j.query.algebra.LocalName;
import org.eclipse.rdf4j.query.algebra.MathExpr;
import org.eclipse.rdf4j.query.algebra.Namespace;
import org.eclipse.rdf4j.query.algebra.Not;
import org.eclipse.rdf4j.query.algebra.Or;
import org.eclipse.rdf4j.query.algebra.Regex;
import org.eclipse.rdf4j.query.algebra.SameTerm;
import org.eclipse.rdf4j.query.algebra.Str;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.eclipse.rdf4j.query.algebra.evaluation.function.FunctionRegistry;
import org.eclipse.rdf4j.query.algebra.evaluation.function.datetime.Now;
import org.eclipse.rdf4j.query.algebra.evaluation.util.MathUtil;
import org.eclipse.rdf4j.query.algebra.evaluation.util.QueryEvaluationUtil;

/**
 * Evaluates "value" expressions (low level language functions and operators, instances of {@link ValueExpr}) from SPARQL such as 'Regex', 'IsURI', math expressions etc.
 *
 * @author Adam Sotona (MSD)
 */
class HalyardValueExprEvaluation {

    private final HalyardEvaluationStrategy parentStrategy;
    private final ValueFactory valueFactory;

    HalyardValueExprEvaluation(HalyardEvaluationStrategy parentStrategy, ValueFactory valueFactory) {
        this.parentStrategy = parentStrategy;
        this.valueFactory = valueFactory;
    }

    /**
     * Determines the "effective boolean value" of the {@link Value} returned by evaluating the expression.
     * See {@link QueryEvaluationUtil#getEffectiveBooleanValue(Value)} for the definition of "effective boolean value.
     * @param expr
     * @param bindings the set of named value bindings
     * @return
     * @throws QueryEvaluationException
     */
    boolean isTrue(ValueExpr expr, BindingSet bindings) throws QueryEvaluationException {
        try {
            Value value = evaluate(expr, bindings);
            return QueryEvaluationUtil.getEffectiveBooleanValue(value);
        } catch (ValueExprEvaluationException e) {
            return false;
        }
    }

    /**
     * Determines which evaluate method to call based on the type of {@link ValueExpr}
     * @param expr the expression to evaluate
     * @param bindings the set of named value bindings the set of named value bindings
     * @return the {@link Value} resulting from the evaluation
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    Value evaluate(ValueExpr expr, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        if (expr instanceof Var) {
            return evaluate((Var) expr, bindings);
        } else if (expr instanceof ValueConstant) {
            return evaluate((ValueConstant) expr, bindings);
        } else if (expr instanceof BNodeGenerator) {
            return evaluate((BNodeGenerator) expr, bindings);
        } else if (expr instanceof Bound) {
            return evaluate((Bound) expr, bindings);
        } else if (expr instanceof Str) {
            return evaluate((Str) expr, bindings);
        } else if (expr instanceof Label) {
            return evaluate((Label) expr, bindings);
        } else if (expr instanceof Lang) {
            return evaluate((Lang) expr, bindings);
        } else if (expr instanceof LangMatches) {
            return evaluate((LangMatches) expr, bindings);
        } else if (expr instanceof Datatype) {
            return evaluate((Datatype) expr, bindings);
        } else if (expr instanceof Namespace) {
            return evaluate((Namespace) expr, bindings);
        } else if (expr instanceof LocalName) {
            return evaluate((LocalName) expr, bindings);
        } else if (expr instanceof IsResource) {
            return evaluate((IsResource) expr, bindings);
        } else if (expr instanceof IsURI) {
            return evaluate((IsURI) expr, bindings);
        } else if (expr instanceof IsBNode) {
            return evaluate((IsBNode) expr, bindings);
        } else if (expr instanceof IsLiteral) {
            return evaluate((IsLiteral) expr, bindings);
        } else if (expr instanceof IsNumeric) {
            return evaluate((IsNumeric) expr, bindings);
        } else if (expr instanceof IRIFunction) {
            return evaluate((IRIFunction) expr, bindings);
        } else if (expr instanceof Regex) {
            return evaluate((Regex) expr, bindings);
        } else if (expr instanceof Coalesce) {
            return evaluate((Coalesce) expr, bindings);
        } else if (expr instanceof Like) {
            return evaluate((Like) expr, bindings);
        } else if (expr instanceof FunctionCall) {
            return evaluate((FunctionCall) expr, bindings);
        } else if (expr instanceof And) {
            return evaluate((And) expr, bindings);
        } else if (expr instanceof Or) {
            return evaluate((Or) expr, bindings);
        } else if (expr instanceof Not) {
            return evaluate((Not) expr, bindings);
        } else if (expr instanceof SameTerm) {
            return evaluate((SameTerm) expr, bindings);
        } else if (expr instanceof Compare) {
            return evaluate((Compare) expr, bindings);
        } else if (expr instanceof MathExpr) {
            return evaluate((MathExpr) expr, bindings);
        } else if (expr instanceof In) {
            return evaluate((In) expr, bindings);
        } else if (expr instanceof CompareAny) {
            return evaluate((CompareAny) expr, bindings);
        } else if (expr instanceof CompareAll) {
            return evaluate((CompareAll) expr, bindings);
        } else if (expr instanceof Exists) {
            return evaluate((Exists) expr, bindings);
        } else if (expr instanceof If) {
            return evaluate((If) expr, bindings);
        } else if (expr instanceof ListMemberOperator) {
            return evaluate((ListMemberOperator) expr, bindings);
        } else if (expr == null) {
            throw new IllegalArgumentException("expr must not be null");
        } else {
            throw new QueryEvaluationException("Unsupported value expr type: " + expr.getClass());
        }
    }

    /**
     * Evaluate a {@link Var} query model node.
     * @param var
     * @param bindings the set of named value bindings
     * @return the result of {@link Var#getValue()} from either {@code var}, or if {@code null}, from the {@ bindings}
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private Value evaluate(Var var, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        Value value = var.getValue();
        if (value == null) {
            value = bindings.getValue(var.getName());
        }
        if (value == null) {
            throw new ValueExprEvaluationException();
        }
        return value;
    }

    /**
     * Evaluate a {@link ValueConstant} query model node.
     * @param valueConstant
     * @param bindings the set of named value bindings
     * @return the {@link Value} of {@code valueConstant}
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private Value evaluate(ValueConstant valueConstant, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        return valueConstant.getValue();
    }

    /**
     * Evaluate a {@link BNodeGenerator} node
     * @param node the node to evaluate
     * @param bindings the set of named value bindings
     * @return the value of the evaluation
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private Value evaluate(BNodeGenerator node, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        ValueExpr nodeIdExpr = node.getNodeIdExpr();
        if (nodeIdExpr != null) {
            Value nodeId = evaluate(nodeIdExpr, bindings);
            if (nodeId instanceof Literal) {
                String nodeLabel = ((Literal) nodeId).getLabel() + (bindings.toString().hashCode());
                return valueFactory.createBNode(nodeLabel);
            } else {
                throw new ValueExprEvaluationException("BNODE function argument must be a literal");
            }
        }
        return valueFactory.createBNode();
    }

    /**
     * Evaluate a {@link Bound} node
     * @param node the node to evaluate
     * @param bindings the set of named value bindings
     * @return {@link BooleanLiteral#TRUE} if the node can be evaluated or {@link BooleanLiteral#FALSE} if an exception occurs
     * @throws QueryEvaluationException
     */
    private Value evaluate(Bound node, BindingSet bindings) throws QueryEvaluationException {
        try {
            evaluate(node.getArg(), bindings);
            return BooleanLiteral.TRUE;
        } catch (ValueExprEvaluationException e) {
            return BooleanLiteral.FALSE;
        }
    }

    /**
     * Evaluate a {@link Str} node
     * @param node the node to evaluate
     * @param bindings the set of named value bindings
     * @return a literal representation of the evaluation: a URI, the value of a simple literal or the label of any other literal
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private Value evaluate(Str node, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        Value argValue = evaluate(node.getArg(), bindings);
        if (argValue instanceof URI) {
            return valueFactory.createLiteral(argValue.toString());
        } else if (argValue instanceof Literal) {
            Literal literal = (Literal) argValue;
            if (QueryEvaluationUtil.isSimpleLiteral(literal)) {
                return literal;
            } else {
                return valueFactory.createLiteral(literal.getLabel());
            }
        } else {
            throw new ValueExprEvaluationException();
        }
    }

    /**
     * Evaluate a {@link Label} node
     * @param node the node to evaluate
     * @param bindings the set of named value bindings
     * @return the {@link Literal} resulting from the evaluation of the argument of the {@code Label}.
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private Value evaluate(Label node, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        // FIXME: deprecate Label in favour of Str(?)
        Value argValue = evaluate(node.getArg(), bindings);
        if (argValue instanceof Literal) {
            Literal literal = (Literal) argValue;
            if (QueryEvaluationUtil.isSimpleLiteral(literal)) {
                return literal;
            } else {
                return valueFactory.createLiteral(literal.getLabel());
            }
        } else {
            throw new ValueExprEvaluationException();
        }
    }

    /**
     * Evaluate a {@link Lang} node
     * @param node the node to evaluate
     * @param bindings the set of named value bindings
     * @return a {@link Literal} of the language tag of the {@code Literal} returned by evaluating the argument of the {@code node} or a
     * {code Literal} representing an empty {@code String} if there is no tag
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private Value evaluate(Lang node, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        Value argValue = evaluate(node.getArg(), bindings);
        if (argValue instanceof Literal) {
            Literal literal = (Literal) argValue;
            Optional<String> langTag = literal.getLanguage();
            if (langTag.isPresent()) {
                return valueFactory.createLiteral(langTag.get());
            }
            return valueFactory.createLiteral("");
        }
        throw new ValueExprEvaluationException();
    }

    /**
     * Evaluate a {@link Datatype} node
     * @param node the node to evaluate
     * @param bindings the set of named value bindings
     * @return a {@link Literal} representing the evaluation of the argument of the {@link Datatype}.
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private Value evaluate(Datatype node, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        Value v = evaluate(node.getArg(), bindings);
        if (v instanceof Literal) {
            Literal literal = (Literal) v;
            if (literal.getDatatype() != null) {
                // literal with datatype
                return literal.getDatatype();
            } else if (literal.getLanguage() != null) {
                return RDF.LANGSTRING;
            } else {
                // simple literal
                return XMLSchema.STRING;
            }
        }
        throw new ValueExprEvaluationException();
    }

    /**
     * Evaluate a {@link Namespace} node
     * @param node the node to evaluate
     * @param bindings the set of named value bindings
     * @return the {@link Literal} of the URI of {@link URI} returned by evaluating the argument of the {@code node}
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private Value evaluate(Namespace node, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        Value argValue = evaluate(node.getArg(), bindings);
        if (argValue instanceof URI) {
            URI uri = (URI) argValue;
            return valueFactory.createURI(uri.getNamespace());
        } else {
            throw new ValueExprEvaluationException();
        }
    }

    /**
     * Evaluate a LocalName node
     * @param node the node to evaluate
     * @param bindings the set of named value bindings
     * @return the {@link Literal} of the  {@link URI} returned by evaluating the argument of the {@code node}
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private Value evaluate(LocalName node, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        Value argValue = evaluate(node.getArg(), bindings);
        if (argValue instanceof URI) {
            URI uri = (URI) argValue;
            return valueFactory.createLiteral(uri.getLocalName());
        } else {
            throw new ValueExprEvaluationException();
        }
    }

    /**
     * Determines whether the operand (a variable) contains a Resource.
     *
     * @return <tt>true</tt> if the operand contains a Resource, <tt>false</tt>
     * otherwise.
     */
    private Value evaluate(IsResource node, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        Value argValue = evaluate(node.getArg(), bindings);
        return BooleanLiteral.valueOf(argValue instanceof Resource);
    }

    /**
     * Determines whether the operand (a variable) contains a URI.
     *
     * @return <tt>true</tt> if the operand contains a URI, <tt>false</tt>
     * otherwise.
     */
    private Value evaluate(IsURI node, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        Value argValue = evaluate(node.getArg(), bindings);
        return BooleanLiteral.valueOf(argValue instanceof URI);
    }

    /**
     * Determines whether the operand (a variable) contains a BNode.
     *
     * @return <tt>true</tt> if the operand contains a BNode, <tt>false</tt>
     * otherwise.
     */
    private Value evaluate(IsBNode node, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        Value argValue = evaluate(node.getArg(), bindings);
        return BooleanLiteral.valueOf(argValue instanceof BNode);
    }

    /**
     * Determines whether the operand (a variable) contains a Literal.
     *
     * @return <tt>true</tt> if the operand contains a Literal, <tt>false</tt>
     * otherwise.
     */
    private Value evaluate(IsLiteral node, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        Value argValue = evaluate(node.getArg(), bindings);
        return BooleanLiteral.valueOf(argValue instanceof Literal);
    }

    /**
     * Determines whether the operand (a variable) contains a numeric datatyped literal, i.e. a literal with datatype xsd:float, xsd:double, xsd:decimal, or a
     * derived datatype of xsd:decimal.
     *
     * @return <tt>true</tt> if the operand contains a numeric datatyped literal,
     * <tt>false</tt> otherwise.
     */
    private Value evaluate(IsNumeric node, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        Value argValue = evaluate(node.getArg(), bindings);
        if (argValue instanceof Literal) {
            Literal lit = (Literal) argValue;
            IRI datatype = lit.getDatatype();
            return BooleanLiteral.valueOf(XMLDatatypeUtil.isNumericDatatype(datatype));
        } else {
            return BooleanLiteral.FALSE;
        }
    }

    /**
     * Creates a URI from the operand value (a plain literal or a URI).
     *
     * @param node the node to evaluate, represents an invocation of the SPARQL IRI function
     * @param bindings the set of named value bindings used to generate the value that the URI is based on
     * @return a URI generated from the given arguments
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private IRI evaluate(IRIFunction node, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        Value argValue = evaluate(node.getArg(), bindings);
        if (argValue instanceof Literal) {
            final Literal lit = (Literal) argValue;
            String uriString = lit.getLabel();
            final String baseURI = node.getBaseURI();
            try {
                    ParsedIRI iri = ParsedIRI.create(uriString);
                    if (!iri.isAbsolute() && baseURI != null) {
                            // uri string may be a relative reference.
                            uriString = ParsedIRI.create(baseURI).resolve(iri).toString();
                    }
                    else if (!iri.isAbsolute()) {
                            throw new ValueExprEvaluationException("not an absolute IRI reference: " + uriString);
                    }
            }
            catch (IllegalArgumentException e) {
                    throw new ValueExprEvaluationException("not a valid IRI reference: " + uriString);
            }
            IRI result = null;
            try {
                result = valueFactory.createIRI(uriString);
            } catch (IllegalArgumentException e) {
                throw new ValueExprEvaluationException(e.getMessage());
            }
            return result;
        } else if (argValue instanceof IRI) {
            return ((IRI) argValue);
        }
        throw new ValueExprEvaluationException();
    }

    /**
     * Determines whether the two operands match according to the <code>regex</code> operator.
     *
     * @return <tt>true</tt> if the operands match according to the
     * <tt>regex</tt> operator, <tt>false</tt> otherwise.
     */
    private Value evaluate(Regex node, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        Value arg = evaluate(node.getArg(), bindings);
        Value parg = evaluate(node.getPatternArg(), bindings);
        Value farg = null;
        ValueExpr flagsArg = node.getFlagsArg();
        if (flagsArg != null) {
            farg = evaluate(flagsArg, bindings);
        }
        if (QueryEvaluationUtil.isStringLiteral(arg) && QueryEvaluationUtil.isSimpleLiteral(parg)
                && (farg == null || QueryEvaluationUtil.isSimpleLiteral(farg))) {
            String text = ((Literal) arg).getLabel();
            String ptn = ((Literal) parg).getLabel();
            String flags = "";
            if (farg != null) {
                flags = ((Literal) farg).getLabel();
            }
            // TODO should this Pattern be cached?
            int f = 0;
            for (char c : flags.toCharArray()) {
                switch (c) {
                    case 's':
                        f |= Pattern.DOTALL;
                        break;
                    case 'm':
                        f |= Pattern.MULTILINE;
                        break;
                    case 'i':
                        f |= Pattern.CASE_INSENSITIVE;
                        f |= Pattern.UNICODE_CASE;
                        break;
                    case 'x':
                        f |= Pattern.COMMENTS;
                        break;
                    case 'd':
                        f |= Pattern.UNIX_LINES;
                        break;
                    case 'u':
                        f |= Pattern.UNICODE_CASE;
                        break;
                    default:
                        throw new ValueExprEvaluationException(flags);
                }
            }
            Pattern pattern = Pattern.compile(ptn, f);
            boolean result = pattern.matcher(text).find();
            return BooleanLiteral.valueOf(result);
        }
        throw new ValueExprEvaluationException();
    }

    /**
     * Determines whether the language tag or the node matches the language argument of the node
     * @param node the node to evaluate
     * @param bindings the set of named value bindings
     * @return
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private Value evaluate(LangMatches node, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        Value langTagValue = evaluate(node.getLeftArg(), bindings);
        Value langRangeValue = evaluate(node.getRightArg(), bindings);
        if (QueryEvaluationUtil.isSimpleLiteral(langTagValue)
                && QueryEvaluationUtil.isSimpleLiteral(langRangeValue)) {
            String langTag = ((Literal) langTagValue).getLabel();
            String langRange = ((Literal) langRangeValue).getLabel();
            boolean result = false;
            if (langRange.equals("*")) {
                result = langTag.length() > 0;
            } else if (langTag.length() == langRange.length()) {
                result = langTag.equalsIgnoreCase(langRange);
            } else if (langTag.length() > langRange.length()) {
                // check if the range is a prefix of the tag
                String prefix = langTag.substring(0, langRange.length());
                result = prefix.equalsIgnoreCase(langRange) && langTag.charAt(langRange.length()) == '-';
            }
            return BooleanLiteral.valueOf(result);
        }
        throw new ValueExprEvaluationException();
    }

    /**
     * Determines whether the two operands match according to the <code>like</code> operator. The operator is defined as a string comparison with the possible
     * use of an asterisk (*) at the end and/or the start of the second operand to indicate substring matching.
     *
     * @return <tt>true</tt> if the operands match according to the
     * <tt>like</tt>
     * operator, <tt>false</tt> otherwise.
     */
    private Value evaluate(Like node, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        Value val = evaluate(node.getArg(), bindings);
        String strVal = null;
        if (val instanceof URI) {
            strVal = ((URI) val).toString();
        } else if (val instanceof Literal) {
            strVal = ((Literal) val).getLabel();
        }
        if (strVal == null) {
            throw new ValueExprEvaluationException();
        }
        if (!node.isCaseSensitive()) {
            // Convert strVal to lower case, just like the pattern has been done
            strVal = strVal.toLowerCase(Locale.ROOT);
        }
        int valIndex = 0;
        int prevPatternIndex = -1;
        int patternIndex = node.getOpPattern().indexOf('*');
        if (patternIndex == -1) {
            // No wildcards
            return BooleanLiteral.valueOf(node.getOpPattern().equals(strVal));
        }
        String snippet;
        if (patternIndex > 0) {
            // Pattern does not start with a wildcard, first part must match
            snippet = node.getOpPattern().substring(0, patternIndex);
            if (!strVal.startsWith(snippet)) {
                return BooleanLiteral.FALSE;
            }
            valIndex += snippet.length();
            prevPatternIndex = patternIndex;
            patternIndex = node.getOpPattern().indexOf('*', patternIndex + 1);
        }
        while (patternIndex != -1) {
            // Get snippet between previous wildcard and this wildcard
            snippet = node.getOpPattern().substring(prevPatternIndex + 1, patternIndex);
            // Search for the snippet in the value
            valIndex = strVal.indexOf(snippet, valIndex);
            if (valIndex == -1) {
                return BooleanLiteral.FALSE;
            }
            valIndex += snippet.length();
            prevPatternIndex = patternIndex;
            patternIndex = node.getOpPattern().indexOf('*', patternIndex + 1);
        }
        // Part after last wildcard
        snippet = node.getOpPattern().substring(prevPatternIndex + 1);
        if (snippet.length() > 0) {
            // Pattern does not end with a wildcard.
            // Search last occurence of the snippet.
            valIndex = strVal.indexOf(snippet, valIndex);
            int i;
            while ((i = strVal.indexOf(snippet, valIndex + 1)) != -1) {
                // A later occurence was found.
                valIndex = i;
            }
            if (valIndex == -1) {
                return BooleanLiteral.FALSE;
            }
            valIndex += snippet.length();
            if (valIndex < strVal.length()) {
                // Some characters were not matched
                return BooleanLiteral.FALSE;
            }
        }
        return BooleanLiteral.TRUE;
    }

    /**
     * Evaluates a function.
     */
    private Value evaluate(FunctionCall node, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        Optional<Function> function = FunctionRegistry.getInstance().get(node.getURI());
        if (!function.isPresent()) {
            throw new QueryEvaluationException("Unknown function '" + node.getURI() + "'");
        }
        // the NOW function is a special case as it needs to keep a shared return
        // value for the duration of the query.
        if (function.get() instanceof Now) {
            return evaluate((Now) function.get(), bindings);
        }
        List<ValueExpr> args = node.getArgs();
        Value[] argValues = new Value[args.size()];
        for (int i = 0; i < args.size(); i++) {
            argValues[i] = evaluate(args.get(i), bindings);
        }
        return function.get().evaluate(valueFactory, argValues);
    }

    /**
     * Evaluate an {@link And} node
     * @param node the node to evaluate
     * @param bindings the set of named value bindings
     * @return
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private Value evaluate(And node, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        try {
            Value leftValue = evaluate(node.getLeftArg(), bindings);
            if (QueryEvaluationUtil.getEffectiveBooleanValue(leftValue) == false) {
                // Left argument evaluates to false, we don't need to look any
                // further
                return BooleanLiteral.FALSE;
            }
        } catch (ValueExprEvaluationException e) {
            // Failed to evaluate the left argument. Result is 'false' when
            // the right argument evaluates to 'false', failure otherwise.
            Value rightValue = evaluate(node.getRightArg(), bindings);
            if (QueryEvaluationUtil.getEffectiveBooleanValue(rightValue) == false) {
                return BooleanLiteral.FALSE;
            } else {
                throw new ValueExprEvaluationException();
            }
        }
        // Left argument evaluated to 'true', result is determined
        // by the evaluation of the right argument.
        Value rightValue = evaluate(node.getRightArg(), bindings);
        return BooleanLiteral.valueOf(QueryEvaluationUtil.getEffectiveBooleanValue(rightValue));
    }

    /**
     * Evaluate an {@link And} node
     * @param node the node to evaluate
     * @param bindings the set of named value bindings
     * @return
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private Value evaluate(Or node, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        try {
            Value leftValue = evaluate(node.getLeftArg(), bindings);
            if (QueryEvaluationUtil.getEffectiveBooleanValue(leftValue) == true) {
                // Left argument evaluates to true, we don't need to look any
                // further
                return BooleanLiteral.TRUE;
            }
        } catch (ValueExprEvaluationException e) {
            // Failed to evaluate the left argument. Result is 'true' when
            // the right argument evaluates to 'true', failure otherwise.
            Value rightValue = evaluate(node.getRightArg(), bindings);
            if (QueryEvaluationUtil.getEffectiveBooleanValue(rightValue) == true) {
                return BooleanLiteral.TRUE;
            } else {
                throw new ValueExprEvaluationException();
            }
        }
        // Left argument evaluated to 'false', result is determined
        // by the evaluation of the right argument.
        Value rightValue = evaluate(node.getRightArg(), bindings);
        return BooleanLiteral.valueOf(QueryEvaluationUtil.getEffectiveBooleanValue(rightValue));
    }

    /**
     * Evaluate a {@link Not} node
     * @param node the node to evaluate
     * @param bindings the set of named value bindings
     * @return
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private Value evaluate(Not node, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        Value argValue = evaluate(node.getArg(), bindings);
        boolean argBoolean = QueryEvaluationUtil.getEffectiveBooleanValue(argValue);
        return BooleanLiteral.valueOf(!argBoolean);
    }

    /**
     * Evaluate a {@link Now} node. the value of 'now' is shared across the whole query and evaluation strategy
     * @param node the node to evaluate
     * @param bindings the set of named value bindings
     * @return
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private Value evaluate(Now node, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        if (parentStrategy.sharedValueOfNow == null) {
            parentStrategy.sharedValueOfNow = node.evaluate(valueFactory);
        }
        return parentStrategy.sharedValueOfNow;
    }

    /**
     * Evaluate if the left and right arguments of the {@link SameTerm} node are equal
     * @param node the node to evaluate
     * @param bindings the set of named value bindings
     * @return
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private Value evaluate(SameTerm node, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        Value leftVal = evaluate(node.getLeftArg(), bindings);
        Value rightVal = evaluate(node.getRightArg(), bindings);
        return BooleanLiteral.valueOf(leftVal != null && leftVal.equals(rightVal));
    }

    /**
     * Evaluate a {@link Coalesce} node
     * @param node the node to evaluate
     * @param bindings the set of named value bindings
     * @return the first {@link Value} that doesn't produce an error on evaluation
     * @throws ValueExprEvaluationException
     */
    private Value evaluate(Coalesce node, BindingSet bindings) throws ValueExprEvaluationException {
        for (ValueExpr expr : node.getArguments()) {
            try {
                Value result = evaluate(expr, bindings);
                if (result != null) return result;
                // return first result that does not produce an error on evaluation.
            } catch (QueryEvaluationException ignore) {
            }
        }
        throw new ValueExprEvaluationException("COALESCE arguments do not evaluate to a value: " + node.getSignature());
    }

    /**
     * Evaluates a Compare node
     * @param node the node to evaluate
     * @param bindings the set of named value bindings
     * @return the {@link Value} resulting from the comparison of the left and right arguments of the {@code node} using the comparison operator
     * of the {@code node}.
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private Value evaluate(Compare node, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        Value leftVal = evaluate(node.getLeftArg(), bindings);
        Value rightVal = evaluate(node.getRightArg(), bindings);
        return BooleanLiteral.valueOf(QueryEvaluationUtil.compare(leftVal, rightVal, node.getOperator()));
    }

    /**
     * Evaluate a {@link MathExpr}
     * @param node the node to evaluate
     * @param bindings the set of named value bindings
     * @return the {@link Value} of the math operation on the {@link Value}s return from evaluating the left and right arguments of the node
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private Value evaluate(MathExpr node, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        // Do the math
        Value leftVal = evaluate(node.getLeftArg(), bindings);
        Value rightVal = evaluate(node.getRightArg(), bindings);
        if (leftVal instanceof Literal && rightVal instanceof Literal) {
            return MathUtil.compute((Literal) leftVal, (Literal) rightVal, node.getOperator());
        }
        throw new ValueExprEvaluationException("Both arguments must be numeric literals");
    }

    /**
     * Evaluate an {@link If} node
     * @param node the node to evaluate
     * @param bindings the set of named value bindings
     * @return
     * @throws QueryEvaluationException
     */
    private Value evaluate(If node, BindingSet bindings) throws QueryEvaluationException {
        Value result;
        boolean conditionIsTrue;
        try {
            Value value = evaluate(node.getCondition(), bindings);
            conditionIsTrue = QueryEvaluationUtil.getEffectiveBooleanValue(value);
        } catch (ValueExprEvaluationException e) {
            // in case of type error, if-construction should result in empty
            // binding.
            return null;
        }
        if (conditionIsTrue) {
            result = evaluate(node.getResult(), bindings);
        } else {
            result = evaluate(node.getAlternative(), bindings);
        }
        return result;
    }

    /**
     * Evaluate an {@link In} node
     * @param node the node to evaluate
     * @param bindings the set of named value bindings
     * @return
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private Value evaluate(In node, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        Value leftValue = evaluate(node.getArg(), bindings);
        // Result is false until a match has been found
        boolean result = false;
        // Use first binding name from tuple expr to compare values
        String bindingName = node.getSubQuery().getBindingNames().iterator().next();
        try (CloseableIteration<BindingSet, QueryEvaluationException> iter = parentStrategy.evaluate(node.getSubQuery(), bindings)) {
            while (result == false && iter.hasNext()) {
                BindingSet bindingSet = iter.next();
                Value rightValue = bindingSet.getValue(bindingName);
                result = leftValue == null && rightValue == null || leftValue != null
                        && leftValue.equals(rightValue);
            }
        }
        return BooleanLiteral.valueOf(result);
    }

    /**
     * Evaluate a {@link ListMemberOperator}
     * @param node the node to evaluate
     * @param bindings the set of named value bindings
     * @return
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private Value evaluate(ListMemberOperator node, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        List<ValueExpr> args = node.getArguments();
        Value leftValue = evaluate(args.get(0), bindings);
        boolean result = false;
        ValueExprEvaluationException typeError = null;
        for (int i = 1; i < args.size(); i++) {
            ValueExpr arg = args.get(i);
            try {
                Value rightValue = evaluate(arg, bindings);
                result = leftValue == null && rightValue == null;
                if (!result) {
                    result = QueryEvaluationUtil.compare(leftValue, rightValue, Compare.CompareOp.EQ);
                }
                if (result) {
                    break;
                }
            } catch (ValueExprEvaluationException caught) {
                typeError = caught;
            }
        }
        if (typeError != null && !result) {
            // cf. SPARQL spec a type error is thrown if the value is not in the
            // list and one of the list members caused a type error in the
            // comparison.
            throw typeError;
        }
        return BooleanLiteral.valueOf(result);
    }

    /**
     * Evaluate a {@link CompareAny} node
     * @param node the node to evaluate
     * @param bindings the set of named value bindings
     * @return
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private Value evaluate(CompareAny node, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        Value leftValue = evaluate(node.getArg(), bindings);
        // Result is false until a match has been found
        boolean result = false;
        // Use first binding name from tuple expr to compare values
        String bindingName = node.getSubQuery().getBindingNames().iterator().next();
        try (CloseableIteration<BindingSet, QueryEvaluationException> iter = parentStrategy.evaluate(node.getSubQuery(), bindings)) {
            while (result == false && iter.hasNext()) {
                BindingSet bindingSet = iter.next();
                Value rightValue = bindingSet.getValue(bindingName);
                try {
                    result = QueryEvaluationUtil.compare(leftValue, rightValue, node.getOperator());
                } catch (ValueExprEvaluationException e) {
                    // ignore, maybe next value will match
                }
            }
        }
        return BooleanLiteral.valueOf(result);
    }

    /**
     * Evaluate a {@link CompareAll} node
     * @param node the node to evaluate
     * @param bindings the set of named value bindings
     * @return
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private Value evaluate(CompareAll node, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        Value leftValue = evaluate(node.getArg(), bindings);
        // Result is true until a mismatch has been found
        boolean result = true;
        // Use first binding name from tuple expr to compare values
        String bindingName = node.getSubQuery().getBindingNames().iterator().next();
        try (CloseableIteration<BindingSet, QueryEvaluationException> iter = parentStrategy.evaluate(node.getSubQuery(), bindings)) {
            while (result == true && iter.hasNext()) {
                BindingSet bindingSet = iter.next();
                Value rightValue = bindingSet.getValue(bindingName);
                try {
                    result = QueryEvaluationUtil.compare(leftValue, rightValue, node.getOperator());
                } catch (ValueExprEvaluationException e) {
                    // Exception thrown by ValueCompare.isTrue(...)
                    result = false;
                }
            }
        }
        return BooleanLiteral.valueOf(result);
    }

    /**
     * Evaluate a {@link Exists} node
     * @param node the node to evaluate
     * @param bindings the set of named value bindings
     * @return
     * @throws ValueExprEvaluationException
     * @throws QueryEvaluationException
     */
    private Value evaluate(Exists node, BindingSet bindings) throws ValueExprEvaluationException, QueryEvaluationException {
        try (CloseableIteration<BindingSet, QueryEvaluationException> iter = parentStrategy.evaluate(node.getSubQuery(), bindings)) {
            return BooleanLiteral.valueOf(iter.hasNext());
        }
    }
}
