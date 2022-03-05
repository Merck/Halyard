package com.msd.gin.halyard.vocab;

import com.msd.gin.halyard.common.Vocabulary;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public final class ONTOLEX implements Vocabulary {
    private static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

    public static final String PREFIX = "ontolex";

    public static final String NAMESPACE = "http://www.w3.org/ns/lemon/ontolex#";

    public static final Namespace NS = new SimpleNamespace(PREFIX, NAMESPACE);

    public static final IRI AFFIX = SVF.createIRI(NAMESPACE, "Affix");
    public static final IRI CONCEPT_SET = SVF.createIRI(NAMESPACE, "ConceptSet");
    public static final IRI MULTI_WORD_EXPRESSION = SVF.createIRI(NAMESPACE, "MultiWordExpression");
    public static final IRI WORD = SVF.createIRI(NAMESPACE, "Word");
    public static final IRI FORM = SVF.createIRI(NAMESPACE, "Form");
    public static final IRI LEXICAL_CONCEPT = SVF.createIRI(NAMESPACE, "LexicalConcept");
    public static final IRI LEXICAL_ENTRY = SVF.createIRI(NAMESPACE, "LexicalEntry");
    public static final IRI LEXICAL_SENSE = SVF.createIRI(NAMESPACE, "LexicalSense");

    public static final IRI DENOTES = SVF.createIRI(NAMESPACE, "denotes");
    public static final IRI IS_DENOTED_BY = SVF.createIRI(NAMESPACE, "isDenotedBy");
    public static final IRI CONCEPT = SVF.createIRI(NAMESPACE, "concept");
    public static final IRI IS_CONCEPT_OF = SVF.createIRI(NAMESPACE, "isConceptOf");
    public static final IRI EVOKES = SVF.createIRI(NAMESPACE, "evokes");
    public static final IRI IS_EVOKED_BY = SVF.createIRI(NAMESPACE, "isEvokedBy");
    public static final IRI SENSE = SVF.createIRI(NAMESPACE, "sense");
    public static final IRI IS_SENSE_OF = SVF.createIRI(NAMESPACE, "isSenseOf");
    public static final IRI REFERENCE = SVF.createIRI(NAMESPACE, "reference");
    public static final IRI IS_REFERENCE_OF = SVF.createIRI(NAMESPACE, "isReferenceOf");
    public static final IRI LEXICALIZED_SENSE = SVF.createIRI(NAMESPACE, "lexicalizedSense");
    public static final IRI IS_LEXICALIZED_SENSE_OF = SVF.createIRI(NAMESPACE, "isLexicalizedSenseOf");
    public static final IRI MORPHOLOGICAL_PATTERN = SVF.createIRI(NAMESPACE, "morphologicalPattern");
    public static final IRI USAGE = SVF.createIRI(NAMESPACE, "usage");

    public static final IRI LEXICAL_FORM = SVF.createIRI(NAMESPACE, "lexicalForm");
    public static final IRI CANONICAL_FORM = SVF.createIRI(NAMESPACE, "canonicalForm");
    public static final IRI OTHER_FORM = SVF.createIRI(NAMESPACE, "otherForm");

    public static final IRI REPRESENTATION = SVF.createIRI(NAMESPACE, "representation");
    public static final IRI PHONETIC_REP = SVF.createIRI(NAMESPACE, "phoneticRep");
    public static final IRI WRITTEN_REP = SVF.createIRI(NAMESPACE, "writtenRep");
}
