package com.msd.gin.halyard.vocab;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.kohsuke.MetaInfServices;

import com.msd.gin.halyard.common.Vocabulary;

@MetaInfServices(Vocabulary.class)
public final class WIKIBASE implements Vocabulary {
    private static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

    public static final String PREFIX = "wikibase";

    public static final String NAMESPACE = "http://wikiba.se/ontology#";

    public static final Namespace NS = new SimpleNamespace(PREFIX, NAMESPACE);

    public static final IRI ITEM = SVF.createIRI(NAMESPACE, "Item");
    public static final IRI REFERENCE = SVF.createIRI(NAMESPACE, "Reference");
    public static final IRI STATEMENT = SVF.createIRI(NAMESPACE, "Statement");
    public static final IRI VALUE = SVF.createIRI(NAMESPACE, "Value");

    public static final IRI CLAIM = SVF.createIRI(NAMESPACE, "claim");
    public static final IRI IDENTIFIERS = SVF.createIRI(NAMESPACE, "identifiers");
    public static final IRI RANK = SVF.createIRI(NAMESPACE, "rank");
    public static final IRI SITELINKS = SVF.createIRI(NAMESPACE, "sitelinks");
    public static final IRI STATEMENTS = SVF.createIRI(NAMESPACE, "statements");
    public static final IRI WIKI_GROUP = SVF.createIRI(NAMESPACE, "wikiGroup");
}
