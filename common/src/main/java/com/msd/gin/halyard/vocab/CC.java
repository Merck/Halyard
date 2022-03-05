package com.msd.gin.halyard.vocab;

import com.msd.gin.halyard.common.Vocabulary;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public final class CC implements Vocabulary {
    private static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

    public static final String PREFIX = "cc";

    public static final String NAMESPACE = "http://creativecommons.org/ns#";

    public static final Namespace NS = new SimpleNamespace(PREFIX, NAMESPACE);

    public static final IRI WORK = SVF.createIRI(NAMESPACE, "Work");
    public static final IRI LICENSE_CLASS = SVF.createIRI(NAMESPACE, "License");
    public static final IRI JURISDICRION = SVF.createIRI(NAMESPACE, "Jurisdiction");

    public static final IRI PERMISSION = SVF.createIRI(NAMESPACE, "Permission");
    public static final IRI REPRODUCTION = SVF.createIRI(NAMESPACE, "Reproduction");
    public static final IRI DISTRIBUTION = SVF.createIRI(NAMESPACE, "Distribution");
    public static final IRI DERIVATIVE_WORKS = SVF.createIRI(NAMESPACE, "DerivativeWorks");
    public static final IRI SHARING = SVF.createIRI(NAMESPACE, "Sharing");

    public static final IRI REQUIREMENT = SVF.createIRI(NAMESPACE, "Requirement");
    public static final IRI NOTICE = SVF.createIRI(NAMESPACE, "Notice");
    public static final IRI ATTRIBUTION = SVF.createIRI(NAMESPACE, "Attribution");
    public static final IRI SHARE_ALIKE = SVF.createIRI(NAMESPACE, "ShareAlike");
    public static final IRI SOURCE_CODE = SVF.createIRI(NAMESPACE, "SourceCode");
    public static final IRI COPYLEFT = SVF.createIRI(NAMESPACE, "Copyleft");
    public static final IRI LESSER_COPYLEFT = SVF.createIRI(NAMESPACE, "LesserCopyleft");

    public static final IRI PROHIBITION = SVF.createIRI(NAMESPACE, "Prohibition");
    public static final IRI COMMERCIAL_USE = SVF.createIRI(NAMESPACE, "CommercialUse");
    public static final IRI HIGH_INCOME_NATION_USE = SVF.createIRI(NAMESPACE, "HighIncomeNationUse");

    public static final IRI PERMITS = SVF.createIRI(NAMESPACE, "permits");
    public static final IRI REQUIRES = SVF.createIRI(NAMESPACE, "requires");
    public static final IRI PROHIBITS = SVF.createIRI(NAMESPACE, "prohibits");
    public static final IRI JURISDICTION = SVF.createIRI(NAMESPACE, "jurisdiction");
    public static final IRI LEGALCODE = SVF.createIRI(NAMESPACE, "legalcode");
    public static final IRI DEPRECATED_ON = SVF.createIRI(NAMESPACE, "deprecatedOn");

    public static final IRI LICENSE = SVF.createIRI(NAMESPACE, "license");
    public static final IRI MORE_PERMISSIONS = SVF.createIRI(NAMESPACE, "morePermissions");
    public static final IRI ATTRIBUTION_NAME = SVF.createIRI(NAMESPACE, "attributionName");
    public static final IRI ATTRIBUTION_URL = SVF.createIRI(NAMESPACE, "attributionURL");
    public static final IRI USE_GUIDELINES = SVF.createIRI(NAMESPACE, "useGuidelines");
}
