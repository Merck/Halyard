/*
 * Copyright © 2014 Merck Sharp & Dohme Corp., a subsidiary of Merck & Co., Inc.
 * All rights reserved.
 */
package com.msd.gin.halyard.sail;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class VOID_EXT {

    public static final String PREFIX = "void-ext";

    public static final String NAMESPACE = "http://ldf.fi/void-ext#";

    private static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

    public static final IRI DISTINCT_IRI_REFERENCE_SUBJECTS = SVF.createIRI(NAMESPACE, "distinctIRIReferenceSubjects");

    public static final IRI DISTINCT_IRI_REFERENCE_OBJECTS = SVF.createIRI(NAMESPACE, "distinctIRIReferenceObjects");

    public static final IRI DISTINCT_BLANK_NODE_OBJECTS = SVF.createIRI(NAMESPACE, "distinctBlankNodeObjects");

    public static final IRI DISTINCT_BLANK_NODE_SUBJECTS = SVF.createIRI(NAMESPACE, "distinctBlankNodeSubjects");

    public static final IRI DISTINCT_LITERALS = SVF.createIRI(NAMESPACE, "distinctLiterals");

    public static final IRI SUBJECT = SVF.createIRI(NAMESPACE, "subject");

    public static final IRI SUBJECT_PARTITION = SVF.createIRI(NAMESPACE, "subjectPartition");

    public static final IRI OBJECT = SVF.createIRI(NAMESPACE, "object");

    public static final IRI OBJECT_PARTITION = SVF.createIRI(NAMESPACE, "objectPartition");

}
