/*
 * Copyright © 2014 Merck Sharp & Dohme Corp., a subsidiary of Merck & Co., Inc.
 * All rights reserved.
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
package com.msd.gin.halyard.sail;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 * Prefix, namespace and IRIs for the parts of the VIOD ontology used in Halyard
 * @author Adam Sotona (MSD)
 */
public final class VOID_EXT {

    VOID_EXT(){}

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
