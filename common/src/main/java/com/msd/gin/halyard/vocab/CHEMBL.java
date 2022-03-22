package com.msd.gin.halyard.vocab;

import com.msd.gin.halyard.common.Vocabulary;

import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;

public final class CHEMBL implements Vocabulary {

    public static final String PREFIX = "cco";

    public static final String NAMESPACE = "http://rdf.ebi.ac.uk/terms/chembl#";

    public static final Namespace NS = new SimpleNamespace(PREFIX, NAMESPACE);
    public static final Namespace CHEMBL_NS = new SimpleNamespace("act", "http://rdf.ebi.ac.uk/resource/chembl/");
    public static final Namespace BAO_NS = new SimpleNamespace("bao", "http://www.bioassayontology.org/bao#");
    public static final Namespace BIBO_NS = new SimpleNamespace("bibo", "http://purl.org/ontology/bibo/");
    public static final Namespace ID_NS = new SimpleNamespace("id", "http://identifiers.org/");
    public static final Namespace QUDT_NS = new SimpleNamespace("qudt", "http://qudt.org/1.1/vocab/unit#");
    public static final Namespace OPS_NS = new SimpleNamespace("ops", "http://www.openphacts.org/units/");
    public static final Namespace CLO_NS = new SimpleNamespace("clo", "http://purl.obolibrary.org/obo/");
    public static final Namespace EFO_NS = new SimpleNamespace("efo", "http://www.ebi.ac.uk/efo/");
}
