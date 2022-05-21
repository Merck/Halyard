package com.msd.gin.halyard.vocab;

import com.msd.gin.halyard.common.Vocabulary;

import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Vocabulary.class)
public final class IDENTIFIERS_ORG implements Vocabulary {
    public static final String PREFIX = "qudt";

    public static final String BASE_NAMESPACE = "http://identifiers.org/";

    public static final Namespace INTACT_NS = new SimpleNamespace("id_intact", BASE_NAMESPACE+"intact/");
    public static final Namespace MESH_NS = new SimpleNamespace("id_mesh", BASE_NAMESPACE+"mesh/");
    public static final Namespace OBO_NS = new SimpleNamespace("id_obo", BASE_NAMESPACE+"obo.go/");
    public static final Namespace PDB_NS = new SimpleNamespace("id_pdb", BASE_NAMESPACE+"pdb/");
    public static final Namespace PUBMED_NS = new IntegerNamespace("id_pm", BASE_NAMESPACE+"pubmed/");
    public static final Namespace REACTOME_NS = new SimpleNamespace("id_react", BASE_NAMESPACE+"reactome/");
    public static final Namespace TAXONOMY_NS = new IntegerNamespace("id_tax", BASE_NAMESPACE+"taxonomy/");
}
