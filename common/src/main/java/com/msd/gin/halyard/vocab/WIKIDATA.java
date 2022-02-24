package com.msd.gin.halyard.vocab;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.kohsuke.MetaInfServices;

import com.msd.gin.halyard.common.Vocabulary;

@MetaInfServices(Vocabulary.class)
public final class WIKIDATA implements Vocabulary {
    private static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

    public static final Namespace WDATA_NS = new SimpleNamespace("wdata", "http://www.wikidata.org/wiki/Special:EntityData/");
    public static final String WD_NAMESPACE = "http://www.wikidata.org/entity/";
    public static final Namespace WD_NS = new SimpleNamespace("wd", WD_NAMESPACE);
    public static final Namespace WDS_NS = new SimpleNamespace("wds", "http://www.wikidata.org/entity/statement/");
    public static final Namespace WDV_NS = new SimpleNamespace("wdv", "http://www.wikidata.org/value/");
    public static final Namespace WDREF_NS = new SimpleNamespace("wdref", "http://www.wikidata.org/reference/");
    public static final Namespace WDT_NS = new SimpleNamespace("wdt", "http://www.wikidata.org/prop/direct/");
    public static final Namespace WDTN_NS = new SimpleNamespace("wdtn", "http://www.wikidata.org/prop/direct-normalized/");
    public static final String P_NAMESPACE = "http://www.wikidata.org/prop/";
    public static final Namespace P_NS = new SimpleNamespace("p", P_NAMESPACE);
    public static final Namespace WDNO_NS = new SimpleNamespace("wdno", "http://www.wikidata.org/prop/novalue/");
    public static final Namespace PS_NS = new SimpleNamespace("ps", "http://www.wikidata.org/prop/statement/");
    public static final Namespace PSV_NS = new SimpleNamespace("psv", "http://www.wikidata.org/prop/statement/value/");
    public static final Namespace PSN_NS = new SimpleNamespace("psn", "http://www.wikidata.org/prop/statement/value-normalized/");
    public static final Namespace PQ_NS = new SimpleNamespace("pq", "http://www.wikidata.org/prop/qualifier/");
    public static final Namespace PQV_NS = new SimpleNamespace("pqv", "http://www.wikidata.org/prop/qualifier/value/");
    public static final Namespace PQN_NS = new SimpleNamespace("pqn", "http://www.wikidata.org/prop/qualifier/value-normalized/");
    public static final Namespace PR_NS = new SimpleNamespace("pr", "http://www.wikidata.org/prop/reference/");
    public static final Namespace PRV_NS = new SimpleNamespace("prv", "http://www.wikidata.org/prop/reference/value/");
    public static final Namespace PRN_NS = new SimpleNamespace("prn", "http://www.wikidata.org/prop/reference/value-normalized/");

    public static final IRI COUNTRY = SVF.createIRI(P_NAMESPACE, "P17");
    public static final IRI IMAGE = SVF.createIRI(P_NAMESPACE, "P18");
    public static final IRI INSTANCE_OF = SVF.createIRI(P_NAMESPACE, "P31");
    public static final IRI AUTHOR = SVF.createIRI(P_NAMESPACE, "P50");
    public static final IRI OCCUPATION = SVF.createIRI(P_NAMESPACE, "P106");
    public static final IRI STATED_IN = SVF.createIRI(P_NAMESPACE, "P248");
    public static final IRI PAGES = SVF.createIRI(P_NAMESPACE, "P304");
    public static final IRI DOI = SVF.createIRI(P_NAMESPACE, "P356");
    public static final IRI ISSUE = SVF.createIRI(P_NAMESPACE, "P433");
    public static final IRI VOLUME = SVF.createIRI(P_NAMESPACE, "P478");
    public static final IRI CATALOG_CODE = SVF.createIRI(P_NAMESPACE, "P528");
    public static final IRI PUBLICATION_DATE = SVF.createIRI(P_NAMESPACE, "P577");
    public static final IRI OF = SVF.createIRI(P_NAMESPACE, "P642");
    public static final IRI PUBMED_ID = SVF.createIRI(P_NAMESPACE, "P698");
    public static final IRI FAMILY_NAME = SVF.createIRI(P_NAMESPACE, "P734");
    public static final IRI GIVEN_NAME = SVF.createIRI(P_NAMESPACE, "P735");
    public static final IRI RETRIEVED = SVF.createIRI(P_NAMESPACE, "P813");
    public static final IRI REFERENCE_URL = SVF.createIRI(P_NAMESPACE, "P854");
    public static final IRI MAIN_SUBJECT = SVF.createIRI(P_NAMESPACE, "P921");
    public static final IRI CATALOG = SVF.createIRI(P_NAMESPACE, "P972");
    public static final IRI APPARENT_MAGNITUDE = SVF.createIRI(P_NAMESPACE, "P1215");
    public static final IRI ASTRONOMICAL_FILTER = SVF.createIRI(P_NAMESPACE, "P1227");
    public static final IRI PUBLISHED_IN = SVF.createIRI(P_NAMESPACE, "P1433");
    public static final IRI P_TITLE = SVF.createIRI(P_NAMESPACE, "P1476");
    public static final IRI SERIES_ORDINAL = SVF.createIRI(P_NAMESPACE, "P1545");
    public static final IRI SEE_ALSO = SVF.createIRI(P_NAMESPACE, "P1659");
    public static final IRI NATIVE_LABEL = SVF.createIRI(P_NAMESPACE, "P1705");
    public static final IRI AUTHOR_NAME_STRING = SVF.createIRI(P_NAMESPACE, "P2093");
    public static final IRI CITES_WORK = SVF.createIRI(P_NAMESPACE, "P2860");

    public static final IRI HUMAN = SVF.createIRI(WD_NAMESPACE, "Q5");
    public static final IRI UNITED_STATES = SVF.createIRI(WD_NAMESPACE, "Q30");
    public static final IRI ENGLISH_WIKIPEDIA = SVF.createIRI(WD_NAMESPACE, "Q328");
    public static final IRI ENGLISH = SVF.createIRI(WD_NAMESPACE, "Q1860");
    public static final IRI SPECIES = SVF.createIRI(WD_NAMESPACE, "Q7432");
    public static final IRI METRE = SVF.createIRI(WD_NAMESPACE, "Q11573");
    public static final IRI TAXON = SVF.createIRI(WD_NAMESPACE, "Q16521");
    public static final IRI PUBMED_CENTRAL = SVF.createIRI(WD_NAMESPACE, "Q229883");
    public static final IRI TITLE = SVF.createIRI(WD_NAMESPACE, "Q783521");
    public static final IRI WIKIMEDIA_CATEGORY = SVF.createIRI(WD_NAMESPACE, "Q4167836");
    public static final IRI CROSSREF = SVF.createIRI(WD_NAMESPACE, "Q5188229");
    public static final IRI EUROPE_PUBMED_CENTRAL = SVF.createIRI(WD_NAMESPACE, "Q5412157");
    public static final IRI MALE = SVF.createIRI(WD_NAMESPACE, "Q6581097");
    public static final IRI SCHOLARLY_ARTICLE = SVF.createIRI(WD_NAMESPACE, "Q13442814");
}
