package com.msd.gin.halyard.vocab;

import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.kohsuke.MetaInfServices;

import com.msd.gin.halyard.common.Vocabulary;

@MetaInfServices(Vocabulary.class)
public final class WIKIDATA implements Vocabulary {

    public static final Namespace WDATA_NS = new SimpleNamespace("wdata", "http://www.wikidata.org/wiki/Special:EntityData/");
    public static final Namespace WD_NS = new SimpleNamespace("wd", "http://www.wikidata.org/entity/");
    public static final Namespace WDS_NS = new SimpleNamespace("wds", "http://www.wikidata.org/entity/statement/");
    public static final Namespace WDV_NS = new SimpleNamespace("wdv", "http://www.wikidata.org/value/");
    public static final Namespace WDREF_NS = new SimpleNamespace("wdref", "http://www.wikidata.org/reference/");
    public static final Namespace WDT_NS = new SimpleNamespace("wdt", "http://www.wikidata.org/prop/direct/");
    public static final Namespace WDTN_NS = new SimpleNamespace("wdtn", "http://www.wikidata.org/prop/direct-normalized/");
    public static final Namespace P_NS = new SimpleNamespace("p", "http://www.wikidata.org/prop/");
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
}
