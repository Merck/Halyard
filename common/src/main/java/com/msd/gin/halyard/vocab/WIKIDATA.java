package com.msd.gin.halyard.vocab;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.kohsuke.MetaInfServices;

import com.msd.gin.halyard.common.Vocabulary;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@MetaInfServices(Vocabulary.class)
public final class WIKIDATA implements Vocabulary {
    private static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

    public static final Namespace WDATA_NS = new SimpleNamespace("wdata", "https://www.wikidata.org/wiki/Special:EntityData/");
    public static final String WD_NAMESPACE = "http://www.wikidata.org/entity/";
    public static final String WDT_NAMESPACE = "http://www.wikidata.org/prop/direct/";
    public static final String WDTN_NAMESPACE = "http://www.wikidata.org/prop/direct-normalized/";
    public static final String WDNO_NAMESPACE = "http://www.wikidata.org/prop/novalue/";
    public static final Namespace WD_NS = new SimpleNamespace("wd", WD_NAMESPACE);
    public static final Namespace WDS_NS = new SimpleNamespace("wds", "http://www.wikidata.org/entity/statement/");
    public static final Namespace WDV_NS = new SimpleNamespace("wdv", "http://www.wikidata.org/value/");
    public static final Namespace WDREF_NS = new SimpleNamespace("wdref", "http://www.wikidata.org/reference/");
    public static final Namespace WDT_NS = new SimpleNamespace("wdt", WDT_NAMESPACE);
    public static final Namespace WDTN_NS = new SimpleNamespace("wdtn", WDTN_NAMESPACE);
    public static final Namespace WDNO_NS = new SimpleNamespace("wdno", WDNO_NAMESPACE);

    public static final String P_NAMESPACE = "http://www.wikidata.org/prop/";
    public static final String PS_NAMESPACE = "http://www.wikidata.org/prop/statement/";
    public static final String PSV_NAMESPACE = "http://www.wikidata.org/prop/statement/value/";
    public static final String PSN_NAMESPACE = "http://www.wikidata.org/prop/statement/value-normalized/";
    public static final String PQ_NAMESPACE = "http://www.wikidata.org/prop/qualifier/";
    public static final String PQV_NAMESPACE = "http://www.wikidata.org/prop/qualifier/value/";
    public static final String PQN_NAMESPACE = "http://www.wikidata.org/prop/qualifier/value-normalized/";
    public static final String PR_NAMESPACE = "http://www.wikidata.org/prop/reference/";
    public static final String PRV_NAMESPACE = "http://www.wikidata.org/prop/reference/value/";
    public static final String PRN_NAMESPACE = "http://www.wikidata.org/prop/reference/value-normalized/";

    public static final Namespace P_NS = new SimpleNamespace("p", P_NAMESPACE);
    public static final Namespace PS_NS = new SimpleNamespace("ps", PS_NAMESPACE);
    public static final Namespace PSV_NS = new SimpleNamespace("psv", PSV_NAMESPACE);
    public static final Namespace PSN_NS = new SimpleNamespace("psn", PSN_NAMESPACE);
    public static final Namespace PQ_NS = new SimpleNamespace("pq", PQ_NAMESPACE);
    public static final Namespace PQV_NS = new SimpleNamespace("pqv", PQV_NAMESPACE);
    public static final Namespace PQN_NS = new SimpleNamespace("pqn", PQN_NAMESPACE);
    public static final Namespace PR_NS = new SimpleNamespace("pr", PR_NAMESPACE);
    public static final Namespace PRV_NS = new SimpleNamespace("prv", PRV_NAMESPACE);
    public static final Namespace PRN_NS = new SimpleNamespace("prn", PRN_NAMESPACE);

    // common ID namespaces
    public static final Namespace DOI_NS = new SimpleNamespace("doi", "http://dx.doi.org/");
    public static final Namespace ORCID_NS = new SimpleNamespace("orcid", "https://orcid.org/");
    public static final Namespace GKG_NS = new SimpleNamespace("gkg", "http://g.co/kg/g/");
    public static final Namespace GEONAMES_NS = new SimpleNamespace("geonames", "http://sws.geonames.org/");
    public static final Namespace ENTREZ_NS = new SimpleNamespace("entrez", "http://purl.uniprot.org/geneid/");
    public static final Namespace UNIPROT_NS = new SimpleNamespace("uniprot", "http://purl.uniprot.org/uniprot/");
    public static final Namespace GND_NS = new SimpleNamespace("gnd", "https://d-nb.info/gnd/");

    // popular items
    public static final IRI HUMAN = SVF.createIRI(WD_NAMESPACE, "Q5");
    public static final IRI UNITED_STATES = SVF.createIRI(WD_NAMESPACE, "Q30");
    public static final IRI ENGLISH_WIKIPEDIA = SVF.createIRI(WD_NAMESPACE, "Q328");
    public static final IRI ENGLISH = SVF.createIRI(WD_NAMESPACE, "Q1860");
    public static final IRI SPECIES = SVF.createIRI(WD_NAMESPACE, "Q7432");
    public static final IRI GENE = SVF.createIRI(WD_NAMESPACE, "Q7187");
    public static final IRI METRE = SVF.createIRI(WD_NAMESPACE, "Q11573");
    public static final IRI TAXON = SVF.createIRI(WD_NAMESPACE, "Q16521");
    public static final IRI PUBMED_CENTRAL = SVF.createIRI(WD_NAMESPACE, "Q229883");
    public static final IRI TITLE = SVF.createIRI(WD_NAMESPACE, "Q783521");
    public static final IRI WIKIMEDIA_CATEGORY = SVF.createIRI(WD_NAMESPACE, "Q4167836");
    public static final IRI CROSSREF = SVF.createIRI(WD_NAMESPACE, "Q5188229");
    public static final IRI EUROPE_PUBMED_CENTRAL = SVF.createIRI(WD_NAMESPACE, "Q5412157");
    public static final IRI GBIF = SVF.createIRI(WD_NAMESPACE, "Q1531570");
    public static final IRI MALE = SVF.createIRI(WD_NAMESPACE, "Q6581097");
    public static final IRI FEMALE = SVF.createIRI(WD_NAMESPACE, "Q6581072");
    public static final IRI SCHOLARLY_ARTICLE = SVF.createIRI(WD_NAMESPACE, "Q13442814");

    public static final class Properties {
        // popular properties
        public static final String COUNTRY = "P17";
        public static final String IMAGE = "P18";
        public static final String PLACE_OF_BIRTH = "P19";
        public static final String PLACE_OF_DEATH = "P20";
        public static final String SEX_OR_GENDER = "P21";
        public static final String INSTANCE_OF = "P31";
        public static final String AUTHOR = "P50";
        public static final String FAMILY = "P53";
        public static final String EDUCATED_AT = "P69";
        public static final String OCCUPATION = "P106";
        public static final String FOLLOWS = "P155";
        public static final String FOLLOWED_BY= "P156";
        public static final String GND_ID = "P227";
        public static final String STATED_IN = "P248";
        public static final String LOCATION = "P276";
        public static final String SUBCLASS_OF = "P279";
        public static final String POSTAL_CODE = "P281";
        public static final String PAGES = "P304";
        public static final String ENTREZ_GENE_ID = "P351";
        public static final String UNIPROT_PROTEIN_ID = "P352";
        public static final String DOI = "P356";
        public static final String PART_OF = "P361";
        public static final String ISSUE = "P433";
        public static final String VOLUME = "P478";
        public static final String ORCID = "P496";
        public static final String CATALOG_CODE = "P528";
        public static final String DATE_OF_BIRTH = "P569";
        public static final String DATE_OF_DEATH = "P570";
        public static final String INCEPTION = "P571";
        public static final String PUBLICATION_DATE = "P577";
        public static final String END_TIME = "P582";
        public static final String POINT_IN_TIME = "P585";
        public static final String COORDINATE_LOCATION = "P625";
        public static final String OF = "P642";
        public static final String STREET_NUMBER = "P670";
        public static final String PUBMED_ID = "P698";
        public static final String FAMILY_NAME = "P734";
        public static final String GIVEN_NAME = "P735";
        public static final String RETRIEVED = "P813";
        public static final String ARXIV_ID = "P818";
        public static final String REFERENCE_URL = "P854";
        public static final String OFFICIAL_WEBSITE = "P856";
        public static final String MAIN_SUBJECT = "P921";
        public static final String PMCID = "P932";
        public static final String WORK_LOCATION = "P937";
        public static final String CATALOG = "P972";
        public static final String CHROMOSOME = "P1057";
        public static final String POPULATION = "P1082";
        public static final String APPARENT_MAGNITUDE = "P1215";
        public static final String ASTRONOMICAL_FILTER = "P1227";
        public static final String PUBLISHED_IN = "P1433";
        public static final String OFFICIAL_NAME = "P1448";
        public static final String TITLE = "P1476";
        public static final String SERIES_ORDINAL = "P1545";
        public static final String GEONAMES_ID = "P1566";
        public static final String SEE_ALSO = "P1659";
        public static final String NATIVE_LABEL = "P1705";
        public static final String NAMED_AS = "P1810";
        public static final String SHORT_NAME = "P1813";
        public static final String LENGTH = "P2043";
        public static final String AREA = "P2046";
        public static final String DURATION = "P2047";
        public static final String HEIGHT = "P2048";
        public static final String WIDTH = "P2049";
        public static final String AUTHOR_NAME_STRING = "P2093";
        public static final String PARALLAX = "P2214";
        public static final String PROPER_MOTION = "P2215";
        public static final String RADIAL_VELOCITY = "P2216";
        public static final String GOOGLE_KNOWLEDGE_GRAPH_ID = "P2671";
        public static final String CITES_WORK = "P2860";
        public static final String EXACT_MATCH = "P2888";
        public static final String RIGHT_ASCENSION = "P6257";
        public static final String DECLINATION = "P6258";
        public static final String EPOCH = "P6259";
        public static final String STREET_ADDRESS = "P6375";
    }

	public static Collection<IRI> getIRIs() {
		List<String> properties = new ArrayList<>(61);
		for (Field f : Properties.class.getFields()) {
			if (f.getType() == Namespace.class) {
				try {
					properties.add((String) f.get(null));
				} catch (IllegalAccessException ex) {
					throw new AssertionError(ex);
				}
			}
		}
		String[] propertyNamespaces = {
			WDT_NAMESPACE, WDTN_NAMESPACE, WDNO_NAMESPACE,
			P_NAMESPACE, PS_NAMESPACE, PSV_NAMESPACE, PSN_NAMESPACE,
			PQ_NAMESPACE, PQV_NAMESPACE, PQN_NAMESPACE, PR_NAMESPACE, PRV_NAMESPACE, PRN_NAMESPACE
		};

		List<IRI> iris = new ArrayList<>(properties.size()*propertyNamespaces.length);
		for (String ns : propertyNamespaces) {
			for (String prop : properties) {
				iris.add(SVF.createIRI(ns, prop));
			}
		}
		return iris;
	}
}
