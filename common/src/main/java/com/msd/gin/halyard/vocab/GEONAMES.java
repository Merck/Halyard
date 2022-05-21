package com.msd.gin.halyard.vocab;

import com.msd.gin.halyard.common.Vocabulary;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Vocabulary.class)
public final class GEONAMES implements Vocabulary {
    private static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

    public static final String PREFIX = "gn_ont";
    public static final String ID_PREFIX = "gn_id";

    public static final String NAMESPACE = "http://www.geonames.org/ontology#";
    public static final String ID_NAMESPACE = "https://sws.geonames.org/";

    public static final Namespace NS = new SimpleNamespace(PREFIX, NAMESPACE);
    public static final Namespace ID_NS = new IntegerNamespace(ID_PREFIX, ID_NAMESPACE);

    public static final IRI CLASS = SVF.createIRI(NAMESPACE, "Class");
    public static final IRI CODE = SVF.createIRI(NAMESPACE, "Code");
    public static final IRI FEATURE = SVF.createIRI(NAMESPACE, "Feature");
    public static final IRI GEONAMES_FEATURE = SVF.createIRI(NAMESPACE, "GeonamesFeature");
    public static final IRI MAP = SVF.createIRI(NAMESPACE, "Map");
    public static final IRI RDFDATA = SVF.createIRI(NAMESPACE, "RDFData");
    public static final IRI WIKIPEDIA_ARTICLE_TYPE = SVF.createIRI(NAMESPACE, "WikipediaArticle");

    public static final IRI ALTERNATE_NAME = SVF.createIRI(NAMESPACE, "alternateName");
    public static final IRI CHILDREN_FEATURES = SVF.createIRI(NAMESPACE, "childrenFeatures");
    public static final IRI COLLOQUIAL_NAME = SVF.createIRI(NAMESPACE, "colloquialName");
    public static final IRI COUNTRY_CODE = SVF.createIRI(NAMESPACE, "countryCode");
    public static final IRI FEATURE_CODE = SVF.createIRI(NAMESPACE, "featureCode");
    public static final IRI GEONAMES_ID = SVF.createIRI(NAMESPACE, "geonamesID");
    public static final IRI HISTORICAL_NAME = SVF.createIRI(NAMESPACE, "historicalName");
    public static final IRI LAT = SVF.createIRI(NAMESPACE, "lat");
    public static final IRI LOCATED_IN = SVF.createIRI(NAMESPACE, "locatedIn");
    public static final IRI LOCATION_MAP = SVF.createIRI(NAMESPACE, "locationMap");
    public static final IRI LONG = SVF.createIRI(NAMESPACE, "long");
    public static final IRI NAME = SVF.createIRI(NAMESPACE, "name");
    public static final IRI NEARBY = SVF.createIRI(NAMESPACE, "nearby");
    public static final IRI NEARBY_FEATURES = SVF.createIRI(NAMESPACE, "nearbyFeatures");
    public static final IRI NEIGHBOUR = SVF.createIRI(NAMESPACE, "neighbour");
    public static final IRI NEIGHBOURING_FEATURES = SVF.createIRI(NAMESPACE, "neighbouringFeatures");
    public static final IRI OFFICIAL_NAME = SVF.createIRI(NAMESPACE, "officialName");
    public static final IRI PARENT_ADM1 = SVF.createIRI(NAMESPACE, "parentADM1");
    public static final IRI PARENT_ADM2 = SVF.createIRI(NAMESPACE, "parentADM2");
    public static final IRI PARENT_ADM3 = SVF.createIRI(NAMESPACE, "parentADM3");
    public static final IRI PARENT_ADM4 = SVF.createIRI(NAMESPACE, "parentADM4");
    public static final IRI PARENT_COUNTRY = SVF.createIRI(NAMESPACE, "parentCountry");
    public static final IRI PARENT_FEATURE = SVF.createIRI(NAMESPACE, "parentFeature");
    public static final IRI POPULATION = SVF.createIRI(NAMESPACE, "population");
    public static final IRI POSTAL_CODE = SVF.createIRI(NAMESPACE, "postalCode");
    public static final IRI SHORT_NAME = SVF.createIRI(NAMESPACE, "shortName");
    public static final IRI WIKIPEDIA_ARTICLE = SVF.createIRI(NAMESPACE, "wikipediaArticle");
}
