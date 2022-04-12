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
    public static final IRI WIKIBASE_ITEM = SVF.createIRI(NAMESPACE, "WikibaseItem");
    public static final IRI PROPERTY = SVF.createIRI(NAMESPACE, "Property");
    public static final IRI LEXEME = SVF.createIRI(NAMESPACE, "Lexeme");
    public static final IRI FORM = SVF.createIRI(NAMESPACE, "Form");
    public static final IRI REFERENCE_CLASS = SVF.createIRI(NAMESPACE, "Reference");
    public static final IRI STATEMENT = SVF.createIRI(NAMESPACE, "Statement");
    public static final IRI COMMONS_MEDIA = SVF.createIRI(NAMESPACE, "CommonsMedia");
    public static final IRI MONOLINGUALTEXT = SVF.createIRI(NAMESPACE, "Monolingualtext");
    public static final IRI VALUE = SVF.createIRI(NAMESPACE, "Value");
    public static final IRI GLOBECOORDINATE_VALUE = SVF.createIRI(NAMESPACE, "GlobecoordinateValue");
    public static final IRI TIME_VALUE_CLASS = SVF.createIRI(NAMESPACE, "TimeValue");
    public static final IRI QUANTITY_VALUE = SVF.createIRI(NAMESPACE, "QuantityValue");
    public static final IRI QUANTITY = SVF.createIRI(NAMESPACE, "Quantity");
    public static final IRI SENSE = SVF.createIRI(NAMESPACE, "Sense");
    public static final IRI STRING = SVF.createIRI(NAMESPACE, "String");
    public static final IRI TIME = SVF.createIRI(NAMESPACE, "Time");
    public static final IRI URL = SVF.createIRI(NAMESPACE, "Url");
    public static final IRI BEST_RANK = SVF.createIRI(NAMESPACE, "BestRank");
    public static final IRI NORMAL_RANK = SVF.createIRI(NAMESPACE, "NormalRank");
    public static final IRI PREFERRED_RANK = SVF.createIRI(NAMESPACE, "PreferredRank");
    public static final IRI DEPRECATED_RANK = SVF.createIRI(NAMESPACE, "DeprecatedRank");

    public static final IRI BADGE = SVF.createIRI(NAMESPACE, "badge");
    public static final IRI CLAIM = SVF.createIRI(NAMESPACE, "claim");
    public static final IRI DIRECT_CLAIM = SVF.createIRI(NAMESPACE, "directClaim");
    public static final IRI DIRECT_CLAIM_NORMALIZED = SVF.createIRI(NAMESPACE, "directClaimNormalized");
    public static final IRI GEO_LATITUDE = SVF.createIRI(NAMESPACE, "geoLatitude");
    public static final IRI GEO_LONGITUDE = SVF.createIRI(NAMESPACE, "geoLongitude");
    public static final IRI GEO_PRECISION = SVF.createIRI(NAMESPACE, "geoPrecision");
    public static final IRI GEO_GLOBE = SVF.createIRI(NAMESPACE, "geoGlobe");
    public static final IRI IDENTIFIERS = SVF.createIRI(NAMESPACE, "identifiers");
    public static final IRI LEMMA = SVF.createIRI(NAMESPACE, "lemma");
    public static final IRI LEXICAL_CATEGORY = SVF.createIRI(NAMESPACE, "lexicalCategory");
    public static final IRI GRAMMATICAL_FEATURE = SVF.createIRI(NAMESPACE, "grammaticalFeature");
    public static final IRI STATEMENT_PROPERTY = SVF.createIRI(NAMESPACE, "statementProperty");
    public static final IRI STATEMENT_VALUE = SVF.createIRI(NAMESPACE, "statementValue");
    public static final IRI STATEMENT_VALUE_NORMALIZED = SVF.createIRI(NAMESPACE, "statementValueNormalized");
    public static final IRI QUALIFIER = SVF.createIRI(NAMESPACE, "qualifier");
    public static final IRI QUALIFIER_VALUE = SVF.createIRI(NAMESPACE, "qualifierValue");
    public static final IRI QUALIFIER_VALUE_NORMALIZED = SVF.createIRI(NAMESPACE, "qualifierValueNormalized");
    public static final IRI QUANTITY_AMOUNT = SVF.createIRI(NAMESPACE, "quantityAmount");
    public static final IRI QUANTITY_LOWER_BOUND = SVF.createIRI(NAMESPACE, "quantityLowerBound");
    public static final IRI QUANTITY_UPPER_BOUND = SVF.createIRI(NAMESPACE, "quantityUpperBound");
    public static final IRI QUANTITY_UNIT = SVF.createIRI(NAMESPACE, "quantityUnit");
    public static final IRI QUANTITY_NORMALIZED = SVF.createIRI(NAMESPACE, "quantityNormalized");
    public static final IRI REFERENCE = SVF.createIRI(NAMESPACE, "reference");
    public static final IRI REFERENCE_VALUE = SVF.createIRI(NAMESPACE, "referenceValue");
    public static final IRI REFERENCE_VALUE_NORMALIZED = SVF.createIRI(NAMESPACE, "referenceValueNormalized");
    public static final IRI RANK = SVF.createIRI(NAMESPACE, "rank");
    public static final IRI SITELINKS = SVF.createIRI(NAMESPACE, "sitelinks");
    public static final IRI STATEMENTS = SVF.createIRI(NAMESPACE, "statements");
    public static final IRI TIME_VALUE = SVF.createIRI(NAMESPACE, "timeValue");
    public static final IRI TIME_PRECISION = SVF.createIRI(NAMESPACE, "timePrecision");
    public static final IRI TIME_TIMEZONE = SVF.createIRI(NAMESPACE, "timeTimezone");
    public static final IRI TIME_CALENDAR_MODEL = SVF.createIRI(NAMESPACE, "timeCalendarModel");
    public static final IRI WIKI_GROUP = SVF.createIRI(NAMESPACE, "wikiGroup");

    public static final IRI DUMP = SVF.createIRI(NAMESPACE, "Dump");
}
