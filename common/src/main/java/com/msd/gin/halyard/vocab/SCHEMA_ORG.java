package com.msd.gin.halyard.vocab;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.kohsuke.MetaInfServices;

import com.msd.gin.halyard.common.Vocabulary;

@MetaInfServices(Vocabulary.class)
public final class SCHEMA_ORG implements Vocabulary {
    private static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

    public static final String PREFIX = "s";

    public static final String NAMESPACE = "http://schema.org/";

    public static final Namespace NS = new SimpleNamespace(PREFIX, NAMESPACE);

    public static final IRI MEDIA_OBJECT = SVF.createIRI(NAMESPACE, "MediaObject");
    public static final IRI AUDIO_OBJECT = SVF.createIRI(NAMESPACE, "AudioObject");
    public static final IRI IMAGE_OBJECT = SVF.createIRI(NAMESPACE, "ImageObject");
    public static final IRI VIDEO_OBJECT = SVF.createIRI(NAMESPACE, "VideoObject");

    public static final IRI DATE_MODIFIED = SVF.createIRI(NAMESPACE, "dateModified");
    public static final IRI DATE_PUBLISHED = SVF.createIRI(NAMESPACE, "datePublished");
    public static final IRI DESCRIPTION = SVF.createIRI(NAMESPACE, "description");
    public static final IRI IN_LANGUAGE = SVF.createIRI(NAMESPACE, "inLanguage");
    public static final IRI VERSION = SVF.createIRI(NAMESPACE, "version");

    public static final IRI THING = SVF.createIRI(NAMESPACE, "Thing");
    public static final IRI PERSON = SVF.createIRI(NAMESPACE, "Person");
    public static final IRI PHYSICIAN = SVF.createIRI(NAMESPACE, "Physician");
    public static final IRI ARTICLE = SVF.createIRI(NAMESPACE, "Article");
    public static final IRI PUBLICATION_ISSUE = SVF.createIRI(NAMESPACE, "PublicationIssue");
    public static final IRI PUBLICATION_VOLUME = SVF.createIRI(NAMESPACE, "PublicationVolume");
    public static final IRI PERIODICAL = SVF.createIRI(NAMESPACE, "Periodical");
    public static final IRI ORGANIZATION = SVF.createIRI(NAMESPACE, "Organization");
    public static final IRI EDUCATIONAL_ORGANIZATION = SVF.createIRI(NAMESPACE, "EducationalOrganization");
    public static final IRI HOSPITAL = SVF.createIRI(NAMESPACE, "Hospital");
    public static final IRI MONETARY_GRANT = SVF.createIRI(NAMESPACE, "MonetaryGrant");
    public static final IRI POSTAL_ADDRESS = SVF.createIRI(NAMESPACE, "PostalAddress");
    public static final IRI CONTACT_POINT_TYPE = SVF.createIRI(NAMESPACE, "ContactPoint");
    public static final IRI COMPLETED = SVF.createIRI(NAMESPACE, "Completed");
    public static final IRI RECRUITING = SVF.createIRI(NAMESPACE, "Recruiting");
    public static final IRI NOT_YET_RECRUITING = SVF.createIRI(NAMESPACE, "NotYetRecruiting");

    public static final IRI ABOUT = SVF.createIRI(NAMESPACE, "about");
    public static final IRI CONTENT_SIZE = SVF.createIRI(NAMESPACE, "contentSize");
    public static final IRI CONTENT_URL = SVF.createIRI(NAMESPACE, "contentUrl");
    public static final IRI DURATION = SVF.createIRI(NAMESPACE, "duration");
    public static final IRI ENCODING_FORMAT = SVF.createIRI(NAMESPACE, "encodingFormat");
    public static final IRI HEIGHT = SVF.createIRI(NAMESPACE, "height");
    public static final IRI IS_PART_OF = SVF.createIRI(NAMESPACE, "isPartOf");
    public static final IRI NAME = SVF.createIRI(NAMESPACE, "name");
    public static final IRI NUMBER_OF_PAGES = SVF.createIRI(NAMESPACE, "numberOfPages");
    public static final IRI WIDTH = SVF.createIRI(NAMESPACE, "width");

    public static final IRI ACCOUNTABLE_PERSON = SVF.createIRI(NAMESPACE, "accountablePerson");
    public static final IRI ADDITIONAL_NAME = SVF.createIRI(NAMESPACE, "additionalName");
    public static final IRI ADDRESS_COUNTRY = SVF.createIRI(NAMESPACE, "addressCountry");
    public static final IRI ADDRESS_LOCALITY = SVF.createIRI(NAMESPACE, "addressLocality");
    public static final IRI ADDRESS_REGION = SVF.createIRI(NAMESPACE, "addressRegion");
    public static final IRI AFFILIATION = SVF.createIRI(NAMESPACE, "affiliation");
    public static final IRI ALTERNATE_NAME = SVF.createIRI(NAMESPACE, "alternateName");
    public static final IRI AUTHOR = SVF.createIRI(NAMESPACE, "author");
    public static final IRI CITATION = SVF.createIRI(NAMESPACE, "citation");
    public static final IRI CONTACT_POINT = SVF.createIRI(NAMESPACE, "contactPoint");
    public static final IRI CONTACT_TYPE = SVF.createIRI(NAMESPACE, "contactType");
    public static final IRI DEPARTMENT = SVF.createIRI(NAMESPACE, "department");
    public static final IRI EMAIL = SVF.createIRI(NAMESPACE, "email");
    public static final IRI FAMILY_NAME = SVF.createIRI(NAMESPACE, "familyName");
    public static final IRI FUNDER = SVF.createIRI(NAMESPACE, "funder");
    public static final IRI GIVEN_NAME = SVF.createIRI(NAMESPACE, "givenName");
    public static final IRI HONORIFIC_PREFIX = SVF.createIRI(NAMESPACE, "honorificPrefix");
    public static final IRI HONORIFIC_SUFFIX = SVF.createIRI(NAMESPACE, "honorificSuffix");
    public static final IRI IDENTIFIER = SVF.createIRI(NAMESPACE, "identifier");
    public static final IRI ISSN = SVF.createIRI(NAMESPACE, "issn");
    public static final IRI ISSUE_NUMBER = SVF.createIRI(NAMESPACE, "issueNumber");
    public static final IRI KEYWORDS = SVF.createIRI(NAMESPACE, "keywords");
    public static final IRI MEDICINE_SPECIALTY = SVF.createIRI(NAMESPACE, "medicineSpecialty");
    public static final IRI MEDICINE_TYPE = SVF.createIRI(NAMESPACE, "medicineType");
    public static final IRI POSTAL_CODE = SVF.createIRI(NAMESPACE, "postalCode");
    public static final IRI RELEVANT_SPECIALTY = SVF.createIRI(NAMESPACE, "relevantSpecialty");
    public static final IRI ROLE_NAME = SVF.createIRI(NAMESPACE, "roleName");
    public static final IRI SPONSOR = SVF.createIRI(NAMESPACE, "sponsor");
    public static final IRI START_DATE = SVF.createIRI(NAMESPACE, "startDate");
    public static final IRI STATUS = SVF.createIRI(NAMESPACE, "status");
    public static final IRI STREET_ADDRESS = SVF.createIRI(NAMESPACE, "streetAddress");
    public static final IRI STUDY = SVF.createIRI(NAMESPACE, "study");
    public static final IRI STUDY_LOCATION = SVF.createIRI(NAMESPACE, "studyLocation");
    public static final IRI STUDY_SUBJECT = SVF.createIRI(NAMESPACE, "studySubject");
    public static final IRI TELEPHONE = SVF.createIRI(NAMESPACE, "telephone");
    public static final IRI URL = SVF.createIRI(NAMESPACE, "url");
    public static final IRI VOLUME_NUMBER = SVF.createIRI(NAMESPACE, "volumeNumber");
    public static final IRI WORKS_FOR = SVF.createIRI(NAMESPACE, "worksFor");
}
