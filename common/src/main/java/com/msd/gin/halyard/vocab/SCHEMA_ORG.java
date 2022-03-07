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
    public static final IRI DESCRIPTION = SVF.createIRI(NAMESPACE, "description");
    public static final IRI IN_LANGUAGE = SVF.createIRI(NAMESPACE, "inLanguage");
    public static final IRI VERSION = SVF.createIRI(NAMESPACE, "version");

    public static final IRI THING = SVF.createIRI(NAMESPACE, "Thing");
    public static final IRI ARTICLE = SVF.createIRI(NAMESPACE, "Article");

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
}
