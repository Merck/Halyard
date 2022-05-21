package com.msd.gin.halyard.vocab;

import com.msd.gin.halyard.common.Vocabulary;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Vocabulary.class)
public final class QUDT implements Vocabulary {
    private static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

    public static final String PREFIX = "qudt";

    public static final String NAMESPACE = "http://qudt.org/vocab/unit#";

    public static final Namespace NS = new SimpleNamespace(PREFIX, NAMESPACE);

    public static final IRI CENTIMETER = SVF.createIRI(NAMESPACE, "Centimeter");
    public static final IRI MILLIMETER = SVF.createIRI(NAMESPACE, "Millimeter");
    public static final IRI MICROMETER = SVF.createIRI(NAMESPACE, "Micrometer");

    public static final IRI LITER = SVF.createIRI(NAMESPACE, "Liter");
    public static final IRI INTL_UNIT_PER_LITER = SVF.createIRI(NAMESPACE, "InternationalUnitPerLiter");

    public static final IRI DEGREE_CELSIUS = SVF.createIRI(NAMESPACE, "DegreeCelsius");

    public static final IRI KILOGRAM = SVF.createIRI(NAMESPACE, "Kilogram");
    public static final IRI GRAM = SVF.createIRI(NAMESPACE, "Gram");

    public static final IRI DAY = SVF.createIRI(NAMESPACE, "Day");
    public static final IRI HOUR = SVF.createIRI(NAMESPACE, "Hour");
    public static final IRI MINUTE_TIME = SVF.createIRI(NAMESPACE, "MinuteTime");
    public static final IRI SECOND_TIME = SVF.createIRI(NAMESPACE, "SecondTime");
    public static final IRI MILLISECOND = SVF.createIRI(NAMESPACE, "MilliSecond");

    public static final IRI PERCENT = SVF.createIRI(NAMESPACE, "Percent");
}
