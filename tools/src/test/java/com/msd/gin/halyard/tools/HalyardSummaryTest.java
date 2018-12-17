/*
 * Copyright 2018 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
 * Inc., Kenilworth, NJ, USA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.msd.gin.halyard.tools;

import com.msd.gin.halyard.common.HBaseServerTestInstance;
import com.msd.gin.halyard.sail.HBaseSail;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.hadoop.util.ToolRunner;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import static org.junit.Assert.*;
import static com.msd.gin.halyard.tools.HalyardSummary.SummaryType.*;
import org.junit.Test;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardSummaryTest {

    private IRI[] generateIRIs(int num, String prefix) {
        SimpleValueFactory svf = SimpleValueFactory.getInstance();
        IRI iris[] = new IRI[num];
        for (int i = 0; i < num; i++) {
            iris[i] = svf.createIRI(prefix + i);
        }
        return iris;
    }

    @Test
    public void testSummary() throws Exception {
        IRI classes[] = generateIRIs(4, "http://whatever/class#");
        IRI predicates[] = generateIRIs(4, "http://whatever/predicate#");
        IRI literalTypes[] = new IRI[]{XMLSchema.INT, XMLSchema.STRING, XMLSchema.DOUBLE};
        IRI instances[] = generateIRIs(50, "http://whatever/instance#");
        Literal literals[] = new Literal[10];
        SimpleValueFactory svf = SimpleValueFactory.getInstance();
        Random r = new Random(66);
        Map<IRI, Collection<IRI>> classMap = new HashMap<>();
        Map<IRI, Integer> classCardinalities = new HashMap<>(), predicateCardinalities = new HashMap<>();
        Map<List<IRI>, Integer> domainCardinalities = new HashMap<>(), rangeCardinalities = new HashMap<>(), domainAndRangeCardinalities = new HashMap<>();
        Map<List<IRI>, Integer> rangeTypeCardinalities = new HashMap<>(), domainAndRangeTypeCardinalities = new HashMap<>(), classClassCardinalities = new HashMap<>();
        for (int i = 0; i < literals.length; i++) {
            literals[i] = svf.createLiteral("0", literalTypes[r.nextInt(literalTypes.length)]);
        }
        try (HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "summaryTable", true, -1, true, 0, null, null)) {
            sail.initialize();
            for (IRI instance : instances) {
                Collection<IRI> clsC = new ArrayList<>();
                for (IRI clazz : classes) {
                    if (r.nextBoolean()) {
                        sail.addStatement(instance, RDF.TYPE, clazz);
                        for (IRI otherClass : clsC) {
                            List<IRI> keys = new ArrayList<>(2);
                            keys.add(clazz);
                            keys.add(otherClass);
                            classClassCardinalities.put(keys, classClassCardinalities.getOrDefault(keys, 0) + 1);
                        }
                        clsC.add(clazz);
                        classCardinalities.put(clazz, classCardinalities.getOrDefault(clazz, 0) + 1);
                    }
                }
                classMap.put(instance, clsC);
            }
            for (IRI instance : instances) {
                for (IRI predicate : predicates) {
                    if (r.nextBoolean()) {
                        Literal l =literals[r.nextInt(literals.length)];
                        sail.addStatement(instance, predicate, l);
                        predicateCardinalities.put(predicate, predicateCardinalities.getOrDefault(predicate, 0) + 1);
                        for (IRI domainClass: classMap.getOrDefault(instance, Collections.emptySet())) {
                            List<IRI> keys = new ArrayList<>(2);
                            keys.add(predicate);
                            keys.add(domainClass);
                            domainCardinalities.put(keys, domainCardinalities.getOrDefault(keys, 0) + 1);
                            keys = new ArrayList<>(3);
                            keys.add(predicate);
                            keys.add(domainClass);
                            keys.add(l.getDatatype());
                            domainAndRangeTypeCardinalities.put(keys, domainAndRangeTypeCardinalities.getOrDefault(keys, 0) + 1);
                        }
                        List<IRI> keys = new ArrayList<>(2);
                        keys.add(predicate);
                        keys.add(l.getDatatype());
                        rangeTypeCardinalities.put(keys, rangeTypeCardinalities.getOrDefault(keys, 0) + 1);
                    }
                    if (r.nextBoolean()) {
                        IRI otherInstance = instances[r.nextInt(instances.length)];
                        sail.addStatement(instance, predicate, otherInstance);
                        predicateCardinalities.put(predicate, predicateCardinalities.getOrDefault(predicate, 0) + 1);
                        for (IRI domainClass: classMap.getOrDefault(instance, Collections.emptySet())) {
                            List<IRI> keys = new ArrayList<>(2);
                            keys.add(predicate);
                            keys.add(domainClass);
                            domainCardinalities.put(keys, domainCardinalities.getOrDefault(keys, 0) + 1);
                        }
                        for (IRI rangeClass: classMap.getOrDefault(otherInstance, Collections.emptySet())) {
                            for (IRI domainClass: classMap.getOrDefault(instance, Collections.emptySet())) {
                                List<IRI> keys = new ArrayList<>(3);
                                keys.add(predicate);
                                keys.add(domainClass);
                                keys.add(rangeClass);
                                domainAndRangeCardinalities.put(keys, domainAndRangeCardinalities.getOrDefault(keys, 0) + 1);
                            }
                            List<IRI> keys = new ArrayList<>(2);
                            keys.add(predicate);
                            keys.add(rangeClass);
                            rangeCardinalities.put(keys, rangeCardinalities.getOrDefault(keys, 0) + 1);
                        }
                    }
                }
            }
            sail.commit();
        }

        File summary = File.createTempFile("summary", ".trig");
        final IRI namedGraph = svf.createIRI("http://whatever/summary");

        assertEquals(0, ToolRunner.run(HBaseServerTestInstance.getInstanceConfig(), new HalyardSummary(),
            new String[]{"-s", "summaryTable", "-t", summary.toURI().toURL().toString(), "-g", namedGraph.stringValue(), "-d", "1"}));

        try (BufferedReader in = new BufferedReader(new FileReader(summary))) {
            Model model = Rio.parse(in, "http://whatever/", RDFFormat.TRIG);
//            model.forEach((Statement t) -> {
//                System.out.println(t);
//            });
            model = model.filter(null, null, null, namedGraph);
            for (Map.Entry<IRI, Integer> me : classCardinalities.entrySet()) {
                assertCardinality(ClassSummary.CARDINALITY_IRI, me.getValue(), model, ClassSummary.CLASS_IRI, me.getKey());
            }
            for (Map.Entry<IRI, Integer> me : predicateCardinalities.entrySet()) {
                assertCardinality(PredicateSummary.CARDINALITY_IRI, me.getValue(), model, PredicateSummary.PREDICATE_IRI, me.getKey());
            }
            for (Map.Entry<List<IRI>, Integer> me : domainCardinalities.entrySet()) {
                assertCardinality(DomainSummary.CARDINALITY_IRI, me.getValue(), model, DomainSummary.PREDICATE_IRI, me.getKey().get(0), DomainSummary.DOMAIN_IRI, me.getKey().get(1));
            }
            for (Map.Entry<List<IRI>, Integer> me : rangeCardinalities.entrySet()) {
                assertCardinality(RangeSummary.CARDINALITY_IRI, me.getValue(), model, RangeSummary.PREDICATE_IRI, me.getKey().get(0), RangeSummary.RANGE_IRI, me.getKey().get(1));
            }
            for (Map.Entry<List<IRI>, Integer> me : domainAndRangeCardinalities.entrySet()) {
                assertCardinality(DomainAndRangeSummary.CARDINALITY_IRI, me.getValue(), model, DomainAndRangeSummary.PREDICATE_IRI, me.getKey().get(0), DomainAndRangeSummary.DOMAIN_IRI, me.getKey().get(1), DomainAndRangeSummary.RANGE_IRI, me.getKey().get(2));
            }
            for (Map.Entry<List<IRI>, Integer> me : rangeTypeCardinalities.entrySet()) {
                assertCardinality(RangeTypeSummary.CARDINALITY_IRI, me.getValue(), model, RangeTypeSummary.PREDICATE_IRI, me.getKey().get(0), RangeTypeSummary.RANGE_TYPE_IRI, me.getKey().get(1));
            }
            for (Map.Entry<List<IRI>, Integer> me : domainAndRangeTypeCardinalities.entrySet()) {
                assertCardinality(DomainAndRangeTypeSummary.CARDINALITY_IRI, me.getValue(), model, DomainAndRangeTypeSummary.PREDICATE_IRI, me.getKey().get(0), DomainAndRangeTypeSummary.DOMAIN_IRI, me.getKey().get(1), DomainAndRangeTypeSummary.RANGE_TYPE_IRI, me.getKey().get(2));
            }
            for (Map.Entry<List<IRI>, Integer> me : classClassCardinalities.entrySet()) {
                assertCardinality(ClassesOverlapSummary.CARDINALITY_IRI, me.getValue(), model, ClassesOverlapSummary.CLASS_IRI, me.getKey().get(0), ClassesOverlapSummary.CLASS_IRI, me.getKey().get(1));
            }
        }
    }

    private void assertCardinality(IRI cardinalityPredicate, long count, Model model, IRI ... contains)  {
        int cardinality = 63 - Long.numberOfLeadingZeros(count);
        for (Statement st : model.filter(null, cardinalityPredicate, null)) {
            boolean cont = true;
            for (int i=0; i<contains.length; i+=2) {
                cont &= model.contains(st.getSubject(), contains[i], contains[i+1]);
            }
            if (cont) {
                assertEquals("Cardinality mismatch in: " + cardinalityPredicate.getLocalName() + " " + Arrays.asList(contains), cardinality, ((Literal)st.getObject()).intValue());
                return;
            }
        }
        fail("Failed to find match of: " + cardinalityPredicate.getLocalName() + " " + Arrays.asList(contains));
    }

    @Test(expected = MissingOptionException.class)
    public void testRunNoArgs() throws Exception {
        new HalyardSummary().run(new String[0]);
    }

    @Test
    public void testRunVersion() throws Exception {
        assertEquals(0, new HalyardSummary().run(new String[]{"-v"}));
    }

    @Test(expected = UnrecognizedOptionException.class)
    public void testRunInvalid() throws Exception {
        new HalyardSummary().run(new String[]{"-invalid"});
    }
}
