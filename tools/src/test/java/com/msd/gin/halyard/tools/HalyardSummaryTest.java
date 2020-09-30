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
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.SailConnection;
import org.junit.Rule;
import org.junit.Test;
import static org.junit.Assert.*;
/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardSummaryTest {
	@Rule
	public final HadoopLogRule hadoopLogs = HadoopLogRule.create();

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
        IRI literalTypes[] = new IRI[]{XSD.INT, XSD.STRING, XSD.DOUBLE};
        IRI instances[] = generateIRIs(50, "http://whatever/instance#");
        Literal literals[] = new Literal[10];
        SimpleValueFactory svf = SimpleValueFactory.getInstance();
        Random r = new Random(66);
        Map<IRI, Collection<IRI>> classMap = new HashMap<>();
        Map<IRI, Integer> classCardinalities = new HashMap<>(), predicateCardinalities = new HashMap<>();
        Map<List<IRI>, Integer> domainCardinalities = new HashMap<>(), rangeCardinalities = new HashMap<>(), domainAndRangeCardinalities = new HashMap<>();
        for (int i = 0; i < literals.length; i++) {
            literals[i] = svf.createLiteral("0", literalTypes[r.nextInt(literalTypes.length)]);
        }
		HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "summaryTable", true, -1, true, 0, null, null);
		sail.initialize();
		try (SailConnection conn = sail.getConnection()) {
            for (IRI instance : instances) {
                Collection<IRI> clsC = new ArrayList<>();
                for (IRI clazz : classes) {
                    if (r.nextBoolean()) {
						conn.addStatement(instance, RDF.TYPE, clazz);
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
						conn.addStatement(instance, predicate, l);
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
                            domainAndRangeCardinalities.put(keys, domainAndRangeCardinalities.getOrDefault(keys, 0) + 1);
                        }
                        List<IRI> keys = new ArrayList<>(2);
                        keys.add(predicate);
                        keys.add(l.getDatatype());
                        rangeCardinalities.put(keys, rangeCardinalities.getOrDefault(keys, 0) + 1);
                    }
                    if (r.nextBoolean()) {
                        IRI otherInstance = instances[r.nextInt(instances.length)];
						conn.addStatement(instance, predicate, otherInstance);
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
        }
		sail.shutDown();

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
                assertStatement(me.getKey(), RDF.TYPE, HalyardSummary.cardinalityIRI("Class", HalyardSummary.toCardinality(me.getValue())), model);
            }
            for (Map.Entry<IRI, Integer> me : predicateCardinalities.entrySet()) {
                assertStatement(me.getKey(), RDF.TYPE, HalyardSummary.cardinalityIRI("Property", HalyardSummary.toCardinality(me.getValue())), model);
            }
            for (Map.Entry<List<IRI>, Integer> me : domainCardinalities.entrySet()) {
                assertStatement(me.getKey().get(0), HalyardSummary.cardinalityIRI("domain", HalyardSummary.toCardinality(me.getValue())), me.getKey().get(1), model);
            }
            for (Map.Entry<List<IRI>, Integer> me : rangeCardinalities.entrySet()) {
                assertStatement(me.getKey().get(0), HalyardSummary.cardinalityIRI("range", HalyardSummary.toCardinality(me.getValue())), me.getKey().get(1), model);
            }
            for (Map.Entry<List<IRI>, Integer> me : domainAndRangeCardinalities.entrySet()) {
                assertJoins(HalyardSummary.cardinalityIRI("sliceSubProperty", HalyardSummary.toCardinality(me.getValue())), me.getKey().get(0),
                    HalyardSummary.cardinalityIRI("sliceDomain", HalyardSummary.toCardinality(me.getValue())),me.getKey().get(1),
                    HalyardSummary.cardinalityIRI("sliceRange", HalyardSummary.toCardinality(me.getValue())), me.getKey().get(2),
                    model);
            }
        }
    }

    private void assertStatement(Resource subj, IRI pred, IRI obj, Model model) {
        assertTrue("required: <" + subj + "> <" + pred + "> <" + obj + ">", model.contains(subj, pred, obj));
    }

    private void assertJoins(IRI pred1, IRI obj1, IRI pred2, IRI obj2, IRI pred3, IRI obj3, Model model) {
        for (Resource subj : model.filter(null, pred1, obj1).subjects()) {
            if (model.contains(subj, pred2, obj2) && model.contains(subj, pred3, obj3)) {
                return;
            }
        }
        fail("required: [] <" + pred1 + "> <" + obj1 + ">; <" + pred2 + "> <" + obj2 + ">; <" + pred3 + "> <" + obj3 + ">");
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
