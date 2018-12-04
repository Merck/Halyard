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
import java.util.Random;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.hadoop.util.ToolRunner;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardSummaryTest {


    private IRI[] generateIRIs(int num, String prefix) {
        SimpleValueFactory svf = SimpleValueFactory.getInstance();
        IRI iris[] = new IRI[num];
        for (int i=0; i<num; i++) {
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
        for (int i=0; i<literals.length; i++) {
            literals[i] = svf.createLiteral("0", literalTypes[r.nextInt(literalTypes.length)]);
        }
        final HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "summaryTable", true, -1, true, 0, null, null);
        sail.initialize();
        for (IRI instance: instances) {
            for (IRI clazz : classes) {
                if (r.nextBoolean()) sail.addStatement(instance, RDF.TYPE, clazz);
            }
            for (IRI predicate : predicates) {
                if (r.nextBoolean()) sail.addStatement(instance, predicate, literals[r.nextInt(literals.length)]);
                if (r.nextBoolean()) sail.addStatement(instance, predicate, instances[r.nextInt(instances.length)]);
            }
        }
        sail.commit();
        sail.close();

        File summary = File.createTempFile("summary", ".nt");

        assertEquals(0, ToolRunner.run(HBaseServerTestInstance.getInstanceConfig(), new HalyardSummary(),
                new String[]{"-s", "summaryTable", "-t", summary.toURI().toURL().toString()}));

        try (BufferedReader in = new BufferedReader(new FileReader(summary))) {
            String line;
            while ((line = in.readLine()) != null) {
                System.out.println(line);
            }
//            Model m = Rio.parse(in, "http://whatever/", RDFFormat.TURTLE);
//            m.stream().forEach((Statement t) -> {
//                System.out.println(NTriplesUtil.t);
//            });
        }
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
