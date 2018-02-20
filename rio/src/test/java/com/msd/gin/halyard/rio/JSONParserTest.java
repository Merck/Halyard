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
package com.msd.gin.halyard.rio;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collection;
import java.util.Iterator;
import java.util.TreeMap;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.ParserConfig;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.RioSetting;
import org.eclipse.rdf4j.rio.helpers.ContextStatementCollector;
import org.junit.Assert;
import static org.junit.Assert.assertNotNull;
import org.junit.Test;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class JSONParserTest {

    public static void assertEquals(Model expectedModel, Model transformedModel) {
        TreeMap<String,String> fail = new TreeMap<>();
        Iterator<Statement> it = expectedModel.iterator();
        while (it.hasNext()) {
            Statement st = it.next();
            if (!transformedModel.contains(st)) {
                fail.put(st.toString(), "-" + st.toString());
            }
        }
        it = transformedModel.iterator();
        while (it.hasNext()) {
            Statement st = it.next();
            if (!expectedModel.contains(st)) {
                fail.put(st.toString(), "+" + st.toString());
            }
        }
        if (fail.size() > 0) {
            StringBuilder sb = new StringBuilder();
            sb.append('\n');
            for (String line : fail.values()) {
                sb.append(line).append('\n');
            }
            Assert.fail(sb.toString());
        }
        Assert.assertEquals(expectedModel.size(), transformedModel.size());
    }

    @Test
    public void testJSONParserFactory() {
        assertNotNull(Rio.createParser(JSONParser.JSON));
    }

    @Test
    public void testJSONParser() throws Exception {
        Model transformedModel = new LinkedHashModel();
        RDFParser parser = new JSONParser();
        parser.setValueFactory(SimpleValueFactory.getInstance());
        parser.set(JSONParser.GENERATE_ONTOLOGY, true);
        parser.setRDFHandler(new ContextStatementCollector(transformedModel, SimpleValueFactory.getInstance()));
        parser.parse(JSONParserTest.class.getResourceAsStream("testJson.json"), "http://test/");

        Model expectedModel = Rio.parse(JSONParser.class.getResourceAsStream("testJson.ttl"), "http://test/", RDFFormat.TURTLE);

        assertEquals(expectedModel, transformedModel);
    }

    @Test
    public void testGetRDFFormat() {
        Assert.assertEquals(JSONParser.JSON, new JSONParser().getRDFFormat());
    }

    @Test
    public void testGetSupportedSettings() {
        Collection<RioSetting<?>> set = new JSONParser().getSupportedSettings();
        Assert.assertTrue(set.contains(JSONParser.GENERATE_DATA));
        Assert.assertTrue(set.contains(JSONParser.GENERATE_ONTOLOGY));
    }

    @Test
    public void testMethodsThatDoNothing() {
        RDFParser p = new JSONParser();
        p.setVerifyData(true);
        p.setPreserveBNodeIDs(true);
        p.setStopAtFirstError(true);
        p.setDatatypeHandling(RDFParser.DatatypeHandling.NORMALIZE);
        p.setParseErrorListener(null);
        p.setParseLocationListener(null);

    }

    @Test
    public void testParserSettings() {
        RDFParser p = new JSONParser();
        ParserConfig pc = new ParserConfig();
        Assert.assertSame(pc, p.setParserConfig(pc).getParserConfig());
    }

    @Test
    public void testParserEmpty() throws IOException {
        Model transformedModel = new LinkedHashModel();
        RDFParser parser = new JSONParser();
        parser.setRDFHandler(new ContextStatementCollector(transformedModel, SimpleValueFactory.getInstance()));
        parser.parse(new StringReader("[]"), "http://test/");
        System.out.println(transformedModel);
        Assert.assertTrue(transformedModel.isEmpty());
    }

    @Test
    public void testParserEmptyObj() throws IOException {
        Model transformedModel = new LinkedHashModel();
        RDFParser parser = new JSONParser();
        parser.setRDFHandler(new ContextStatementCollector(transformedModel, SimpleValueFactory.getInstance()));
        parser.parse(new StringReader("{}"), "http://test/");
        Assert.assertEquals(1, transformedModel.size());
        Assert.assertTrue(transformedModel.contains(null, RDF.TYPE, SimpleValueFactory.getInstance().createIRI("http://test/:Node"), null));
    }

    @Test (expected = IllegalArgumentException.class)
    public void testParseWrongJSON() throws IOException {
        new JSONParser().parse(new StringReader("true"), "http://test/");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testParseInvalidURI() throws IOException  {
        new JSONParser().parse(new StringReader("[]"), "invalid_uri");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testParserMultiObj() throws IOException {
        Model transformedModel = new LinkedHashModel();
        RDFParser parser = new JSONParser();
        parser.setRDFHandler(new ContextStatementCollector(transformedModel, SimpleValueFactory.getInstance()));
        parser.parse(new StringReader("{}{}"), "http://test/");
    }

}
