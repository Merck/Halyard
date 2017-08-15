/*
 * Copyright 2016 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
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
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.zip.GZIPOutputStream;
import org.apache.hadoop.util.ToolRunner;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardBulkLoadTest {

    @Test
    public void testBulkLoad() throws Exception {
        File root = File.createTempFile("test_triples", "");
        root.delete();
        root.mkdirs();
        try (PrintStream ps = new PrintStream(new File(root, "test_triples.jsonld"))) {
            ps.println("{\"@graph\": [");
            for (int i = 0; i < 10; i++) {
                if (i > 0) ps.print(',');
                ps.println("{\"@id\": \"http://whatever/subj" + i +"\"");
                for (int j=0; j < 10; j++) {
                    ps.println(",\"http://whatever/pred" + j +"\": [");
                    for (int k=0; k < 9; k++) {
                        if (k > 0) ps.print(',');
                        ps.print("\"literal" + k + "\"");
                    }
                    ps.println(']');
                }
                ps.println("}");
            }
            ps.println("]}");
        }
        try (PrintStream ps = new PrintStream(new GZIPOutputStream(new FileOutputStream(new File(root, "test_triples.nt.gz"))))) {
            for (int i = 0; i < 100; i++) {
                ps.println("<http://whatever/NTsubj> <http://whatever/NTpred" + i + "> \"whatever NT value" + i + "\" .");
            }
        }
        try (PrintStream ps = new PrintStream(new File(root, "test_triples_invalid.nt"))) {
            ps.println("this is an invalid NT file content");
        }
        File htableDir = File.createTempFile("test_htable", "");
        htableDir.delete();
        assertEquals(0, ToolRunner.run(HBaseServerTestInstance.getInstanceConfig(), new HalyardBulkLoad(), new String[]{"-Dhalyard.table.splitbits=-1", "-Dhalyard.parser.skipinvalid=true", root.toURI().toURL().toString(), htableDir.toURI().toURL().toString(), "bulkLoadTable"}));

        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "bulkLoadTable", false, 0, true, 0, null, null);
        SailRepository rep = new SailRepository(sail);
        rep.initialize();
        TupleQuery q = rep.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, "select (count(*) as ?c) where {?s ?p ?o}");
        TupleQueryResult res = q.evaluate();
        assertTrue(res.hasNext());
        Value c = res.next().getValue("c");
        assertNotNull(c);
        assertTrue(c instanceof Literal);
        assertEquals(1000, ((Literal)c).intValue());
        rep.shutDown();
    }

    @Test
    public void testRunNoArgs() throws Exception {
        assertEquals(-1, new HalyardBulkLoad().run(new String[0]));
    }
}
