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
package com.msd.gin.halyard.sail;

import com.msd.gin.halyard.common.HBaseServerTestInstance;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class SES2154SubselectOptionalTest {

    @Test
    public void testSES2154SubselectOptional() throws Exception {
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "SES2154SubselectOptionaltable", true, 0, true, 0, null, null);
        SailRepository rep = new SailRepository(sail);
        rep.initialize();
        SimpleValueFactory vf = SimpleValueFactory.getInstance();
        IRI person = vf.createIRI("http://schema.org/Person");
        for (char c = 'a'; c < 'k'; c++) {
            sail.addStatement(vf.createIRI("http://example.com/" + c), RDF.TYPE, person);
        }
        sail.commit();
        TupleQueryResult res = rep.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, "PREFIX : <http://example.com/>\n" + "PREFIX schema: <http://schema.org/>\n" + "\n" + "SELECT (COUNT(*) AS ?count)\n" + "WHERE {\n" + "  {\n" + "    SELECT ?person\n" + "    WHERE {\n" + "      ?person a schema:Person .\n" + "    }\n" + "    LIMIT 5\n" + "  }\n" + "  OPTIONAL {\n" + "    [] :nonexistent [] .\n" + "  }\n" + "}").evaluate();
        assertEquals(5, ((Literal) res.next().getBinding("count").getValue()).intValue());
        rep.shutDown();
    }

}
