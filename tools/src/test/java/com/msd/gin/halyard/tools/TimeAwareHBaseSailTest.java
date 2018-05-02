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
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class TimeAwareHBaseSailTest {

    @Test
    public void timestampLongTest() throws Exception {
        CloseableIteration<? extends Statement, SailException> iter;
        HBaseSail sail = new TimeAwareHBaseSail(HBaseServerTestInstance.getInstanceConfig(), "timestamptable", true, 0, true, 0, null, null);
        SailRepository rep = new SailRepository(sail);
        rep.initialize();
        SailRepositoryConnection con = rep.getConnection();
        assertTrue(testUpdate(con, "insert {<http://whatever> <http://whatever> <http://whatever>} where {bind(2 as ?HALYARD_TIMESTAMP_SPECIAL_VARIABLE)}"));
        assertTrue(testUpdate(con, "delete {<http://whatever> <http://whatever> <http://whatever>} where {bind(1 as ?HALYARD_TIMESTAMP_SPECIAL_VARIABLE)}"));
        assertFalse(testUpdate(con, "delete {<http://whatever> <http://whatever> <http://whatever>} where {bind(4 as ?HALYARD_TIMESTAMP_SPECIAL_VARIABLE)}"));
        assertFalse(testUpdate(con, "insert {<http://whatever> <http://whatever> <http://whatever>} where {bind(3 as ?HALYARD_TIMESTAMP_SPECIAL_VARIABLE)}"));
        assertTrue(testUpdate(con, "insert {<http://whatever> <http://whatever> <http://whatever>} where {bind(4 as ?HALYARD_TIMESTAMP_SPECIAL_VARIABLE)}"));
        rep.shutDown();

    }

    @Test
    public void timestampDateTimeTest() throws Exception {
        CloseableIteration<? extends Statement, SailException> iter;
        HBaseSail sail = new TimeAwareHBaseSail(HBaseServerTestInstance.getInstanceConfig(), "timestamptable", true, 0, true, 0, null, null);
        SailRepository rep = new SailRepository(sail);
        rep.initialize();
        SailRepositoryConnection con = rep.getConnection();
        assertTrue(testUpdate(con, "insert {<http://whatever> <http://whatever> <http://whatever>} where {bind(\"2002-05-30T09:30:10.2\"^^<http://www.w3.org/2001/XMLSchema#dateTime> as ?HALYARD_TIMESTAMP_SPECIAL_VARIABLE)}"));
        assertTrue(testUpdate(con, "delete {<http://whatever> <http://whatever> <http://whatever>} where {bind(\"2002-05-30T09:30:10.1\"^^<http://www.w3.org/2001/XMLSchema#dateTime> as ?HALYARD_TIMESTAMP_SPECIAL_VARIABLE)}"));
        assertFalse(testUpdate(con, "delete {<http://whatever> <http://whatever> <http://whatever>} where {bind(\"2002-05-30T09:30:10.4\"^^<http://www.w3.org/2001/XMLSchema#dateTime> as ?HALYARD_TIMESTAMP_SPECIAL_VARIABLE)}"));
        assertFalse(testUpdate(con, "insert {<http://whatever> <http://whatever> <http://whatever>} where {bind(\"2002-05-30T09:30:10.3\"^^<http://www.w3.org/2001/XMLSchema#dateTime> as ?HALYARD_TIMESTAMP_SPECIAL_VARIABLE)}"));
        assertTrue(testUpdate(con, "insert {<http://whatever> <http://whatever> <http://whatever>} where {bind(\"2002-05-30T09:30:10.4\"^^<http://www.w3.org/2001/XMLSchema#dateTime> as ?HALYARD_TIMESTAMP_SPECIAL_VARIABLE)}"));
        rep.shutDown();
    }

    private boolean testUpdate(SailRepositoryConnection con, String update) {
        Update u = con.prepareUpdate(update);
        ((MapBindingSet)u.getBindings()).addBinding(new TimeAwareHBaseSail.TimestampCallbackBinding());
        u.execute();
        con.commit();
        return con.prepareTupleQuery(QueryLanguage.SPARQL, "select * where {<http://whatever> ?p ?o}").evaluate().hasNext();
    }
}
