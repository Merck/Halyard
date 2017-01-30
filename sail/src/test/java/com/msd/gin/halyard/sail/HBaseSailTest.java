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
package com.msd.gin.halyard.sail;

import com.msd.gin.halyard.common.HBaseServerTestInstance;
import java.util.List;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.eclipse.rdf4j.IsolationLevel;
import org.eclipse.rdf4j.IsolationLevels;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.UnknownSailTransactionStateException;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HBaseSailTest {

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDataDir() throws Exception {
        new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null).getDataDir();
    }

    @Test
    public void testInitializeAndShutDown() throws Exception {
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null);
        assertFalse(sail.isOpen());
        sail.initialize();
        assertTrue(sail.isOpen());
        sail.shutDown();
        assertFalse(sail.isOpen());
    }

    @Test
    public void testIsWritable() throws Exception {
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertableRW", true, 0, true, 0, null);
        sail.initialize();
        HTableDescriptor desc = sail.table.getTableDescriptor();
        assertTrue(sail.isWritable());
        sail.shutDown();
        try (Connection con = ConnectionFactory.createConnection(HBaseServerTestInstance.getInstanceConfig())) {
            try (Admin ha = con.getAdmin()) {
                desc = new HTableDescriptor(desc);
                desc.setReadOnly(true);
                ha.modifyTable(desc.getTableName(), desc);
            }
        }
        sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), desc.getNameAsString(), true, 0, true, 0, null);
        sail.initialize();
        assertFalse(sail.isWritable());
        sail.shutDown();
    }

    @Test(expected = SailException.class)
    public void testWriteToReadOnly() throws Exception {
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertableRO", true, 0, true, 0, null);
        sail.initialize();
        try {
            HTableDescriptor desc = sail.table.getTableDescriptor();
            try (Connection con = ConnectionFactory.createConnection(HBaseServerTestInstance.getInstanceConfig())) {
                try (Admin ha = con.getAdmin()) {
                    desc = new HTableDescriptor(desc);
                    desc.setReadOnly(true);
                    ha.modifyTable(desc.getTableName(), desc);
                }
            }
            ValueFactory vf = SimpleValueFactory.getInstance();
            sail.addStatement(vf.createIRI("http://whatever/subj"), vf.createIRI("http://whatever/pred"), vf.createLiteral("whatever"));
        } finally {
            sail.shutDown();
        }
    }

    @Test
    public void testGetConnection() throws Exception {
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null);
        assertSame(sail, sail.getConnection());
    }

    @Test
    public void testGetValueFactory() throws Exception {
        assertSame(SimpleValueFactory.getInstance(), new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null).getValueFactory());
    }

    @Test
    public void testGetSupportedIsolationLevels() throws Exception {
        List<IsolationLevel> il = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null).getSupportedIsolationLevels();
        assertEquals(1, il.size());
        assertTrue(il.contains(IsolationLevels.NONE));
    }

    @Test
    public void testGetDefaultIsolationLevel() throws Exception {
        assertSame(IsolationLevels.NONE, new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null).getDefaultIsolationLevel());
    }

    @Test
    public void testGetContextIDs() throws Exception {
        assertFalse(new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null).getContextIDs().hasNext());
    }

    @Test
    public void testSize() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertablesize", true, 0, true, 0, null);
        sail.initialize();
        for (int i=0; i<100; i++) {
            sail.addStatement(vf.createIRI("http://whatever/subj/" + i), vf.createIRI("http://whatever/pred/" + i), vf.createLiteral(i));
        }
        sail.commit();
        try (Connection con = ConnectionFactory.createConnection(HBaseServerTestInstance.getInstanceConfig())) {
            try (Admin ha = con.getAdmin()) {
                ha.flush(TableName.valueOf("whatevertablesize"));
            }
        }
        assertEquals(100, sail.size());
    }

    @Test(expected = UnknownSailTransactionStateException.class)
    public void testBegin() throws Exception {
        new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null).begin(IsolationLevels.READ_COMMITTED);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRollback() throws Exception {
        new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null).rollback();
    }

    @Test
    public void testIsActive() throws Exception {
        assertTrue(new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null).isActive());
    }

    @Test
    public void testNamespaces() throws Exception {
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null);
        sail.initialize();
        assertFalse(sail.getNamespaces().hasNext());
        sail.setNamespace("prefix", "http://whatever/namespace/");
        sail.shutDown();
        sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", false, 0, true, 0, null);
        sail.initialize();
        assertTrue(sail.getNamespaces().hasNext());
        sail.removeNamespace("prefix");
        sail.shutDown();
        sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", false, 0, true, 0, null);
        sail.initialize();
        assertFalse(sail.getNamespaces().hasNext());
        sail.setNamespace("prefix", "http://whatever/namespace/");
        sail.setNamespace("prefix", "http://whatever/namespace2/");
        sail.shutDown();
        sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", false, 0, true, 0, null);
        sail.initialize();
        assertEquals("http://whatever/namespace2/", sail.getNamespace("prefix"));
        sail.clearNamespaces();
        sail.shutDown();
        sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", false, 0, true, 0, null);
        sail.initialize();
        assertFalse(sail.getNamespaces().hasNext());
        sail.shutDown();
    }

    @Test
    public void testClear() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        Resource subj = vf.createIRI("http://whatever/subj/");
        IRI pred = vf.createIRI("http://whatever/pred/");
        Value obj = vf.createLiteral("whatever");
        IRI context = vf.createIRI("http://whatever/context/");
        CloseableIteration<? extends Statement, SailException> iter;
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null);
        sail.initialize();
        sail.addStatement(subj, pred, obj, context);
        sail.commit();
        iter = sail.getStatements(subj, pred, obj, true);
        assertTrue(iter.hasNext());
        iter.close();
        sail.clear(context);
        iter = sail.getStatements(subj, pred, obj, true);
        assertTrue(iter.hasNext());
        iter.close();
        sail.clear();
        iter = sail.getStatements(subj, pred, obj, true);
        assertFalse(iter.hasNext());
        iter.close();
    }

    @Test
    public void testEvaluate() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        Resource subj = vf.createIRI("http://whatever/subj/");
        IRI pred = vf.createIRI("http://whatever/pred/");
        Value obj = vf.createLiteral("whatever");
        CloseableIteration<? extends Statement, SailException> iter;
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null);
        SailRepository rep = new SailRepository(sail);
        rep.initialize();
        sail.addStatement(subj, pred, obj);
        sail.commit();
        TupleQuery q = rep.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, "select ?s ?p ?o where {<http://whatever/subj/> <http://whatever/pred/> \"whatever\"}");
        TupleQueryResult res = q.evaluate();
        assertTrue(res.hasNext());
        rep.shutDown();
    }

    @Test
    public void testEvaluateWithContext() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        Resource subj = vf.createIRI("http://whatever/subj/");
        IRI pred = vf.createIRI("http://whatever/pred/");
        Value obj = vf.createLiteral("whatever");
        IRI context = vf.createIRI("http://whatever/context/");
        CloseableIteration<? extends Statement, SailException> iter;
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null);
        SailRepository rep = new SailRepository(sail);
        rep.initialize();
        sail.addStatement(subj, pred, obj, context);
        sail.commit();
        TupleQuery q = rep.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, "select ?s ?p ?o from named <http://whatever/context/> where {<http://whatever/subj/> <http://whatever/pred/> \"whatever\"}");
        TupleQueryResult res = q.evaluate();
        assertFalse(res.hasNext());
        rep.shutDown();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testStatementsIteratorRemove1() throws Exception {
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null);
        try {
            sail.initialize();
            sail.getStatements(null, null, null, true).remove();
        } finally {
            sail.shutDown();
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testStatementsIteratorRemove2() throws Exception {
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null);
        try {
            sail.initialize();
            ValueFactory vf = SimpleValueFactory.getInstance();
            sail.getStatements(vf.createIRI("http://whatever/subj/"), vf.createIRI("http://whatever/pred/"), vf.createIRI("http://whatever/obj/"), true).remove();
        } finally {
            sail.shutDown();
        }
    }

    @Test
    public void testEmptyMethodsThatShouldDoNothing() throws Exception {
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 0, null);
        sail.setDataDir(null);
        sail.prepare();
        sail.begin();
        sail.flush();
        sail.startUpdate(null);
        sail.endUpdate(null);
        sail.close();
    }

    @Test(expected = SailException.class)
    public void testTimeoutGetStatements() throws Exception {
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), "whatevertable", true, 0, true, 1, null);
        sail.initialize();
        try {
            sail.commit();
            CloseableIteration<? extends Statement, SailException> it = sail.getStatements(null, null, null, true);
            Thread.sleep(2000);
            it.hasNext();
        } finally {
            sail.shutDown();
        }
    }
}
