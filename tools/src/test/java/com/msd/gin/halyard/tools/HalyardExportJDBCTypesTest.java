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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import org.apache.hadoop.util.ToolRunner;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.Test;
import org.junit.BeforeClass;
import static org.junit.Assert.*;
import org.junit.Rule;
import org.junit.rules.TestName;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardExportJDBCTypesTest {

    private static final String TABLE = "exportjdbctesttable";

    @Rule
    public TestName name = new TestName();

    @BeforeClass
    public static void setup() throws Exception {
        ValueFactory vf = SimpleValueFactory.getInstance();
        HBaseSail sail = new HBaseSail(HBaseServerTestInstance.getInstanceConfig(), TABLE, true, 0, true, 0, null, null);
        sail.initialize();
        for (int i=1; i<10; i++) {
            sail.addStatement(vf.createIRI("http://whatever/subj" + i), vf.createIRI("http://whatever/date"), vf.createLiteral(new Date(i, i, i)));
            Date d = new Date(i, i, i, i, i, i);
            sail.addStatement(vf.createIRI("http://whatever/subj" + i), vf.createIRI("http://whatever/time"), vf.createLiteral(d));
            sail.addStatement(vf.createIRI("http://whatever/subj" + i), vf.createIRI("http://whatever/timestamp"), vf.createLiteral(new Date(d.getTime() + i))); // add millis
            sail.addStatement(vf.createIRI("http://whatever/subj" + i), vf.createIRI("http://whatever/string"), vf.createLiteral("value" + i));
            sail.addStatement(vf.createIRI("http://whatever/subj" + i), vf.createIRI("http://whatever/boolean"), vf.createLiteral(i < 5));
            sail.addStatement(vf.createIRI("http://whatever/subj" + i), vf.createIRI("http://whatever/byte"), vf.createLiteral((byte)i));
            sail.addStatement(vf.createIRI("http://whatever/subj" + i), vf.createIRI("http://whatever/double"), vf.createLiteral((double)i/100.0));
            sail.addStatement(vf.createIRI("http://whatever/subj" + i), vf.createIRI("http://whatever/float"), vf.createLiteral((float)i/10.0));
            sail.addStatement(vf.createIRI("http://whatever/subj" + i), vf.createIRI("http://whatever/int"), vf.createLiteral(i * 100));
            sail.addStatement(vf.createIRI("http://whatever/subj" + i), vf.createIRI("http://whatever/long"), vf.createLiteral((long)i * 10000000000l));
            sail.addStatement(vf.createIRI("http://whatever/subj" + i), vf.createIRI("http://whatever/short"), vf.createLiteral((short)(i * 10)));
        }
        sail.commit();
        sail.shutDown();
    }


    @Test
    public void testExportJDBCTypes() throws Exception {
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        try (Connection c = DriverManager.getConnection("jdbc:derby:memory:halyard-export-types-test;create=true")) {
            c.createStatement().executeUpdate("create table " + name.getMethodName() + " (subj varchar(100), date date, time time, timestamp timestamp, string varchar(100), bool boolean, byte smallint, doubl double, floa float, itg integer, long bigint, short smallint)");
        }
        try {
            ToolRunner.run(HBaseServerTestInstance.getInstanceConfig(), new HalyardExport(), new String[] {"-s", TABLE, "-q", "PREFIX : <http://whatever/> select * where {?subj :date ?date; :time ?time; :timestamp ?timestamp; :string ?string; :boolean ?bool; :byte ?byte; :double ?doubl; :float ?floa; :int ?itg; :long ?long; :short ?short.}", "-t", "jdbc:derby:memory:halyard-export-types-test/" + name.getMethodName(), "-c", "org.apache.derby.jdbc.EmbeddedDriver", "-r"});
            try (Connection c = DriverManager.getConnection("jdbc:derby:memory:halyard-export-types-test")) {
                try (ResultSet rs = c.createStatement().executeQuery("select * from " + name.getMethodName())) {
                    ResultSetMetaData m = rs.getMetaData();
                    int count = m.getColumnCount();
                    for (int row = 1; row < 10; row++) {
                        assertTrue(rs.next());
                        int i = rs.getByte("byte");
                        assertEquals("http://whatever/subj" + i, rs.getString("subj"));
                        assertEquals(new java.sql.Date(i, i, i), rs.getDate("date"));
                        assertEquals(new Time(i, i, i), rs.getTime("time"));
                        assertEquals(new Timestamp(i, i, i, i, i, i, 1000000*i), rs.getTimestamp("timestamp"));
                        assertEquals("value" + i, rs.getString("string"));
                        assertEquals((double)i/100.0, rs.getDouble("doubl"), 0.001);
                        assertEquals((float)i/10.0, rs.getFloat("floa"), 0.01);
                        assertEquals(i * 100, rs.getInt("itg"));
                        assertEquals((long)i * 10000000000l, rs.getLong("long"));
                        assertEquals((short)(i * 10), rs.getShort("short"));
                    System.out.println();
                    }
                }
            }
        } finally {
            try {
                DriverManager.getConnection("jdbc:derby:memory:halyard-export-types-test;shutdown=true").close();
            } catch (SQLException ignore) {}
        }
    }
}
