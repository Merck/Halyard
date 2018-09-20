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

import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.junit.Test;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardMainTest {

    @Test
    public void testEmpty() throws Exception {
        HalyardMain.main(new String[]{});
    }

    @Test
    public void testVersion() throws Exception {
        HalyardMain.main(new String[]{"-v"});
    }

    @Test
    public void testVersionLong() throws Exception {
        HalyardMain.main(new String[]{"--version"});
    }

    @Test
    public void testHelp() throws Exception {
        HalyardMain.main(new String[]{"-h"});
    }

    @Test
    public void testHelpLong() throws Exception {
        HalyardMain.main(new String[]{"--help"});
    }

    @Test(expected = UnrecognizedOptionException.class)
    public void testInvalid() throws Exception {
        HalyardMain.main(new String[]{"invalid"});
    }

    @Test(expected = MissingOptionException.class)
    public void testPresplit() throws Exception {
        HalyardMain.main(new String[]{"presplit"});
    }

    @Test(expected = MissingOptionException.class)
    public void testBulkload() throws Exception {
        HalyardMain.main(new String[]{"bulkload"});
    }

    @Test(expected = MissingOptionException.class)
    public void testStats() throws Exception {
        HalyardMain.main(new String[]{"stats"});
    }

    @Test(expected = MissingOptionException.class)
    public void testEsindex() throws Exception {
        HalyardMain.main(new String[]{"esindex"});
    }

    @Test(expected = MissingOptionException.class)
    public void testUpdate() throws Exception {
        HalyardMain.main(new String[]{"update"});
    }

    @Test(expected = MissingOptionException.class)
    public void testBulkupdate() throws Exception {
        HalyardMain.main(new String[]{"bulkupdate"});
    }

    @Test(expected = MissingOptionException.class)
    public void testExport() throws Exception {
        HalyardMain.main(new String[]{"export"});
    }

    @Test(expected = MissingOptionException.class)
    public void testBulkexport() throws Exception {
        HalyardMain.main(new String[]{"bulkexport"});
    }

    @Test(expected = MissingOptionException.class)
    public void testBulkdelete() throws Exception {
        HalyardMain.main(new String[]{"bulkdelete"});
    }

    @Test(expected = MissingOptionException.class)
    public void testProfile() throws Exception {
        HalyardMain.main(new String[]{"profile"});
    }

    public void testConstructor() {
        new HalyardMain();
    }

    @Test(expected = RuntimeException.class)
    public void testExit() throws Exception {
        HalyardMain.main(new String[]{"bulkload", "-h"});
    }
}
