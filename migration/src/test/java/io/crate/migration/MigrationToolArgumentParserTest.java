/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.migration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.file.Paths;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class MigrationToolArgumentParserTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testParseArgsWrongZeroArgs() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("ERROR: wrong number of arguments.");
        MigrationToolArgumentParser.parseArgs(new String[] {});
    }

    @Test
    public void testParseArgsNoConfigFile() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("ERROR: missing value for -c option.");
        MigrationToolArgumentParser.parseArgs(new String[] {"-c"});
    }

    @Test
    public void testParseArgsBothAllTablesAndTables() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("ERROR: Both --all-tables and --tables are specified.");
        MigrationToolArgumentParser.parseArgs(new String[] {"--all-tables", "--tables"});
    }

    @Test
    public void testParseArgsNeitherAllTablesAndTables() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("ERROR: Wrong argument provided: foo");
        MigrationToolArgumentParser.parseArgs(new String[] {"foo"});
    }

    @Test
    public void testParseArgsAllTables() {
        System.setProperty("es.path.home", "foo");
        MigrationToolConfiguration configuration = MigrationToolArgumentParser.parseArgs(
            new String[] {"-c", "crate.yml", "--all-tables", "--dry-run"});
        assertThat(configuration.isDryrun(), is(true));
        assertThat(configuration.isVerbose(), is(false));
        assertThat(configuration.configFileName(), is("crate.yml"));
        assertThat(configuration.tableNames().isEmpty(), is(true));
        assertThat(configuration.environment().configFile(), is(Paths.get("foo","config")));
    }

    @Test
    public void testParseArgsSpecificTables() {
        System.setProperty("es.path.home", "foo");
        MigrationToolConfiguration configuration = MigrationToolArgumentParser.parseArgs(
            new String[] {"--verbose", "--tables", "table1,table2, table3 , table4"});
        assertThat(configuration.isDryrun(), is(false));
        assertThat(configuration.isVerbose(), is(true));
        assertThat(configuration.configFileName(), nullValue());
        assertThat(configuration.tableNames(), contains("table1", "table2", "table3", "table4"));
        assertThat(configuration.environment().configFile(), is(Paths.get("foo","config")));
    }

    @Test
    public void testParseArgsSpecificTablesMissingArg() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("ERROR: missing value for --tables option.");
        MigrationToolArgumentParser.parseArgs(new String[] {"--tables"});
    }
}
