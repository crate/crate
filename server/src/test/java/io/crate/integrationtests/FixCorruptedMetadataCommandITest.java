/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.integrationtests;

import static io.crate.testing.TestingHelpers.printedTable;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.cluster.coordination.ElasticsearchNodeCommand;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import io.crate.cluster.commands.FixCorruptedMetadataCommand;
import io.crate.server.cli.MockTerminal;
import joptsimple.OptionParser;

@IntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0)
public class FixCorruptedMetadataCommandITest extends IntegTestCase {

    @Test
    public void test_swap_table_partitioned_to_partitioned_dotted_target() throws Exception {
        startUpNodeWithDataDir("/indices/data_home/cratedata-5.1.2-swap-table-bug-#13380.zip");
        //create table m1.t1 (t boolean) partitioned by (t) with (refresh_interval = 300);
        //create table m1.s1 (s char) partitioned by (s) with (refresh_interval = 400);
        //insert into m1.t1 values (true), (false), (false);
        //insert into m1.s1 values ('a'), ('a'), ('b');
        //alter cluster swap table "m1"."t1" to "m1.s1";

        // table t1 properly swapped to s1
        execute("select * from m1.s1 order by t");
        assertThat(printedTable(response.rows())).isEqualTo("false\nfalse\ntrue\n");
        execute("show create table m1.s1");
        assertThat(printedTable(response.rows())).contains(
            List.of(
                """
                    CREATE TABLE IF NOT EXISTS "m1"."s1" (
                       "t" BOOLEAN
                    )
                    """,
                "refresh_interval = 300",
                "PARTITIONED BY (\"t\")")
        );

        execute("select * from m1.t1");
        assertThat(printedTable(response.rows())).isEqualTo("NULL\nNULL\n");
        execute("show create table m1.t1");
        assertThat(printedTable(response.rows())).contains(
            List.of(
                """
                    CREATE TABLE IF NOT EXISTS "m1"."t1" (
                       "s" CHARACTER(1)
                    )
                    """,
                "refresh_interval = 400",
                "PARTITIONED BY (\"s\")"
            )
        );
        execute("insert into m1.t1 values (null), ('x'), ('y')"); // just in case...
        refresh();
        execute("select * from m1.t1");
        assertThat(printedTable(response.rows())).isEqualTo("NULL\nNULL\nNULL\nx\ny\n");

        verify_drop_create_insert_select("m1", List.of("t1", "s1"));
    }

    @Test
    public void test_swap_table_partitioned_dotted_src_to_partitioned_dotted_target() throws Exception {
        startUpNodeWithDataDir("/indices/data_home/cratedata-5.1.2-swap-table-bug-#13380.zip");
        //create table m3.t3 (t boolean) partitioned by (t) with (refresh_interval = 300);
        //create table m3.s3 (s char) partitioned by (s) with (refresh_interval = 400);
        //insert into m3.t3 values (true), (false), (false);
        //insert into m3.s3 values ('a'), ('a'), ('b');
        //alter cluster swap table "m3.t3" to "m3.s3";

        //Very similar to test_swap_table_partitioned_to_partitioned_dotted_target
        //Basically similar corruption occurs to both the src and the target table such that
        //table structure remains but the data are unrecoverable.
        execute("select * from m3.t3");
        assertThat(printedTable(response.rows())).isEqualTo("NULL\nNULL\n");
        execute("show create table m3.t3");
        assertThat(printedTable(response.rows())).contains(
            List.of(
                """
                    CREATE TABLE IF NOT EXISTS "m3"."t3" (
                       "s" CHARACTER(1)
                    )
                    """,
                "refresh_interval = 400",
                "PARTITIONED BY (\"s\")")
        );

        execute("select * from m3.s3");
        assertThat(printedTable(response.rows())).isEqualTo("NULL\n");
        execute("show create table m3.s3");
        assertThat(printedTable(response.rows())).contains(
            List.of(
                """
                    CREATE TABLE IF NOT EXISTS "m3"."s3" (
                       "t" BOOLEAN
                    )
                    """,
                "refresh_interval = 300",
                "PARTITIONED BY (\"t\")")
        );

        verify_drop_create_insert_select("m3", List.of("t3", "s3"));
    }

    @Test
    public void test_swap_table_non_partitioned_dotted_src_to_non_partitioned_dotted_target() throws Exception {
        startUpNodeWithDataDir("/indices/data_home/cratedata-5.1.2-swap-table-bug-#13380.zip");
        //create table m4.t4 (t char) with (refresh_interval = 300);
        //create table m4.s4 (s boolean) with (refresh_interval = 400);
        //insert into m4.t4 values ('a'), ('a'), ('b');
        //insert into m4.s4 values (true), (false), (false);
        //alter cluster swap table "m4.t4" to "m4.s4";
        execute("select * from m4.t4 order by s");
        assertThat(printedTable(response.rows())).isEqualTo("false\nfalse\ntrue\n");
        execute("show create table m4.t4");
        assertThat(printedTable(response.rows())).contains(
            List.of(
                """
                    CREATE TABLE IF NOT EXISTS "m4"."t4" (
                       "s" BOOLEAN
                    )
                    """,
                "refresh_interval = 400"
            )
        );
        execute("insert into m4.t4 values (null), (true)");

        execute("select * from m4.s4 order by t");
        assertThat(printedTable(response.rows())).isEqualTo("a\na\nb\n");
        execute("show create table m4.s4");
        assertThat(printedTable(response.rows())).contains(
            List.of(
                """
                    CREATE TABLE IF NOT EXISTS "m4"."s4" (
                       "t" CHARACTER(1)
                    )
                    """,
                "refresh_interval = 300"
            )
        );
        execute("insert into m4.s4 values ('x'), ('y')");

        verify_drop_create_insert_select("m4", List.of("t4", "s4"));
    }

    @Test
    public void test_swap_table_partitioned_to_non_partitioned_dotted_target() throws Exception {
        startUpNodeWithDataDir("/indices/data_home/cratedata-5.1.2-swap-table-bug-#13380.zip");
        //create table m5.t5 (t boolean) partitioned by (t) with (refresh_interval = 300);
        //create table m5.s5 (s char) with (refresh_interval = 400);
        //insert into m5.t5 values (true), (false), (false);
        //insert into m5.s5 values ('a'), ('a'), ('b');
        //alter cluster swap table "m5"."t5" to "m5.s5";

        // both tables are properly swapped
        execute("select * from m5.t5 order by s");
        assertThat(printedTable(response.rows())).isEqualTo("a\na\nb\n");
        execute("show create table m5.t5");
        assertThat(printedTable(response.rows())).contains(
            List.of(
                """
                    CREATE TABLE IF NOT EXISTS "m5"."t5" (
                       "s" CHARACTER(1)
                    )
                    """,
                "refresh_interval = 400"
            )
        ).doesNotContain("PARTITIONED BY");

        execute("select * from m5.s5 order by t");
        assertThat(printedTable(response.rows())).isEqualTo("false\nfalse\ntrue\n");
        execute("show create table m5.s5");
        assertThat(printedTable(response.rows())).contains(
            List.of(
                """
                    CREATE TABLE IF NOT EXISTS "m5"."s5" (
                       "t" BOOLEAN
                    )
                    """,
                "refresh_interval = 300",
                "PARTITIONED BY (\"t\")"
            )
        );

        verify_drop_create_insert_select("m5", List.of("t5", "s5"));
    }

    @Test
    public void test_swap_table_partitioned_dotted_src_to_non_partitioned() throws Exception {
        startUpNodeWithDataDir("/indices/data_home/cratedata-5.1.2-swap-table-bug-#13380.zip");
        //create table m6.t6 (t boolean) partitioned by (t) with (refresh_interval = 300);
        //create table m6.s6 (s char) with (refresh_interval = 400);
        //insert into m6.t6 values (true), (false), (false);
        //insert into m6.s6 values ('a'), ('a'), ('b');
        //alter cluster swap table "m6.t6" to "m6"."s6";

        execute("select * from m6.t6 order by s");
        assertThat(printedTable(response.rows())).isEqualTo("a\na\nb\n");
        execute("show create table m6.t6");
        assertThat(printedTable(response.rows())).contains(
            List.of(
                """
                    CREATE TABLE IF NOT EXISTS "m6"."t6" (
                       "s" CHARACTER(1)
                    )
                    """,
                "refresh_interval = 400"
            )
        ).doesNotContain("PARTITIONED BY");

        execute("select * from m6.s6");
        assertThat(printedTable(response.rows())).isEqualTo("NULL\n");
        execute("show create table m6.s6");
        assertThat(printedTable(response.rows())).contains(
            List.of(
                """
                    CREATE TABLE IF NOT EXISTS "m6"."s6" (
                       "t" BOOLEAN
                    )
                    """,
                "PARTITIONED BY (\"t\")",
                "refresh_interval = 300"
            )
        );

        verify_drop_create_insert_select("m6", List.of("t6", "s6"));
    }

    @Test
    @TestLogging("org.elasticsearch.index.shard.IndexShard:TRACE,io.crate.metadata.doc.DocTableInfoFactory:DEBUG,io.crate.metadata.doc.DocSchemaInfo:DEBUG")
    public void test_swap_table_partitioned_dotted_src_to_non_partitioned_dotted_target() throws Exception {
        startUpNodeWithDataDir("/indices/data_home/cratedata-5.1.2-swap-table-bug-#13380.zip");
        //create table m7.t7 (t boolean) partitioned by (t) with (refresh_interval = 300);
        //create table m7.s7 (s char) with (refresh_interval = 400);
        //insert into m7.t7 values (true), (false), (false);
        //insert into m7.s7 values ('a'), ('a'), ('b');
        //alter cluster swap table "m7.t7" to "m7.s7";


        execute("select * from m7.t7 order by s");
        assertThat(printedTable(response.rows())).isEqualTo("a\na\nb\n");
        execute("show create table m7.t7");
        assertThat(printedTable(response.rows())).contains(
            List.of(
                """
                    CREATE TABLE IF NOT EXISTS "m7"."t7" (
                       "s" CHARACTER(1)
                    )
                    """,
                "refresh_interval = 400"
            )
        ).doesNotContain("PARTITIONED BY");
        execute("insert into m7.t7 values ('x'), ('y')");

        execute("select * from m7.s7 order by t");
        assertThat(printedTable(response.rows())).isEqualTo("NULL\nNULL\n");
        execute("show create table m7.s7");
        assertThat(printedTable(response.rows())).contains(
            List.of(
                """
                    CREATE TABLE IF NOT EXISTS "m7"."s7" (
                       "t" BOOLEAN
                    )
                    """,
                "PARTITIONED BY (\"t\")",
                "refresh_interval = 300"
            )
        );
        execute("insert into m7.s7 values (true), (null), (false)");
        refresh();
        execute("select * from m7.s7 order by t desc");
        assertThat(printedTable(response.rows())).isEqualTo("NULL\nNULL\nNULL\ntrue\nfalse\n");

        verify_drop_create_insert_select("m7", List.of("t7", "s7"));
    }

    @Test
    public void test_swap_tables_does_not_affect_tables_with_identical_names_in_different_schemas() throws Exception {
        startUpNodeWithDataDir("/indices/data_home/cratedata-5.1.2-swap-table-bug-#13380-2.zip");

        //cr> create table m10.s10 (s char not null, ss char) partitioned by (s,ss);
        //CREATE OK, 1 row affected  (0.136 sec)
        //cr> create table m10.t10 (t boolean, tt boolean not null) partitioned by (t,tt);
        //CREATE OK, 1 row affected  (0.056 sec)
        //cr> create table s10 (s char not null, ss char) partitioned by (s,ss);
        //CREATE OK, 1 row affected  (0.054 sec)
        //cr> create table t10 (t boolean, tt boolean not null) partitioned by (t,tt);
        //CREATE OK, 1 row affected  (0.051 sec)
        //cr> insert into m10.s10 values ('a','a'), ('b','b');
        //INSERT OK, 2 rows affected  (0.428 sec)
        //cr> insert into m10.t10 values (true,true), (false, false), (true, false);
        //INSERT OK, 3 rows affected  (0.496 sec)
        //cr> insert into s10 values ('c','c'), ('d','d');
        //INSERT OK, 2 rows affected  (0.380 sec)
        //cr> insert into t10 values (null,true), (false, true), (null, false);
        //INSERT OK, 3 rows affected  (0.505 sec)
        //cr> alter cluster swap table "m10.s10" to "m10.t10";
        //ALTER OK, 1 row affected  (0.373 sec)

        // partially recovered
        execute("select * from m10.s10 order by t");
        assertThat(printedTable(response.rows())).isEqualTo("NULL| NULL\n");
        execute("show create table m10.s10");
        assertThat(printedTable(response.rows())).contains(
            List.of(
                "CREATE TABLE IF NOT EXISTS \"m10\".\"s10\" (",
                "\"t\" BOOLEAN",
                "\"tt\" BOOLEAN NOT NULL",
                "PARTITIONED BY (\"t\", \"tt\")"
            )
        );
        execute("insert into m10.s10 values (true, true), (false, true)");
        refresh();
        execute("select * from m10.s10 order by t");
        assertThat(printedTable(response.rows())).isEqualTo("false| true\ntrue| true\nNULL| NULL\n");

        // partially recovered
        execute("select * from m10.t10 order by s");
        assertThat(printedTable(response.rows())).isEqualTo("NULL| NULL\n");
        execute("show create table m10.t10");
        assertThat(printedTable(response.rows())).contains(
            List.of(
                """
                    CREATE TABLE IF NOT EXISTS "m10"."t10" (
                       "s" CHARACTER(1) NOT NULL,
                       "ss" CHARACTER(1)
                    )
                    """,
                "PARTITIONED BY (\"s\", \"ss\")"
            )
        );
        execute("insert into m10.t10 values ('t', 't'), ('q', 'q')");
        refresh();
        execute("select * from m10.t10 order by s");
        assertThat(printedTable(response.rows())).isEqualTo("q| q\nt| t\nNULL| NULL\n");


        // tables with same names in doc schema are not affected
        execute("select * from doc.s10");
        assertThat(printedTable(response.rows())).isEqualTo("c| c\nd| d\n");

        execute("select * from doc.t10");
        assertThat(printedTable(response.rows())).isEqualTo("NULL| true\nNULL| false\nfalse| true\n");


        verify_drop_create_insert_select("m10", List.of("t10", "s10"));
        verify_drop_create_insert_select("doc", List.of("t10", "s10"));
    }

    @Test
    public void test_swap_tables_causes_column_losses_depending_on_the_last_remaining_partition() throws Exception {
        startUpNodeWithDataDir("/indices/data_home/cratedata-5.1.2-swap-table-bug-#13380-3.zip");
        //cr> create table m.t (o object, p int) partitioned by (p) with (column_policy='dynamic');
        //CREATE OK, 1 row affected  (0.127 sec)
        //cr> insert into m.t(o,p,q) values ({a={b=1}}, 1, 'abc');
        //INSERT OK, 1 row affected  (0.332 sec)
        //cr> insert into m.t(o,p,q) values ({c={d=3}}, 2, 'abcd');
        //INSERT OK, 1 row affected  (0.218 sec)
        //cr> create table m.t2 (o object, p int) partitioned by (p) with (column_policy='dynamic');
        //CREATE OK, 1 row affected  (0.036 sec)
        //cr> insert into m.t2(o2,p2,q2) values ({a2={b2=12}}, 12, 'abc2');
        //INSERT OK, 1 row affected  (0.203 sec)
        //cr> insert into m.t2(o2,p2,q2) values ({c2={d2=32}}, 22, 'abcd2');
        //INSERT OK, 1 row affected  (0.053 sec)
        //cr> alter cluster swap table "m.t" to "m.t2";
        //ALTER OK, 1 row affected  (0.354 sec)

        // m.t is not of interest

        execute("select * from m.t2");
        // notice that c and d, the sub-columns of o, are missing.
        assertThat(printedTable(response.rows())).isEqualTo("{a={b=1}}| NULL| abc\n");
        execute("show create table m.t2");
        assertThat(printedTable(response.rows())).contains(
            """
                CREATE TABLE IF NOT EXISTS "m"."t2" (
                   "o" OBJECT(DYNAMIC) AS (
                      "a" OBJECT(DYNAMIC) AS (
                         "b" BIGINT
                      )
                   ),
                   "p" INTEGER,
                   "q" TEXT
                """
        );
    }

    private void verify_drop_create_insert_select(String schema, List<String> tables) {
        for (String table : tables) {
            String fqn = schema + "." + table;
            execute("drop table if exists " + fqn);
            execute("create table " + fqn + " (q text) partitioned by (q)");
            execute("insert into " + fqn + " values ('aa'), ('ab'), ('bb')");
            execute("refresh table " + fqn);
            execute("select q from " + fqn + " order by q");
            assertThat(printedTable(response.rows())).isEqualTo("aa\nab\nbb\n");
        }
    }

    private void startUpNodeWithDataDir(String dataPath) throws Exception {
        Path indexDir = createTempDir();
        try (InputStream stream = Files.newInputStream(getDataPath(dataPath))) {
            TestUtil.unzip(stream, indexDir);
        }
        Settings settings =
                Settings.builder().put(Environment.PATH_DATA_SETTING.getKey(), indexDir.toAbsolutePath()).build();

        Environment environment = TestEnvironment.newEnvironment(
                Settings.builder().put(cluster().getDefaultSettings()).put(settings).build());
        MockTerminal terminal = executeCommand(environment);
        assertThat(terminal.getOutput()).contains(FixCorruptedMetadataCommand.CONFIRMATION_MSG);
        assertThat(terminal.getOutput()).contains(FixCorruptedMetadataCommand.METADATA_FIXED_MSG);

        cluster().startNode(settings);
        ensureGreen();
    }

    private MockTerminal executeCommand(Environment environment) throws Exception {
        final MockTerminal terminal = new MockTerminal();
        final String input = randomValueOtherThanMany(c -> c.equalsIgnoreCase("y"), () -> randomAlphaOfLength(1));

        terminal.addTextInput(randomFrom("y", "Y"));
        try (var command = new FixCorruptedMetadataCommand()) {
            command.execute(terminal, new OptionParser().parse(), environment);
        } finally {
            assertThat(terminal.getOutput()).contains(ElasticsearchNodeCommand.STOP_WARNING_MSG);
        }

        return terminal;
    }
}
