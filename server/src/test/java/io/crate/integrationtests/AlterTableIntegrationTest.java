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

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.protocols.postgres.PGErrorStatus.UNDEFINED_COLUMN;
import static io.crate.protocols.postgres.PGErrorStatus.UNDEFINED_TABLE;
import static io.crate.testing.Asserts.assertExpectedLogMessages;
import static io.crate.testing.Asserts.assertSQLError;
import static io.crate.testing.Asserts.assertThat;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Locale;

import org.apache.logging.log4j.Level;
import org.elasticsearch.test.IntegTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.junit.Test;

import io.crate.common.unit.TimeValue;
import io.crate.execution.ddl.tables.AlterTableClient;
import io.crate.testing.Asserts;
import io.crate.testing.TestingHelpers;
import io.crate.testing.UseNewCluster;

public class AlterTableIntegrationTest extends IntegTestCase {

    @Test
    public void test_create_soft_delete_setting_for_partitioned_tables() {
        assertSQLError(() -> execute(
                "create table test(i int) partitioned by (i) WITH(\"soft_deletes.enabled\" = false) "))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("Invalid property \"soft_deletes.enabled\" passed to [ALTER | CREATE] TABLE statement");
    }

    @Test
    public void test_alter_table_drop_column_dropped_meanwhile() {
        execute("CREATE TABLE t(a int, b int)");
        PlanForNode plan = plan("ALTER TABLE t DROP b");
        execute("ALTER TABLE t DROP COLUMN b");
        assertSQLError(() -> execute(plan).getResult())
            .hasMessageContaining("Column b unknown");
    }

    @Test
    public void test_alter_table_drop_simple_column() {
        execute("create table t(id integer primary key, a integer, b integer)");
        execute("insert into t(id, a, b) values(1, 11, 111)");
        execute("refresh table t");
        execute("select * from t");
        assertThat(response).hasRows("1| 11| 111");

        execute("alter table t drop column a");
        execute("select * from t");
        assertThat(response).hasRows("1| 111");
        Asserts.assertSQLError(() -> execute("select a from t"))
            .hasPGError(UNDEFINED_COLUMN)
            .hasHTTPError(NOT_FOUND, 4043)
            .hasMessageContaining("Column a unknown");
        execute("show create table t");
        var currentSchema = sqlExecutor.getCurrentSchema();
        assertThat((String) response.rows()[0][0]).startsWith(
            "CREATE TABLE IF NOT EXISTS \"" + currentSchema + "\".\"t\" (" +
            """

               "id" INTEGER NOT NULL,
               "b" INTEGER,
               PRIMARY KEY ("id")
            )
            """);
    }

    @Test
    public void test_alter_partitioned_table_drop_simple_column() {
        execute("create table t(id integer primary key, a integer, b integer) partitioned by(id)");
        execute("insert into t(id, a, b) values(1, 11, 111)");
        execute("insert into t(id, a, b) values(2, 22, 222)");
        execute("insert into t(id, a, b) values(3, 33, 333)");
        execute("refresh table t");
        execute("select * from t order by id");
        assertThat(response).hasRows(
            "1| 11| 111",
            "2| 22| 222",
            "3| 33| 333");

        execute("alter table t drop column a");
        execute("select * from t order by id");
        assertThat(response).hasRows(
            "1| 111",
            "2| 222",
            "3| 333");
        execute("select * from t where id = 2");
        assertThat(response).hasRows("2| 222");
        Asserts.assertSQLError(() -> execute("select a from t"))
            .hasPGError(UNDEFINED_COLUMN)
            .hasHTTPError(NOT_FOUND, 4043)
            .hasMessageContaining("Column a unknown");
    }

    @Test
    public void test_alter_table_drop_leaf_subcolumn() {
        // Use same names for sub-cols at different level of nesting
        execute("create table t(id integer primary key, o object AS(a int, b int, oo object AS(a int, b int)))");
        execute("insert into t(id, o) values(1, '{\"a\":1, \"b\":2, \"oo\":{\"a\":11, \"b\":22}}')");
        execute("refresh table t");
        execute("select * from t");
        assertThat(response).hasRows("1| {a=1, b=2, oo={a=11, b=22}}");

        execute("alter table t drop column o['oo']['b']");
        execute("select * from t");
        assertThat(response).hasRows("1| {a=1, b=2, oo={a=11}}");
        Asserts.assertSQLError(() -> execute("select o['oo']['b'] from t"))
            .hasPGError(UNDEFINED_COLUMN)
            .hasHTTPError(NOT_FOUND, 4043)
            .hasMessageContaining("Column o['oo']['b'] unknown");
    }

    @Test
    public void test_alter_table_drop_leaf_subcolumn_with_parent_object_array() {
        // Use same names for sub-cols at different level of nesting
        execute("create table t(id integer primary key, " +
                "o object AS(a int, b int, oo array(object AS(a int, b int, s int, t int)), s int, t int))");
        execute("insert into t(id, o) values(1, '{\"a\":1, \"b\":2, " +
                "\"oo\":[{\"a\":11, \"b\":22, \"s\":33, \"t\":44}], \"s\":3, \"t\":4}')");
        execute("refresh table t");
        execute("select * from t");
        assertThat(response).hasRows("1| {a=1, b=2, oo=[{a=11, b=22, s=33, t=44}], s=3, t=4}");

        execute("alter table t drop column o['oo']['b'], drop column o['oo']['t']");
        execute("select * from t");
        assertThat(response).hasRows("1| {a=1, b=2, oo=[{a=11, s=33}], s=3, t=4}");
        Asserts.assertSQLError(() -> execute("select o['oo']['b'] from t"))
            .hasPGError(UNDEFINED_COLUMN)
            .hasHTTPError(NOT_FOUND, 4043)
            .hasMessageContaining("Column o['oo']['b'] unknown");
    }

    @Test
    public void test_alter_table_drop_subcolumn_with_children() {
        // Use same names for sub-cols at different level of nesting
        execute("create table t(id integer primary key, o object AS(a int, b int, oo object AS(a int, b int)))");
        execute("insert into t(id, o) values(1, '{\"a\":1, \"b\":2, \"oo\":{\"a\":11, \"b\":22}}')");
        execute("refresh table t");
        execute("select * from t");
        assertThat(response).hasRows("1| {a=1, b=2, oo={a=11, b=22}}");

        execute("alter table t drop column o['oo']");
        execute("select * from t");
        assertThat(response).hasRows("1| {a=1, b=2}");
        Asserts.assertSQLError(() -> execute("select o['oo'] from t"))
            .hasPGError(UNDEFINED_COLUMN)
            .hasHTTPError(NOT_FOUND, 4043)
            .hasMessageContaining("Column o['oo'] unknown");

        // Ensure that we cannot select implicitly dropped column.
        // SELECT * is not enough to verify "cascade deletion" as it takes different code path.
        Asserts.assertSQLError(() -> execute("select o['oo']['a'] from t"))
            .hasPGError(UNDEFINED_COLUMN)
            .hasHTTPError(NOT_FOUND, 4043)
            .hasMessageContaining("Column o['oo']['a'] unknown");
    }

    @Test
    public void test_alter_table_drop_simple_column_view_updated() {
        execute("create table t(id integer primary key, a integer, b integer)");
        execute("insert into t(id, a, b) values(1, 11, 111)");
        execute("refresh table t");
        execute("create view vt as select id, b from t");
        execute("select * from vt");
        assertThat(response).hasRows("1| 111");

        execute("alter table t drop column a");
        execute("select * from vt");
        assertThat(response).hasRows("1| 111");
        Asserts.assertSQLError(() -> execute("select a from vt"))
            .hasPGError(UNDEFINED_COLUMN)
            .hasHTTPError(NOT_FOUND, 4043)
            .hasMessageContaining("Column a unknown");
    }

    @Test
    public void test_drop_sub_column_readd_and_update() {
        execute("CREATE TABLE t1 (id int, obj object as (x int, y int))");
        execute("INSERT INTO t1 (id, obj) VALUES (1, {x=11, y=21})");
        execute("refresh table t1");
        execute("SELECT id, obj FROM t1");
        assertThat(response).hasRows("1| {x=11, y=21}");

        execute("ALTER TABLE t1 DROP COLUMN obj['y']");
        execute("SELECT id, obj FROM t1");
        assertThat(response).hasRows("1| {x=11}");

        execute("ALTER TABLE t1 ADD COLUMN obj['y'] TEXT");
        execute("UPDATE t1 SET obj['y'] = 'foo'");
        assertThat(response.rowCount()).isEqualTo(1L);
        execute("refresh table t1");
        execute("SELECT id, obj FROM t1");
        assertThat(response).hasRows("1| {x=11, y=foo}");
    }

    @Test
    @UseNewCluster
    public void test_alter_table_drop_column_can_add_again() {
        execute("create table t(a integer, b integer, o object AS(a int, oo object AS(a int)))");
        execute("insert into t(a, b, o) values(1, 11, '{\"a\":111, \"oo\":{\"a\":1111}}')");
        execute("refresh table t");
        execute_statements_that_drop_and_add_column_with_the_same_name_again(false);
    }

    @Test
    @UseNewCluster
    public void test_alter_partitioned_table_drop_column_can_add_again() {
        // Method execute_statements_that_drop_and_add_column_with_the_same_name_again has some INSERT statements.
        // Using generated partitioned column in order not to adjust them.
        execute("create table t(a integer, b integer, o object AS(a int, oo object AS(a int)), p integer as b + 1) PARTITIONED BY (p)");
        execute("insert into t(a, b, o) values(1, 11, '{\"a\":111, \"oo\":{\"a\":1111}}')");
        execute("refresh table t");
        execute_statements_that_drop_and_add_column_with_the_same_name_again(true);
    }


    private void execute_statements_that_drop_and_add_column_with_the_same_name_again(boolean partitioned) {
        // Re-add top-level column with same name and different type.
        execute("alter table t drop column a");
        execute("alter table t add column a text");

        execute("insert into t(a, b, o) values('some-text', 222, '{\"a\":222, \"oo\":{\"a\":2222}}')");
        execute("refresh table t");
        execute("select _raw from t order by b");
        long newColumnOid = partitioned ? 8 : 7;
        // Old 'a' with OID 1 is gone in the second entry (we don't write NULL values to the source)
        // Instead, we have new 'a' with new oid having text value "some-text".
        String rawRows = """
            {"2": 11, "4": 111, "6": 1111}
            {"%d": "some-text", "2": 222," 3": {"5": {"6": 2222}, "4": 222}}
            """;
        assertThat(TestingHelpers.jsonMap(TestingHelpers.printedTable(response.rows())))
            .isEqualTo(TestingHelpers.jsonMap(String.format(Locale.ENGLISH, rawRows, newColumnOid)));

        // Ensure that re-added column is the last in 'SELECT *' output since it has the highest ordinal.
        // This is aligned with PG 14 behavior.
        execute("select * from t order by b");
        String[] columns = partitioned ? new String[]{"b", "o", "p", "a"} : new String[]{"b", "o", "a"};
        assertThat(response).hasColumns(columns);

        // Drop sub-column which in turn, has children column.
        // Re-add columns with the same names but leaf having different type.
        execute("alter table t drop column o['oo']");
        execute("alter table t add column o['oo'] object AS(a text)");

        execute("insert into t(a, b, o) values('another-text', 333, '{\"a\":333, \"oo\":{\"a\":\"hello\"}}')");
        execute("refresh table t");
        execute("select _raw from t where b = 333");
        // Root column 'o' and it's unaffected child o['a'] retained their OID-s (3 and 4).
        // Dropped children column got updated OID-s.
        rawRows = """
            {"%d": "another-text", "2": 333, "%d": "hello", "4": 333}
            """;
        assertThat(
            TestingHelpers.jsonMap(TestingHelpers.printedTable(response.rows()))
        ).isEqualTo(
            TestingHelpers.jsonMap(String.format(Locale.ENGLISH, rawRows, newColumnOid, newColumnOid + 2))
        );
    }

    @Test
    public void test_alter_table_can_add_column_after_dropping_column_with_max_known_position() {
        execute("create table t(a int, b int)");
        execute("alter table t drop column b");
        execute("alter table t add column c int");

        execute("""
            select attname, attnum
            from pg_attribute
            where attrelid = 't'::regclass AND attname = 'c'
            """
        );
        // New column gets position of the dropped column.
        assertThat(response).hasRows(
            "c| 2"
        );
    }

    @Test
    public void test_rename_columns() {
        execute("""
            create table doc.t (
                a int primary key,
                o object as (a int primary key, b int),
                c int generated always as (abs(a + o['a'])),
                constraint c_1 check (o['a'] < a + c)
            ) partitioned by (a, o['a'])
            """);
        execute("insert into doc.t(a, o) values (1, {a=2})");
        execute("insert into doc.t(a, o) values (4, {a=5})");
        execute("refresh table doc.t");

        execute("alter table doc.t rename column a to a2");
        execute("alter table doc.t rename column o to o2");
        execute("alter table doc.t rename column o2['a'] to o2['a22']");

        execute("show create table doc.t");
        assertThat((String) response.rows()[0][0]).contains(
                """
                    CREATE TABLE IF NOT EXISTS "doc"."t" (
                       "a2" INTEGER NOT NULL,
                       "o2" OBJECT(DYNAMIC) AS (
                          "a22" INTEGER NOT NULL,
                          "b" INTEGER
                       ),
                       "c" INTEGER GENERATED ALWAYS AS abs(("a2" + "o2"['a22'])),
                       PRIMARY KEY ("a2", "o2"['a22']),
                       CONSTRAINT c_1 CHECK("o2"['a22'] < ("a2" + "c"))
                    )
                    """.stripIndent())
            .contains("PARTITIONED BY (\"a2\", \"o2\"['a22'])");

        execute("select * from doc.t order by a2");
        assertThat(response.cols()).isEqualTo(new String[]{"a2", "o2", "c"});
        assertThat(response).hasRows("1| {a22=2}| 3", "4| {a22=5}| 9");

        execute("select attname from pg_attribute where attrelid = 'doc.t'::regclass");
        assertThat(response).hasRows("a2", "o2", "o2['a22']", "o2['b']", "c");

        execute("select column_name from information_schema.columns where table_name = 't'");
        assertThat(response).hasRows("a2", "o2", "o2['a22']", "o2['b']", "c");
    }

    @Test
    public void test_add_sub_column_to_ignored_parent_if_table_is_not_empty_logs_warning() throws Exception {
        execute("CREATE TABLE t1 (obj object(ignored))");
        execute("INSERT INTO t1 (obj) VALUES ({a={b=21, c=22}})");
        execute("refresh table t1");

        var expectation = new MockLogAppender.SeenEventExpectation(
            "Logs warning",
            AlterTableClient.class.getName(),
            Level.WARN,
            "Adding a sub column to an OBJECT(IGNORED) parent may shade existing data of this column as the table isn't empty"
        );
        assertExpectedLogMessages(
            () -> execute("ALTER TABLE t1 ADD COLUMN obj['a'] text"),
            AlterTableClient.class.getName(),
            expectation
        );
    }

    @Test
    public void test_can_add_sub_column_to_ignored_parent_if_table_is_empty() {
        execute("CREATE TABLE t1 (obj object(ignored))");
        execute("ALTER TABLE t1 ADD COLUMN obj['a'] text");
        execute("INSERT INTO t1 (obj) VALUES ({a='bar'})");
        execute("refresh table t1");
        execute("SELECT obj['a'] FROM t1");
        assertThat(response).hasRows("bar");
    }

    @Test
    public void test_can_add_sub_column_to_ignored_parent_if_table_is_not_empty_and_can_query_data_of_different_type() {
        // Setting translog to async to increase chances that the first record is processed after the column is added.
        execute("CREATE TABLE t1 (obj object(ignored)) WITH (\"translog.durability\" = 'async')");
        execute("INSERT INTO t1 (obj) VALUES ({a={b=21, c=22}})");
        execute("REFRESH TABLE t1");
        boolean enableColumnarStore = randomBoolean();
        execute("ALTER TABLE t1 ADD COLUMN obj['a'] text STORAGE WITH (COLUMNSTORE=" + enableColumnarStore + ")");
        execute("INSERT INTO t1 (obj) VALUES ({a='bar'})");
        execute("REFRESH TABLE t1");

        execute("SELECT obj['a'] FROM t1 ORDER BY obj['a'] NULLS FIRST");
        assertThat(response).hasRows("NULL", "bar");

        execute("SELECT obj['a'] FROM t1 WHERE obj['a'] IS NOT NULL ORDER BY obj['a']");
        assertThat(response).hasRows("bar");
    }

    @Test
    public void test_increase_num_shards_does_not_delete_source_index_on_alloc_failures() throws Exception {
        execute("create table tbl (x int, p int) clustered into 1 shards partitioned by (p) with (\"routing.allocation.total_shards_per_node\" = 2)");
        execute("insert into tbl (x, p) values (1, 1), (1, 2), (2, 1), (2, 2)");
        execute("refresh table tbl");
        execute("alter table tbl partition (p = 1) set (\"blocks.write\" = true)");
        int numShards = (cluster().numNodes() + 1) * 2;
        assertThatThrownBy(() ->
            execute("alter table tbl partition (p = 1) set (number_of_shards = ?)", new Object[] { numShards }, TimeValue.timeValueSeconds(30))
        ).extracting(ex -> ex.getMessage()).satisfiesAnyOf(
            x -> assertThat(x).contains("Resize operation wasn't acknowledged. Check shard allocation and retry"),
            x -> assertThat(x).contains("Timeout while running")
        );
        assertThat(execute("select * from tbl")).hasRowCount(4);

        execute("alter table tbl set (\"routing.allocation.total_shards_per_node\" = 200)");
        execute("alter table tbl partition (p = 1) set (number_of_shards = ?)", new Object[] { numShards }, TimeValue.timeValueSeconds(30));
        assertBusy(() -> assertThat(execute("select * from tbl")).hasRowCount(4));
    }

    @Test
    public void test_renamed_tables_can_be_queried_after_cluster_restart() throws Exception {
        execute("CREATE TABLE q1 (a INTEGER)");
        execute("CREATE TABLE q2 (a INTEGER)");

        execute("INSERT INTO q1 VALUES (1), (2)");
        execute("INSERT INTO q2 VALUES (3), (4)");
        execute("REFRESH TABLE q1, q2");

        execute("ALTER TABLE q1 RENAME TO q1_old");
        execute("ALTER TABLE q2 RENAME TO q1");

        cluster().fullRestart();

        ensureGreen();

        var res = execute("SELECT * FROM q1_old order by a");
        assertThat(res).hasRows("1", "2");

        res = execute("SELECT * FROM q1 order by a");
        assertThat(res).hasRows("3", "4");

        assertSQLError(() -> execute("SELECT * FROM q2"))
            .hasPGError(UNDEFINED_TABLE)
            .hasHTTPError(NOT_FOUND, 4041)
            .hasMessageContaining("Relation 'q2' unknown");

    }

    @Test
    public void test_reset_setting_on_complete_partitioned_table() {
        execute("CREATE TABLE t1 (a INTEGER, b INTEGER) PARTITIONED BY (a)");
        execute("INSERT INTO t1 VALUES (1, 2), (2, 3)");
        execute("ALTER TABLE t1 SET (\"blocks.write\" = TRUE)");
        execute("SELECT table_name, settings['blocks']['write'] FROM information_schema.tables WHERE table_name = 't1' ORDER BY 1");
        assertThat(response).hasRows(
            "t1| true"
        );
        execute("SELECT partition_ident, settings['blocks']['write'] FROM information_schema.table_partitions WHERE table_name = 't1' ORDER BY 1");
        assertThat(response).hasRows(
            "04132| true",
            "04134| true"
        );
        execute("ALTER TABLE t1 RESET (\"blocks.write\")");
        execute("SELECT table_name, settings['blocks']['write'] FROM information_schema.tables WHERE table_name = 't1' ORDER BY 1");
        assertThat(response).hasRows(
            "t1| false"
        );
        execute("SELECT partition_ident, settings['blocks']['write'] FROM information_schema.table_partitions WHERE table_name = 't1' ORDER BY 1");
        assertThat(response).hasRows(
            "04132| false",
            "04134| false"
        );
    }
}
