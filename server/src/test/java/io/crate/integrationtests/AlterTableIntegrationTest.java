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
import static io.crate.testing.Asserts.assertSQLError;
import static io.crate.testing.Asserts.assertThat;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

import io.crate.testing.TestingHelpers;
import io.crate.testing.UseNewCluster;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.testing.Asserts;

import java.util.Locale;

public class AlterTableIntegrationTest extends IntegTestCase {

    @Test
    public void test_create_soft_delete_setting_for_partitioned_tables() {
        assertSQLError(() -> execute(
                "create table test(i int) partitioned by (i) WITH(\"soft_deletes.enabled\" = false) "))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("Creating tables with soft-deletes disabled is no longer supported.");
    }

    // Drop column
    @Test
    public void test_alter_table_drop_column_used_meanwhile_in_generated_col() {
        execute("CREATE TABLE t(a int, b int)");
        PlanForNode plan = plan("ALTER TABLE t DROP b");
        execute("ALTER TABLE t ADD COLUMN c GENERATED ALWAYS AS (b + 1)");
        assertSQLError(() -> execute(plan).getResult())
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(INTERNAL_SERVER_ERROR, 5000)
            .hasMessageContaining("Dropping column: b which is used to produce values for generated column is not allowed");
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
            {"1": 1, "2": 11, "3": {"5": {"6": 1111}, "4": 111}}
            {"%d": "some-text", "2": 222," 3": {"5": {"6": 2222}, "4": 222}}
            """;
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualToIgnoringWhitespace(
            String.format(Locale.ENGLISH, rawRows, newColumnOid)
        );

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
           {"%d": "another-text", "2": 333," 3": {"%d": {"%d": "hello"}, "4": 333}}
            """;
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualToIgnoringWhitespace(
            String.format(Locale.ENGLISH, rawRows, newColumnOid, newColumnOid + 1, newColumnOid + 2)
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


}
