/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import static io.crate.testing.Asserts.assertSQLError;
import static io.crate.testing.Asserts.assertThat;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.optimizer.rule.MergeFilterAndCollect;
import io.crate.planner.optimizer.rule.OptimizeCollectWhereClauseAccess;
import io.crate.testing.Asserts;
import io.crate.testing.UseRandomizedOptimizerRules;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

@IntegTestCase.ClusterScope(numDataNodes = 1)
public class ColumnPolicyIntegrationTest extends IntegTestCase {

    private String copyFilePath = Paths.get(getClass().getResource("/essetup/data/copy").toURI()).toUri().toString();

    public ColumnPolicyIntegrationTest() throws URISyntaxException {
    }

    private DocTableInfo getTable(RelationName name) {
        return cluster().getInstance(NodeContext.class)
            .schemas()
            .getTableInfo(name);
    }

    private DocTableInfo getTable(String table) throws IOException {
        return getTable(new RelationName(sqlExecutor.getCurrentSchema(), table));
    }

    private DocTableInfo getTable(String schema, String table) throws IOException {
        return getTable(new RelationName(schema, table));
    }

    @Test
    public void testCopyFromFileStrictTable() throws Exception {
        execute("create table quotes (id int primary key) with (column_policy='strict', number_of_replicas = 0)");
        ensureYellow();

        execute("copy quotes from ?", new Object[]{copyFilePath + "test_copy_from.json"});
        assertThat(response).hasRowCount(0);
    }

    @Test
    public void testInsertArrayIntoDynamicTable() throws Exception {
        execute("create table dynamic_table (" +
                "  meta string" +
                ") with (column_policy='dynamic', number_of_replicas=0)");
        ensureYellow();
        execute("insert into dynamic_table (new, meta) values(['a', 'b', 'c'], 'hello')");
        execute("insert into dynamic_table (new) values(['d', 'e', 'f'])");
        DocTableInfo table = getTable("dynamic_table");
        assertThat(table.getReference(ColumnIdent.of("new")).valueType()).isEqualTo(new ArrayType<>(DataTypes.STRING));
    }

    @Test
    // Ensure that PKLookup is hit.
    @UseRandomizedOptimizerRules(alwaysKeep = {MergeFilterAndCollect.class, OptimizeCollectWhereClauseAccess.class})
    public void testInsertDynamicObjectArray() throws Exception {
        execute("create table dynamic_table (id int PRIMARY KEY, person object(dynamic)) with (number_of_replicas=0)");
        execute("insert into dynamic_table (id, person) values " +
                "(1, {name='Ford', addresses=[{city='West Country', country='GB'}]})");
        execute("refresh table dynamic_table");

        DocTableInfo table = getTable("dynamic_table");
        ObjectType expectedObject = ObjectType.builder()
            .setInnerType("city", DataTypes.STRING)
            .setInnerType("country", DataTypes.STRING)
            .build();
        assertThat(table.getReference(ColumnIdent.of("person", "addresses")).valueType())
            .isEqualTo(new ArrayType<>(expectedObject));
        assertThat(table.getReference(ColumnIdent.of("person", "name")).valueType())
            .isEqualTo(DataTypes.STRING);
        assertThat(table.getReference(ColumnIdent.of("person", List.of("addresses", "city"))).valueType())
            .isEqualTo(DataTypes.STRING);
        assertThat(table.getReference(ColumnIdent.of("person", List.of("addresses", "country"))).valueType())
            .isEqualTo(DataTypes.STRING);

        execute("select person['name'], person['addresses']['city'] from dynamic_table");

        assertThat(response).hasColumns("person['name']", "person['addresses']['city']");
        assertThat(response).hasRows(
            "Ford| [West Country]"
        );

        // Verify that PKLookup can fetch a sub-column, which is array of objects.
        execute("select person['addresses']['city'] from dynamic_table where id = 1");
        assertThat(response).hasRows("[West Country]");
    }

    @Test
    public void testInsertNestedArrayIntoDynamicTable() throws Exception {
        execute("create table dynamic_table (" +
                "  meta string" +
                ") with (column_policy='dynamic', number_of_replicas=0)");
        ensureYellow();
        execute("insert into dynamic_table (new, meta) values({a=['a', 'b', 'c'], nest={a=['a','b']}}, 'hello')");
        execute("refresh table dynamic_table");
        execute("insert into dynamic_table (new) values({a=['d', 'e', 'f']})");
        execute("refresh table dynamic_table");
        execute("insert into dynamic_table (new) values({nest={}, new={}})");

        DocTableInfo table = getTable("dynamic_table");
        assertThat(table.getReference(ColumnIdent.of("new", "a")).valueType())
            .isEqualTo(new ArrayType<>(DataTypes.STRING));
        assertThat(table.getReference(ColumnIdent.of("new", List.of("nest", "a"))).valueType())
            .isEqualTo(new ArrayType<>(DataTypes.STRING));
        assertThat(table.getReference(ColumnIdent.of("meta")).valueType()).isEqualTo(DataTypes.STRING);
    }

    @Test
    public void testInsertNestedObjectOnCustomSchemaTable() throws Exception {
        execute("create table c.dynamic_table (" +
                "  meta object(strict) as (" +
                "     meta object" +
                "  )" +
                ") with (column_policy='dynamic', number_of_replicas=0)");
        ensureYellow();
        execute("insert into c.dynamic_table (meta) values({meta={a=['a','b']}})");
        execute("refresh table c.dynamic_table");
        execute("insert into c.dynamic_table (meta) values({meta={a=['c','d']}})");
        DocTableInfo table = getTable("c", "dynamic_table");
        assertThat(table.getReference(ColumnIdent.of("meta", List.of("meta", "a"))).valueType())
            .isEqualTo(new ArrayType<>(DataTypes.STRING));
    }

    @Test
    public void testInsertMultipleValuesDynamic() throws Exception {
        execute("create table dynamic_table (" +
                "  my_object object " +
                ") with (column_policy='dynamic', number_of_replicas=0)");
        ensureYellow();

        execute("insert into dynamic_table (my_object) values ({a=['a','b']}),({b=['a']})");
        execute("refresh table dynamic_table");

        DocTableInfo table = getTable("dynamic_table");
        assertThat(table.getReference(ColumnIdent.of("my_object", "a")).valueType())
            .isEqualTo(new ArrayType<>(DataTypes.STRING));
        assertThat(table.getReference(ColumnIdent.of("my_object", "b")).valueType())
            .isEqualTo(new ArrayType<>(DataTypes.STRING));
    }

    @Test
    public void testAddColumnToStrictObject() throws Exception {
        execute("create table books(" +
                "   author object(dynamic) as (" +
                "       name object(strict) as (" +
                "           first_name string" +
                "       )" +
                "   )," +
                "   title string" +
                ")");
        ensureYellow();
        Map<String, Object> authorMap = Map.of("name", Map.of("first_name", "Douglas"));
        execute("insert into books (title, author) values (?,?)",
            new Object[]{
                "The Hitchhiker's Guide to the Galaxy",
                authorMap
            });
        execute("refresh table books");
        Asserts.assertSQLError(() -> execute("insert into books (title, author) values (?,?)",
            new Object[]{
                "Life, the Universe and Everything",
                Map.of("name", Map.of("first_name", "Douglas", "middle_name", "Noel"))
            }))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("Cannot add column `middle_name` to strict object `author['name']`");
    }

    @Test
    public void test_cannot_create_implicit_columns_with_strict_column_policy() throws Exception {
        execute("create table tbl (x int, p int) partitioned by (p) with (column_policy = 'strict')");

        assertSQLError(() -> execute("insert into tbl (x, p, new_col) values (1, 2, 3)"))
            .hasMessageContaining("Column new_col unknown");

        // create a partition
        execute("insert into tbl (x, p) values (1, 1)");

        execute("select column_policy from information_schema.tables where table_name = 'tbl'");
        assertThat(response).hasRows("strict");

        // try again after concrete partition exists
        assertSQLError(() -> execute("insert into tbl (x, p, new_col) values (1, 1, 3)"))
            .hasMessageContaining("Column new_col unknown");

        assertSQLError(() -> execute("update tbl set new_col = 1"))
            .hasMessageContaining("Column new_col unknown");
    }

    @Test
    public void test_can_create_column_with_dynamic_column_policy() throws Exception {
        execute("create table tbl (x int, p int) partitioned by (p) with (column_policy = 'dynamic')");
        execute("select column_policy from information_schema.tables where table_name = 'tbl'");
        assertThat(response).hasRows("dynamic");

        // can add column via insert or update
        execute("insert into tbl (x, p, col1) values (1, 1, 1)");
        execute("refresh table tbl");
        execute("update tbl set col1 = 2, col2 = 3 where x = 1");

        execute("""
            select
                ordinal_position,
                column_name
            from information_schema.columns
            where table_name = 'tbl'
            order by ordinal_position
            """);
        assertThat(response).hasRows(
            "1| x",
            "2| p",
            "3| col1",
            "4| col2"
        );
    }

    @Test
    public void test_can_change_column_policy() throws Exception {
        String createTable = "create table tbl (x int, p int)";
        if (randomBoolean()) {
            createTable += " partitioned by (p)";
        }

        execute(createTable);

        // if partitioned this will create two partitions up-front
        execute("insert into tbl (x, p) values (1, 1), (2, 2)");

        execute("select column_policy from information_schema.tables where table_name = 'tbl'");
        assertThat(response).hasRows("strict");

        assertSQLError(() -> execute("insert into tbl (x, p, col1) values (1, 1, 3)"))
            .hasMessageContaining("Column col1 unknown");

        execute("alter table tbl set (column_policy = 'dynamic')");
        execute("select column_policy from information_schema.tables where table_name = 'tbl'");
        assertThat(response).hasRows("dynamic");

        // if partitioned this will hit an existing partition, and a new partition
        execute("insert into tbl (x, p, col1) values (1, 1, 1), (3, 3, 3)");
        execute("alter table tbl reset (column_policy)");

        execute("select column_policy from information_schema.tables where table_name = 'tbl'");
        assertThat(response).hasRows("strict");

        assertSQLError(() -> execute("insert into tbl (x, p, col2) values (1, 1, 3)"))
            .hasMessageContaining("Column col2 unknown");

        assertSQLError(() -> execute("insert into tbl (x, p, col2) values (4, 4, 3)"))
            .hasMessageContaining("Column col2 unknown");
    }
}
