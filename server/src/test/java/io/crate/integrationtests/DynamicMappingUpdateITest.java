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

import static io.crate.testing.Asserts.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import io.crate.testing.UseNewCluster;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.IntegTestCase;
import org.jetbrains.annotations.Nullable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.crate.common.collections.Maps;
import io.crate.server.xcontent.XContentHelper;
import io.crate.testing.UseRandomizedSchema;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;


@UseRandomizedSchema(random = false)
public class DynamicMappingUpdateITest extends IntegTestCase {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    @UseNewCluster
    public void test_concurrent_statements_that_add_columns_result_in_dynamic_mapping_updates() throws InterruptedException, IOException {
        execute("create table t (a int, b object as (x int))");
        execute_concurrent_statements_that_add_columns_result_in_dynamic_mapping_updates();
    }

    @Test
    @UseNewCluster
    public void test_concurrent_statements_that_add_columns_to_partitioned_table_result_in_dynamic_mapping_updates() throws InterruptedException, IOException {
        execute("create table t (a int, b object as (x int)) partitioned by (a)");
        execute_concurrent_statements_that_add_columns_result_in_dynamic_mapping_updates();
    }

    @SuppressWarnings("unchecked")
    private void execute_concurrent_statements_that_add_columns_result_in_dynamic_mapping_updates() throws InterruptedException, IOException {
        // update, insert, alter take slightly different paths to update mappings
        execute("""
                insert into t values (1, {x=1})
                """);
        execute("refresh table t");

        Thread concurrentUpdates1 = new Thread(() -> {
            for (int i = 0; i < 5; i++) {
                execute("update t set b['newcol" + i + "'] = 1 where b['x'] = 1");
            }
        });
        Thread concurrentUpdates2 = new Thread(() -> {
            for (int i = 10; i < 15; i++) {
                execute("update t set b['newcol" + i + "'] = 1 where b['x'] = 1");
            }
        });
        Thread concurrentUpdates3 = new Thread(() -> {
            for (int i = 20; i < 25; i++) {
                execute("update t set b['newcol" + i + "'] = 1 where b['x'] = 1");
            }
        });
        Thread concurrentUpdates4 = new Thread(() -> {
            for (int i = 30; i < 35; i++) {
                execute("alter table t add column b['newcol" + i + "'] int");
            }
        });
        Thread concurrentUpdates5 = new Thread(() -> {
            for (int i = 40; i < 45; i++) {
                execute("alter table t add column b['newcol" + i + "'] int");
            }
        });
        Thread concurrentUpdates6 = new Thread(() -> {
            for (int i = 50; i < 55; i++) {
                execute("alter table t add column b['newcol" + i + "'] int");
            }
        });
        Thread concurrentUpdates7 = new Thread(() -> {
            for (int i = 60; i < 65; i++) {
                execute("insert into t(b) values({newcol" + i + "=1})");
            }
        });
        Thread concurrentUpdates8 = new Thread(() -> {
            for (int i = 70; i < 75; i++) {
                execute("insert into t(b) values({newcol" + i + "=1})");
            }
        });
        Thread concurrentUpdates9 = new Thread(() -> {
            for (int i = 80; i < 85; i++) {
                execute("insert into t(b) values({newcol" + i + "=1})");
            }
        });

        concurrentUpdates1.start();
        concurrentUpdates2.start();
        concurrentUpdates3.start();
        concurrentUpdates4.start();
        concurrentUpdates5.start();
        concurrentUpdates6.start();
        concurrentUpdates7.start();
        concurrentUpdates8.start();
        concurrentUpdates9.start();

        concurrentUpdates1.join();
        concurrentUpdates2.join();
        concurrentUpdates3.join();
        concurrentUpdates4.join();
        concurrentUpdates5.join();
        concurrentUpdates6.join();
        concurrentUpdates7.join();
        concurrentUpdates8.join();
        concurrentUpdates9.join();

        execute("""
            SELECT
                column_name
            FROM
                information_schema.columns
            WHERE
                table_name = 't'
                AND column_name LIKE 'b%'
            ORDER BY
                column_name
            """
        );
        assertThat(response).hasRows(
            "b",
            "b['newcol0']",
            "b['newcol1']",
            "b['newcol10']",
            "b['newcol11']",
            "b['newcol12']",
            "b['newcol13']",
            "b['newcol14']",
            "b['newcol2']",
            "b['newcol20']",
            "b['newcol21']",
            "b['newcol22']",
            "b['newcol23']",
            "b['newcol24']",
            "b['newcol3']",
            "b['newcol30']",
            "b['newcol31']",
            "b['newcol32']",
            "b['newcol33']",
            "b['newcol34']",
            "b['newcol4']",
            "b['newcol40']",
            "b['newcol41']",
            "b['newcol42']",
            "b['newcol43']",
            "b['newcol44']",
            "b['newcol50']",
            "b['newcol51']",
            "b['newcol52']",
            "b['newcol53']",
            "b['newcol54']",
            "b['newcol60']",
            "b['newcol61']",
            "b['newcol62']",
            "b['newcol63']",
            "b['newcol64']",
            "b['newcol70']",
            "b['newcol71']",
            "b['newcol72']",
            "b['newcol73']",
            "b['newcol74']",
            "b['newcol80']",
            "b['newcol81']",
            "b['newcol82']",
            "b['newcol83']",
            "b['newcol84']",
            "b['x']"
        );


        execute("select count(distinct ordinal_position) from information_schema.columns where table_name = 't'");
        assertThat(response.rows()[0][0])
            .as("distinct ordinal positions")
            .isEqualTo(3L + 45L);

        // Verify that there are no holes in positions for a concrete table.
        execute("""
            SELECT
                column_name, ordinal_position
            FROM
                information_schema.columns
            WHERE
                table_name = 't'
            AND
                ordinal_position > 48
            """
        );
        assertThat(response.rows())
            .as("No holes in the positions sequence for a concrete table")
            .isEmpty();

        execute("select column_name, ordinal_position from information_schema.columns where table_name = 't' order by ordinal_position limit 3");
        assertThat(response).hasRows(
            "a| 1",
            "b| 2",
            "b['x']| 3");

        Map<String, Object> mapping = XContentHelper.convertToMap(JsonXContent.JSON_XCONTENT, getIndexMapping("t"), false);
        Set<Long> oids = new HashSet<>();
        collectOID((Map<String, Map<String, Object>>) Maps.getByPath(mapping, "default.properties"), oids);
        assertThat(oids.size()).isEqualTo(48);
        assertThat(oids.stream().max(Long::compareTo).get()).isEqualTo(48);
    }

    @Test
    public void test_update_results_in_dynamic_mapping_updates() {
        execute("create table t (id int primary key) with (column_policy='dynamic')");
        execute_update_stmt_results_in_dynamic_mapping_updates();
    }

    @Test
    public void test_update_partitioned_table_results_in_dynamic_mapping_updates() {
        execute("create table t (id int primary key) partitioned by (id) with (column_policy='dynamic')");
        execute_update_stmt_results_in_dynamic_mapping_updates();
    }

    private void execute_update_stmt_results_in_dynamic_mapping_updates() {
        execute("insert into t values (1)");
        execute("refresh table t");
        execute("update t set name = 'abc'");
        execute("update t set o = {a={b=1}, b=1}");
        execute("update t set o = {q={r={s=1}}}");

        execute("select column_name, ordinal_position from information_schema.columns where table_name = 't'");
        assertThat(response).hasRows(
            "id| 1",
            "name| 2",
            "o| 3",
            "o['a']| 4",
            "o['a']['b']| 5",
            "o['b']| 6",
            "o['q']| 7",
            "o['q']['r']| 8",
            "o['q']['r']['s']| 9");
    }

    @Test
    public void test_alter_table_add_column_results_in_dynamic_mapping_updates() {
        execute("create table t (id int primary key) with (column_policy='dynamic')");
        execute_alter_table_add_column_results_in_dynamic_mapping_updates();
    }

    @Test
    public void test_alter_table_add_column_on_partitioned_table_results_in_dynamic_mapping_updates() {
        execute("create table t (id int primary key) partitioned by (id) with (column_policy='dynamic')");
        execute_alter_table_add_column_results_in_dynamic_mapping_updates();
    }

    private void execute_alter_table_add_column_results_in_dynamic_mapping_updates() {
        execute("alter table t add column name string");
        execute("alter table t add column o object as (a object as (b int), b int)");
        execute("alter table t add column o['q']['r']['s'] int");

        execute("select column_name, ordinal_position from information_schema.columns where table_name = 't'");
        assertThat(response).hasRows(
            "id| 1",
            "name| 2",
            "o| 3",
            "o['a']| 4",
            "o['a']['b']| 5",
            "o['b']| 6",
            "o['q']| 7",
            "o['q']['r']| 8",
            "o['q']['r']['s']| 9");
    }

    @Test
    public void test_insert_deep_nested_object_results_in_dynamic_mapping_updates() {
        execute(
            """
                create table t (
                    tb array(object(dynamic)),
                    p int
                ) with (column_policy = 'dynamic');
                """
        );
        execute_insert_deep_nested_object_results_in_dynamic_mapping_updates();
    }

    @Test
    public void test_insert_deep_nested_object_into_partitioned_table_results_in_dynamic_mapping_updates() {
        execute(
            """
                create table t (
                    tb array(object(dynamic)),
                    p int
                ) partitioned by (p) with (column_policy = 'dynamic');
                """
        );
        execute_insert_deep_nested_object_results_in_dynamic_mapping_updates();
    }

    private void execute_insert_deep_nested_object_results_in_dynamic_mapping_updates() {
        execute("insert into t (tb) values ([{t1 = [{t3 = {t4 = {t5 = 1}}},{t6 = [1,2]}]},{t2 = {}}])");
        execute("insert into t (o) values ({a={b=1}, b=1})");
        execute("refresh table t");
        execute("select column_name, ordinal_position, data_type from information_schema.columns where table_name = 't' order by 2");

        assertThat(response).hasRows(
            "tb| 1| object_array",
            "p| 2| integer",
            "tb['t1']| 3| object_array",
            "tb['t1']['t3']| 4| object",
            "tb['t1']['t3']['t4']| 5| object",
            "tb['t1']['t3']['t4']['t5']| 6| bigint",
            "tb['t1']['t6']| 7| bigint_array",
            "tb['t2']| 8| object",
            "o| 9| object",
            "o['a']| 10| object",
            "o['a']['b']| 11| bigint",
            "o['b']| 12| bigint"
        );
    }

    @Test
    public void test_copy_deep_nested_object_results_in_dynamic_mapping_updates() throws IOException {
        execute(
            """
                create table t (
                    tb array(object(dynamic)),
                    p int
                ) with (column_policy = 'dynamic');
                """
        );
        execute_copy_deep_nested_object_results_in_dynamic_mapping_updates();
    }

    @Test
    public void test_copy_deep_nested_object_to_partitioned_table_results_in_dynamic_mapping_updates() throws IOException {
        execute(
            """
                create table t (
                    tb array(object(dynamic)),
                    p int
                ) partitioned by (p) with (column_policy = 'dynamic');
                """
        );
        execute_copy_deep_nested_object_results_in_dynamic_mapping_updates();
    }

    private void execute_copy_deep_nested_object_results_in_dynamic_mapping_updates() throws IOException {
        List<String> lines = List.of(
            """
            {"tb":[{"t1":[{"t3":{"t4":{"t5":1}}},{"t6":[1,2]}]},{"t2":{}}]}
            """
            );
        File file = folder.newFile(UUID.randomUUID().toString());
        Files.write(file.toPath(), lines, StandardCharsets.UTF_8);
        execute("copy t from ?", new Object[]{Paths.get(file.toURI()).toUri().toString()});

        lines = List.of(
            """
            {"tb":[{"t1":[{"t3":{"t4":{"t5":1}}},{"t6":[1,2]}]},{"t2":{}}]}
            """,
            """
            {"o":{"a":{"b":1}, "b":1}}
            """
        );
        file = folder.newFile(UUID.randomUUID().toString());
        Files.write(file.toPath(), lines, StandardCharsets.UTF_8);
        execute("copy t from ?", new Object[]{Paths.get(file.toURI()).toUri().toString()});
        execute("refresh table t");

        execute("select column_name, ordinal_position from information_schema.columns where table_name='t' order by ordinal_position");
        assertThat(response).hasRows(
            "tb| 1",
            "p| 2",
            "tb['t1']| 3",
            "tb['t1']['t3']| 4",
            "tb['t1']['t3']['t4']| 5",
            "tb['t1']['t3']['t4']['t5']| 6",
            "tb['t1']['t6']| 7",
            "tb['t2']| 8",
            "o| 9",
            "o['a']| 10",
            "o['a']['b']| 11",
            "o['b']| 12");
    }

    private static void collectOID(@Nullable Map<String, Map<String, Object>> propertiesMap, Set<Long> oids) {
        if (propertiesMap != null) {
            for (Map<String, Object> colProps: propertiesMap.values()) {
                String type = (String) colProps.get("type"); // Can be null

                if (ArrayType.NAME.equals(type)) {
                    Map<String, Object> inner = Maps.get(colProps, "inner");
                    Number oid = Maps.get(inner, "oid");
                    assertThat(oid).isNotNull();
                    oids.add(oid.longValue());

                    String innerType = (String) inner.get("type");
                    if (ObjectType.UNTYPED.equals(DataTypes.ofMappingName(innerType))) {
                        collectOID(Maps.get(inner, "properties"), oids);
                    }
                } else {
                    Number oid = Maps.get(colProps, "oid");
                    assertThat(oid).isNotNull();
                    oids.add(oid.longValue());
                    if (type == null || ObjectType.UNTYPED.equals(DataTypes.ofMappingName(type))) {
                        collectOID(Maps.get(colProps,"properties"), oids);
                    }
                }
            }
        }
    }
}
