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


package io.crate.expression.predicate;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.elasticsearch.Version;
import org.junit.Test;

import io.crate.analyze.TableElementsAnalyzer;
import io.crate.sql.SqlFormatter;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.DataTypeTesting;
import io.crate.testing.QueryTester;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.FloatVectorType;
import io.crate.types.StorageSupport;

public class FieldExistsQueryTest extends CrateDummyClusterServiceUnitTest {


    private static final Object[] ARRAY_VALUES = new Object[] {List.of(), null};

    private static final Object[] OBJECT_VALUES = new Object[] {Map.of(), null};

    private QueryTester.Builder getBuilder(String createStatement) throws IOException {
        return new QueryTester.Builder(
            createTempDir(),
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            createStatement
        );
    }

    private void assertMatches(String createStatement, boolean isNull, Object[] values) throws Exception {
        String expression = isNull ? "xs is null" : "xs is not null";
        // ensure the test is operating on a fresh, empty cluster state (no tables)
        resetClusterService();

        QueryTester.Builder builder = getBuilder(createStatement);
        builder.indexValues("xs", values);
        try (var queryTester = builder.build()) {
            var results = queryTester.runQuery("xs", expression);
            assertThat(results)
                .as(expression + " must match 1 record")
                .hasSize(1);
            if (isNull) {
                assertThat(results.get(0)).isNull();
            } else {
                // can be array (list) or object (map)
                if (results.get(0) instanceof Map<?, ?> map) {
                    assertThat(map).isEmpty();
                } else {
                    assertThat(results.get(0)).asList().isEmpty();
                }
            }
        }
    }

    private void assertNotFunction(String createStatement, Object[] values) throws Exception {
        String expression = "NOT xs::text = 'value'";
        // ensure the test is operating on a fresh, empty cluster state (no tables)
        resetClusterService();

        QueryTester.Builder builder = getBuilder(createStatement);
        if (values != null) {
            builder.indexValues("xs", OBJECT_VALUES);
        }
        try (var queryTester = builder.build()) {
            var results = queryTester.runQuery("xs", expression);
            if (values == null) {
                assertThat(results).isEmpty();
            } else {
                assertThat(results).hasSize(2);
                assertThat(results.get(0)).isInstanceOf(Map.class);
                assertThat((Map<?, ?>) results.get(0)).isEmpty();
                assertThat(results.get(1)).isNull();
            }
        }
    }

    @Test
    public void test_is_null_does_not_match_empty_arrays() throws Exception {
        for (DataType<?> type : DataTypeTesting.ALL_STORED_TYPES_EXCEPT_ARRAYS) {
            if (type instanceof FloatVectorType) {
                continue;
            }
            String createStatement = "create table t_" +
                type.getName().replaceAll(" ", "_") +
                " (xs array(" + type.getName() + "))";
            assertMatches(createStatement, true, ARRAY_VALUES);
        }
    }

    @Test
    public void test_is_null_does_not_match_empty_arrays_with_index_off() throws Exception {
        for (DataType<?> type : DataTypeTesting.ALL_STORED_TYPES_EXCEPT_ARRAYS) {
            if (type instanceof FloatVectorType) {
                continue;
            }
            if (TableElementsAnalyzer.UNSUPPORTED_INDEX_TYPE_IDS.contains(type.id()) == false) {
                String createStatement = "create table t_" +
                    type.getName().replaceAll(" ", "_") +
                    " (xs array(" + type.getName() + ") index off)";
                assertMatches(createStatement, true, ARRAY_VALUES);
            }
        }
    }

    @Test
    public void test_is_null_does_not_match_empty_arrays_with_index_and_column_store_off() throws Exception {
        for (var dataType : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            if (dataType.storageSupport() != null && dataType.storageSupport().supportsDocValuesOff()) {
                var createStmt =
                        "create table t_text (xs array(" + dataType + ") index off storage with (columnstore = false))";
                assertMatches(createStmt, true, ARRAY_VALUES);
            }
        }
    }

    @Test
    public void test_is_null_does_not_match_empty_objects() throws Exception {
        assertMatches("create table t (xs OBJECT as (x int))", true, OBJECT_VALUES);
    }

    @Test
    public void test_is_not_null_matches_empty_objects() throws Exception {
        assertMatches("create table t (xs OBJECT as (x int))", false, OBJECT_VALUES);
    }

    @Test
    public void test_not_function_does_not_match_empty_objects() throws Exception {
        assertNotFunction("create table t (xs OBJECT)", null);
        assertNotFunction("create table t (xs OBJECT)", OBJECT_VALUES);
    }

    @Test
    public void test_is_not_null_does_not_match_empty_arrays() throws Exception {
        for (DataType<?> type : DataTypeTesting.ALL_STORED_TYPES_EXCEPT_ARRAYS) {
            if (type instanceof FloatVectorType) {
                continue;
            }
            // including geo_shape
            String createStatement = "create table t_" +
                type.getName().replaceAll(" ", "_") +
                " (xs array(" + type.getName() + "))";
            assertMatches(createStatement, false, ARRAY_VALUES);
        }
    }

    @Test
    public void test_is_not_null_does_not_match_empty_arrays_with_index_off() throws Exception {
        for (DataType<?> type : DataTypeTesting.ALL_STORED_TYPES_EXCEPT_ARRAYS) {
            if (type instanceof FloatVectorType) {
                continue;
            }
            if (TableElementsAnalyzer.UNSUPPORTED_INDEX_TYPE_IDS.contains(type.id()) == false) {
                String createStatement = "create table t_" +
                    type.getName().replaceAll(" ", "_") +
                    " (xs array(" + type.getName() + ") index off)";
                assertMatches(createStatement, false, ARRAY_VALUES);
            }
        }
    }

    @Test
    public void test_is_not_null_does_not_match_empty_arrays_with_index_and_column_store_off() throws Exception {
        for (var dataType : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            if (dataType.storageSupport() != null && dataType.storageSupport().supportsDocValuesOff()) {
                var createStmt =
                        "create table t (xs array(" + dataType + ") index off storage with (columnstore = false))";
                assertMatches(createStmt, false, ARRAY_VALUES);
            }
        }
    }

    @Test
    public void test_is_null_with_values_on_geo_shape_array() throws Exception {
        Supplier<Map<String, Object>> dataGenerator = DataTypeTesting.getDataGenerator(DataTypes.GEO_SHAPE);
        List<Map<String, Object>> shapes = List.of(dataGenerator.get(), dataGenerator.get());
        Object[] rows = new Object[] {
            null,
            shapes
        };
        resetClusterService();
        QueryTester.Builder builder = getBuilder("create table tbl (xs array(geo_shape))");
        builder.indexValues("xs", rows);
        try (var queryTester = builder.build()) {
            List<Object> results = queryTester.runQuery("xs", "xs is null");
            assertThat(results).containsExactly(new Object[] { null });

            results = queryTester.runQuery("xs", "xs is not null");
            assertThat(results).containsExactly(shapes);
        }
    }

    @Test
    public void test_is_null_on_columns_without_doc_values() throws Exception {
        for (var type : DataTypeTesting.ALL_STORED_TYPES_EXCEPT_ARRAYS) {
            StorageSupport<?> storageSupport = type.storageSupport();
            if (storageSupport == null || !storageSupport.supportsDocValuesOff()) {
                continue;
            }
            Supplier<?> dataGenerator = DataTypeTesting.getDataGenerator(type);
            Object val1 = dataGenerator.get();
            var extendedType = DataTypeTesting.extendedType(type, val1);
            String typeDefinition = SqlFormatter.formatSql(extendedType.toColumnType(ColumnPolicy.STRICT, null));
            String stmt = "create table tbl (id int primary key, x " + typeDefinition + " storage with (columnstore = false))";
            QueryTester.Builder builder = new QueryTester.Builder(
                createTempDir(),
                THREAD_POOL,
                clusterService,
                Version.CURRENT,
                stmt
            );
            builder.indexValue("x", val1);
            builder.indexValue("x", null);
            try (var queryTester = builder.build()) {
                assertThat(queryTester.runQuery("x", "x is null"))
                    .containsExactly(new Object[] { null });

                if (!(type instanceof FloatVectorType)) {
                    assertThat(queryTester.toQuery("x is null").toString())
                        .as(type.getName() + " indexes field_names")
                        .isEqualTo("+*:* -ConstantScore(_field_names:x)");
                }
            }
        }
    }
}
