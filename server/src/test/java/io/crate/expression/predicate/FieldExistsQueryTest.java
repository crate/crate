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

import static io.crate.analyze.AnalyzedColumnDefinition.UNSUPPORTED_INDEX_TYPE_IDS;
import static org.assertj.core.api.Assertions.assertThat;

import io.crate.testing.DataTypeTesting;
import io.crate.types.DataType;
import java.io.IOException;
import java.util.Map;

import org.elasticsearch.Version;
import org.junit.Test;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.QueryTester;

public class FieldExistsQueryTest extends CrateDummyClusterServiceUnitTest {


    private static final Object[] ARRAY_VALUES = new Object[] {new Object[] {}, null};

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
            assertThat(results.size()).isEqualTo(1);
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
            String createStatement = "create table t_" +
                type.getName().replaceAll(" ", "_") +
                " (xs array(" + type.getName() + "))";
            assertMatches(createStatement, true, ARRAY_VALUES);
        }
    }

    @Test
    public void test_is_null_does_not_match_empty_arrays_with_index_off() throws Exception {
        for (DataType<?> type : DataTypeTesting.ALL_STORED_TYPES_EXCEPT_ARRAYS) {
            if (UNSUPPORTED_INDEX_TYPE_IDS.contains(type.id()) == false) {
                String createStatement = "create table t_" +
                    type.getName().replaceAll(" ", "_") +
                    " (xs array(" + type.getName() + ") index off)";
                assertMatches(createStatement, true, ARRAY_VALUES);
            }
        }
    }

    @Test
    public void test_is_null_does_not_match_empty_arrays_with_index_and_column_store_off() throws Exception {
        // Turning off columnstore is currently supported only for TEXT.
        // We can enable this case for all types once https://github.com/crate/crate/issues/11652 is implemented.
        String createStatement = "create table t_text (xs array(text) index off storage with (columnstore = false))";
        assertMatches(createStatement, true, ARRAY_VALUES);
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
            if (UNSUPPORTED_INDEX_TYPE_IDS.contains(type.id()) == false) {
                String createStatement = "create table t_" +
                    type.getName().replaceAll(" ", "_") +
                    " (xs array(" + type.getName() + ") index off)";
                assertMatches(createStatement, false, ARRAY_VALUES);
            }
        }
    }

    @Test
    public void test_is_not_null_does_not_match_empty_arrays_with_index_and_column_store_off() throws Exception {
        // Turning off columnstore is currently supported only for TEXT.
        // We can enable this case for all types once https://github.com/crate/crate/issues/11652 is implemented.
        String createStatement = "create table t_text (xs array(text) index off storage with (columnstore = false))";
        assertMatches(createStatement, false, ARRAY_VALUES);
    }
}
