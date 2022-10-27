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
import org.elasticsearch.Version;
import org.junit.Test;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.QueryTester;

import java.util.Map;

public class FieldExistsQueryTest extends CrateDummyClusterServiceUnitTest {


    private static final Object[] ARRAY_VALUES = new Object[] {new Object[] {}, null};

    private static final Object[] OBJECT_VALUES = new Object[] {Map.of(), null};

    private void assertMatches(String createStatement, boolean isNull, Object[] values) throws Exception {
        String expression = isNull ? "xs is null" : "xs is not null";
        // ensure the test is operating on a fresh, empty cluster state (no tables)
        resetClusterService();

        QueryTester.Builder builder = new QueryTester.Builder(
            createTempDir(),
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            createStatement
        );
        builder.indexValues("xs", values);
        try (var queryTester = builder.build()) {
            var results = queryTester.runQuery("xs", expression);
            assertThat(results.size()).isEqualTo(1);
            if (isNull) {
                assertThat(results.get(0)).isNull();
            } else {
                // can be array (list) or object (map)
                if (results.get(0) instanceof Map map) {
                    assertThat(map).isEmpty();
                } else {
                    assertThat(results.get(0)).asList().isEmpty();
                }
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
        for (DataType<?> type : DataTypeTesting.ALL_STORED_TYPES_EXCEPT_ARRAYS) {
            if (UNSUPPORTED_INDEX_TYPE_IDS.contains(type.id()) == false) {
                String createStatement = "create table t_" +
                    type.getName().replaceAll(" ", "_") +
                    " (xs array(" + type.getName() + ") index off storage with (columnstore = false))";
                assertMatches(createStatement, true, ARRAY_VALUES);
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
        for (DataType<?> type : DataTypeTesting.ALL_STORED_TYPES_EXCEPT_ARRAYS) {
            if (UNSUPPORTED_INDEX_TYPE_IDS.contains(type.id()) == false) {
                String createStatement = "create table t_" +
                    type.getName().replaceAll(" ", "_") +
                    " (xs array(" + type.getName() + ") index off storage with (columnstore = false))";
                assertMatches(createStatement, false, ARRAY_VALUES);
            }
        }
    }
}
