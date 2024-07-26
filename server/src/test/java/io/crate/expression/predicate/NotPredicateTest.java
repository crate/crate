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

import static io.crate.testing.Asserts.isFunction;
import static io.crate.testing.Asserts.isLiteral;
import static io.crate.testing.DataTypeTesting.getDataGenerator;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.Version;
import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;
import io.crate.sql.SqlFormatter;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.testing.Asserts;
import io.crate.testing.DataTypeTesting;
import io.crate.testing.QueryTester;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.FloatVectorType;
import io.crate.types.GeoPointType;
import io.crate.types.GeoShapeType;
import io.crate.types.StorageSupport;

public class NotPredicateTest extends ScalarTestCase {

    @Test
    public void testNormalizeSymbolNull() throws Exception {
        assertNormalize("not null", isLiteral(null, DataTypes.BOOLEAN));
    }

    @Test
    public void testNormalizeSymbolBoolean() throws Exception {
        assertNormalize("not true", isLiteral(false));
    }

    @Test
    public void testNormalizeSymbol() throws Exception {
        assertNormalize("not name = 'foo'", isFunction(NotPredicate.NAME));
    }

    @Test
    public void testEvaluate() throws Exception {
        assertEvaluate("not name = 'foo'", false, Literal.of("foo"));
    }

    @Test
    public void test_not_on_case_uses_strict_3vl() throws Exception {
        QueryTester.Builder builder = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table tbl (x int)"
        );
        builder.indexValue("x", null);
        builder.indexValue("x", 2);

        try (var tester = builder.build()) {
            List<Object> result = tester.runQuery("x", "(case when true then 2 else x end) != 1");
            assertThat(result).containsExactly(null, 2);

            result = tester.runQuery("x", "(case when true then 2 else x end) != 2");
            assertThat(result).isEmpty();
        }
    }

    @Test
    public void test_neq_on_array_types_with_non_empty_array_does_not_filter_empty_array() throws Exception {
        for (DataType<?> type : DataTypeTesting.getStorableTypesExceptArrays(random())) {
            if (type instanceof FloatVectorType) {
                continue;
            }
            var arrayOfNulls = new ArrayList<Integer>();
            arrayOfNulls.add(null);
            Object[] values = new Object[] {List.of(), arrayOfNulls, null}; // 3 rows to be indexed: empty array, array of nulls, null

            // ensure the test is operating on a fresh, empty cluster state (no tables)
            resetClusterService();

            String randomData;
            if (type.id() == GeoPointType.ID) {
                randomData = "'POINT (9.7417 47.4108)'::geo_point";
            } else if (type.id() == GeoShapeType.ID) {
                randomData = "'POLYGON ((5 5, 10 5, 10 10, 5 10, 5 5))'::geo_shape";
            } else {
                randomData = Literal.ofUnchecked(type, getDataGenerator(type).get()).toString();
            }
            String typeDefinition = SqlFormatter.formatSql(type.toColumnType(ColumnPolicy.STRICT, null));
            String query = "xs != [" + randomData + "]";

            // with default columnstore
            try (QueryTester tester = new QueryTester.Builder(
                THREAD_POOL,
                clusterService,
                Version.CURRENT,
                "create table \"t_" + type.getName() + "\" (xs array(" + typeDefinition + "))"
            ).indexValues("xs", values).build()) {
                List<Object> result = tester.runQuery("xs", query);
                Asserts.assertThat(result)
                    .as("QUERY: " + query + "; TYPE: " + type + " ; expects '[]' and '[null]' returned")
                    .containsExactlyInAnyOrder(List.of(), arrayOfNulls);
            }

            // storage with (columnstore = false)
            StorageSupport<?> storageSupport = type.storageSupport();
            if (storageSupport == null || !storageSupport.supportsDocValuesOff()) {
                continue;
            }
            try (QueryTester tester = new QueryTester.Builder(
                THREAD_POOL,
                clusterService,
                Version.CURRENT,
                "create table \"t_" + type.getName() + "\" (xs array(\"" + type.getName() + "\") storage with (columnstore = false))"
            ).indexValues("xs", values).build()) {
                List<Object> result = tester.runQuery("xs", query);
                Asserts.assertThat(result)
                    .as("QUERY: " + query + "; TYPE: " + type + " ; expects '[]' and '[null]' returned")
                    .containsExactlyInAnyOrder(List.of(), arrayOfNulls);
            }
        }
    }

    @Test
    public void test_neq_on_array_types_with_empty_array_does_not_filter_array_of_nulls() throws Exception {
        for (DataType<?> type : ALL_STORED_TYPES_EXCEPT_ARRAYS) {
            if (type instanceof FloatVectorType) {
                continue;
            }
            var arrayOfNulls = new ArrayList<Integer>();
            arrayOfNulls.add(null);
            Object[] values = new Object[] {List.of(), arrayOfNulls};

            // ensure the test is operating on a fresh, empty cluster state (no tables)
            resetClusterService();

            String query = "xs != []";

            // with default columnstore
            try (QueryTester tester = new QueryTester.Builder(
                THREAD_POOL,
                clusterService,
                Version.CURRENT,
                "create table \"t_" + type.getName() + "\" (xs array(\"" + type.getName() + "\"))"
            ).indexValues("xs", values).build()) {
                List<Object> result = tester.runQuery("xs", query);
                Asserts.assertThat(result)
                    .as("QUERY: " + query + "; TYPE: " + type + " ; expects '[null]' returned")
                    .containsExactlyInAnyOrder(arrayOfNulls);
            }

            // storage with (columnstore = false)
            StorageSupport<?> storageSupport = type.storageSupport();
            if (storageSupport == null || !storageSupport.supportsDocValuesOff()) {
                continue;
            }
            try (QueryTester tester = new QueryTester.Builder(
                THREAD_POOL,
                clusterService,
                Version.CURRENT,
                "create table \"t_" + type.getName() + "\" (xs array(\"" + type.getName() + "\") storage with (columnstore = false))"
            ).indexValues("xs", values).build()) {
                List<Object> result = tester.runQuery("xs", query);
                Asserts.assertThat(result)
                    .as("QUERY: " + query + "; TYPE: " + type + " ; expects '[null]' returned")
                    .containsExactlyInAnyOrder(arrayOfNulls);
            }
        }
    }

    @Test
    public void test_neq_on_array_scalars_that_evaluate_non_null_inputs_to_nulls() throws Exception {
        var listOfNulls = new ArrayList<Integer>();
        listOfNulls.add(null);
        listOfNulls.add(null);
        Object[] values = new Object[] {List.of(), listOfNulls}; // '[]' and [null, null]
        try (QueryTester tester = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table t (xs array(int))"
        ).indexValues("xs", values).build()) {

            String query = "array_max(xs) != 1";
            List<Object> result = tester.runQuery("xs", query);
            Asserts.assertThat(result).isEmpty();

            query = "array_min(xs) != 1";
            result = tester.runQuery("xs", query);
            Asserts.assertThat(result).isEmpty();

            query = "array_avg(xs) != 1";
            result = tester.runQuery("xs", query);
            Asserts.assertThat(result).isEmpty();

            query = "array_sum(xs) != 1";
            result = tester.runQuery("xs", query);
            Asserts.assertThat(result).isEmpty();

            query = "array_length(xs, -1) != 1";
            result = tester.runQuery("xs", query);
            Asserts.assertThat(result).isEmpty();

            query = "array_lower(xs, -1) != 1";
            result = tester.runQuery("xs", query);
            Asserts.assertThat(result).isEmpty();

            query = "array_lower(xs, -1) != 1";
            result = tester.runQuery("xs", query);
            Asserts.assertThat(result).isEmpty();
        }
    }
}
