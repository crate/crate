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

import static io.crate.testing.Asserts.isLiteral;
import static io.crate.testing.DataTypeTesting.getDataGenerator;
import static io.crate.testing.DataTypeTesting.randomType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.format.Style;
import io.crate.lucene.GenericFunctionQuery;
import io.crate.testing.DataTypeTesting;
import io.crate.testing.QueryTester;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

/*
    A <distinct predicate> tests two values to see whether they are distinct and
    returns either ``TRUE`` or ``FALSE``.
    The two expressions must be comparable. If the attributes are rows, they
    must be of the same degree and each corresponding pair of Fields must have
    comparable <data type>s.
 */
public class DistinctFromTest extends ScalarTestCase {
    @Test
    public void testEvaluateIncomparableDatatypes() {
        assertThatThrownBy(() -> assertEvaluate("3 is distinct from true", isLiteral(false)))
            .isExactlyInstanceOf(ClassCastException.class);
    }

    @Test
    public void testNormalizeSymbolNullNull() {
        // two ``NULL`` values are not distinct from one other
        assertNormalize("null is distinct from null", isLiteral(Boolean.FALSE));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void test_random_data_type() {
        DataType<?> type = DataTypeTesting.randomTypeExcluding(Set.of(DataTypes.GEO_SHAPE));
        Supplier<?> dataGenerator = getDataGenerator(type);

        Object value1 = dataGenerator.get();
        Object value2 = dataGenerator.get();
        // Make sure the two values are distinct. Important for boolean or BitStringType.
        while (((DataType) type).compare(value1, value2) == 0) {
            value2 = dataGenerator.get();
        }

        Literal<?> literal1 = Literal.ofUnchecked(type, value1);
        assertEvaluate("? IS NOT DISTINCT FROM ?", Boolean.TRUE, literal1, literal1);

        Literal<?> literal2 = Literal.ofUnchecked(type, value2);
        assertEvaluate("? IS DISTINCT FROM ?", Boolean.TRUE, literal1, literal2);
    }

    @Test
    public void test_random_data_type_against_null() {
        DataType<?> randomType = randomType();
        var val = Literal.ofUnchecked(randomType, getDataGenerator(randomType).get()).toString(Style.QUALIFIED);

        // Random value IS DISTINCT from another random value.
        assertEvaluate("? IS DISTINCT FROM null", Boolean.TRUE, Literal.of(val));
        assertEvaluate("null IS DISTINCT FROM ?", Boolean.TRUE, Literal.of(val));
    }

    @Test
    public void test_is_distinct_on_array() {
        assertEvaluate("[1, 2] IS DISTINCT FROM [1, 2]", Boolean.FALSE);
        assertEvaluate("[1, 2] IS DISTINCT FROM null", Boolean.TRUE);
        assertEvaluate("null IS DISTINCT FROM [1, 2]", Boolean.TRUE);
    }

    @Test
    public void test_terms_query_primitive_type() throws Exception {
        QueryTester.Builder builder = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table tbl (str string)");
        builder.indexValue("str", "hello");
        builder.indexValue("str", "Duke");
        builder.indexValue("str", "rules");
        builder.indexValue("str", null);
        try (QueryTester tester = builder.build()) {
            Query query = tester.toQuery("str IS DISTINCT FROM 'hello'");
            assertThat(query)
                .hasToString("+*:* -str:hello");

            assertThat(tester.runQuery("str", "str IS DISTINCT FROM 'hello'"))
                .containsExactly("Duke", "rules", null);
            assertThat(tester.runQuery("str", "str IS DISTINCT FROM null"))
                .containsExactly("hello", "Duke", "rules");
            assertThat(tester.runQuery("str", "str IS NOT DISTINCT FROM 'hello'"))
                .containsExactly("hello");
        }
    }

    @Test
    public void test_terms_query_on_internal_id_column() throws Exception {
        QueryTester.Builder builder = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table tbl (str string)");
        builder.indexValue("str", "hello");
        builder.indexValue("str", null);
        try (QueryTester tester = builder.build()) {
            var query = tester.toQuery("_id IS DISTINCT FROM 'dummy-id'");
            assertThat(query)
                .hasToString("+*:* -_id:[76 e9 a6 cb e8 9d]");
            assertThat(tester.runQuery("_id", "_id IS DISTINCT FROM null"))
                .containsExactly("dummy-id", "dummy-id");

            assertThat(tester.runQuery("str", "_id IS DISTINCT FROM 'dummy-id'"))
                .isEmpty();
        }
    }

    @Test
    public void test_is_distinct_on_null_object_results_in_generic_function() throws Exception {
        QueryTester.Builder builder = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table tbl (obj object)");
        builder.indexValue("obj", Map.of());
        builder.indexValue("obj", Map.of("a", 1));
        builder.indexValue("obj", null);
        try (QueryTester tester = builder.build()) {
            Query query = tester.toQuery("obj IS DISTINCT FROM null");
            assertThat(query)
                .isExactlyInstanceOf(GenericFunctionQuery.class)
                .hasToString("(obj IS DISTINCT FROM NULL)");

            assertThat(tester.runQuery("obj", "obj IS DISTINCT FROM null")).containsExactly(
                Map.of(), Map.of("a", 1)
            );
        }
    }

    @Test
    public void test_is_distinct_on_empty_object_results_in_generic_function() throws Exception {
        QueryTester.Builder builder = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table tbl (obj object)");
        builder.indexValue("obj", Map.of());
        builder.indexValue("obj", Map.of("a", 1));
        builder.indexValue("obj", null);
        try (QueryTester tester = builder.build()) {
            Query query = tester.toQuery("obj IS DISTINCT FROM {}");
            assertThat(query)
                .isExactlyInstanceOf(GenericFunctionQuery.class)
                .hasToString("(obj IS DISTINCT FROM {})");

            assertThat(tester.runQuery("obj", "obj IS DISTINCT FROM {}")).containsExactly(
                Map.of("a", 1), null
            );
        }
    }

    @Test
    public void test_is_distinct_on_object_with_known_childs() throws Exception {
        QueryTester.Builder builder = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table tbl (obj object as (a integer))");
        builder.indexValue("obj", Map.of());
        builder.indexValue("obj", Map.of("a", 1));
        builder.indexValue("obj", Map.of("a", 2, "b", 2));
        builder.indexValue("obj", null);
        try (QueryTester tester = builder.build()) {
            Query query = tester.toQuery("obj IS DISTINCT FROM {a=1}");
            assertThat(query)
                .hasToString("+(+*:* -obj.a:[1 TO 1])");

            assertThat(tester.runQuery("obj", "obj IS DISTINCT FROM {a=1}")).containsExactly(
                Map.of(), Map.of("a", 2, "b", 2), null
            );
        }
    }

    @Test
    public void test_is_distinct_on_object_including_unknown_childs() throws Exception {
        QueryTester.Builder builder = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table tbl (obj object as (a integer))");
        builder.indexValue("obj", Map.of());
        builder.indexValue("obj", Map.of("a", 1));
        builder.indexValue("obj", Map.of("a", 2, "b", 2));
        builder.indexValue("obj", null);
        try (QueryTester tester = builder.build()) {
            var query = tester.toQuery("obj IS DISTINCT FROM {a=2, b=2}");
            assertThat(query)
                .hasToString("+(+*:* -obj.a:[2 TO 2]) #(obj IS DISTINCT FROM {\"a\"=2, \"b\"=2})");

            assertThat(tester.runQuery("obj", "obj IS DISTINCT FROM {a=2, b=2}")).containsExactly(
                Map.of(), Map.of("a", 1), null
            );
        }
    }

    @Test
    public void test_is_distinct_on_null_array_uses_field_exists() throws Exception {
        QueryTester.Builder builder = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table tbl (arr array(integer))");
        builder.indexValue("arr", List.of());
        builder.indexValue("arr", List.of(1, 2));
        builder.indexValue("arr", null);
        try (QueryTester tester = builder.build()) {
            Query query = tester.toQuery("arr IS DISTINCT FROM null");
            assertThat(query)
                .hasToString("FieldExistsQuery [field=_array_length_arr]");

            assertThat(tester.runQuery("arr", "arr IS DISTINCT FROM null")).containsExactly(
                List.of(), List.of(1, 2)
            );
        }

    }

    @Test
    public void test_is_distinct_on_array_uses_array_term_query() throws Exception {
        QueryTester.Builder builder = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table tbl (arr array(integer))");
        builder.indexValue("arr", List.of());
        builder.indexValue("arr", List.of(1, 2));
        builder.indexValue("arr", null);
        try (QueryTester tester = builder.build()) {
            Query query = tester.toQuery("arr IS DISTINCT FROM [1]");
            assertThat(query)
                .hasToString("-arr:{1} +(arr IS DISTINCT FROM [1])");

            assertThat(tester.runQuery("arr", "arr IS DISTINCT FROM [1, 2]")).containsExactly(
                List.of(), null
            );
        }
    }

    @Test
    public void test_is_distinct_on_empty_array_uses_array_length_query() throws Exception {
        QueryTester.Builder builder = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table tbl (arr array(integer))");
        builder.indexValue("arr", List.of());
        builder.indexValue("arr", List.of(1, 2));
        builder.indexValue("arr", null);
        try (QueryTester tester = builder.build()) {
            Query query = tester.toQuery("arr IS DISTINCT FROM []");
            assertThat(query)
                .hasToString("+*:* -_array_length_arr:[0 TO 0]");

            assertThat(tester.runQuery("arr", "arr IS DISTINCT FROM []")).containsExactly(
                List.of(1, 2), null
            );
        }

    }
}
