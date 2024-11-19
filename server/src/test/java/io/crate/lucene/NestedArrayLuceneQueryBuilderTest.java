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

package io.crate.lucene;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.Version;
import org.junit.Test;

import io.crate.testing.QueryTester;

public class NestedArrayLuceneQueryBuilderTest extends LuceneQueryBuilderTest {

    @Override
    protected String createStmt() {
        return """
            create table t (
                a int[][],
                b int[],
                c int[][][]
            )
            """;
    }

    @Test
    public void test_nested_array_equals() {
        var query = convert("a = [[1], [1, 2], null]");
        // pre-filter by a terms query with 1 and 2 then a generic function query to make sure an exact match
        assertThat(query.toString()).isEqualTo("+a:{1 2} +(a = [[1], [1, 2], NULL])");
    }

    @Test
    public void test_empty_nested_array_equals() {
        var query = convert("a = [[]]");
        assertThat(query.toString()).isEqualTo("(a = [[]])");

        query = convert("a[1] = []");
        assertThat(query.toString()).isEqualTo("(a[1] = [])");
    }

    @Test
    public void test_empty_nested_array_equals_execution() throws Exception {
        QueryTester.Builder builder = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table t (a int[][])"
        );
        List<Integer> nullArray = new ArrayList<>();
        nullArray.add(null);
        builder.indexValue("a", List.of(List.of()));
        builder.indexValue("a", List.of(List.of(1)));
        builder.indexValue("a", List.of());
        builder.indexValue("a", null);
        builder.indexValue("a", nullArray);
        builder.indexValue("a", List.of(nullArray));
        builder.indexValue("a", List.of(nullArray, List.of()));

        try (QueryTester tester = builder.build()) {
            assertThat(tester.runQuery("a", "a = [[]]")).containsExactly(List.of(List.of()));
            assertThat(tester.runQuery("a", "a = []")).containsExactly(List.of());
            assertThat(tester.runQuery("a", "a[1] = []")).containsExactly(List.of(List.of()));
        }
    }

    @Test
    public void test_array_length_scalar_on_nested_array() {
        var query = convert("array_length(a, 1) = 1");
        assertThat(query.toString()).isEqualTo("_array_length_a:[1 TO 1]");

        query = convert("array_length(a[1], 1) = 1");
        assertThat(query.toString()).isEqualTo("(array_length(a[1], 1) = 1)");

        query = convert("array_length(a, 2) = 1");
        assertThat(query.toString()).isEqualTo("(array_length(a, 2) = 1)");
    }

    @Test
    public void test_is_null_predicate_on_nested_array() {
        var query = convert("a is null");
        assertThat(query.toString()).isEqualTo("+*:* -FieldExistsQuery [field=_array_length_a]");

        query = convert("a[1] is null");
        assertThat(query.toString()).isEqualTo("(a[1] IS NULL)");
    }

    @Test
    public void test_subscript_nested_array_equals() {
        var query = convert("a[1] = [1, 2]");
        // pre-filter by a terms query with 1 and 2 then a generic function query to make sure an exact match
        assertThat(query.toString()).isEqualTo("+a:{1 2} #(a[1] = [1, 2])");
    }

    @Test
    public void test_any_equals_nested_array_literal() {
        var query = convert("a = any([ [ [1], [1, 2] ], [ [3], [4, 5] ] ])");
        // pre-filter by a terms query with 1, 2, 3, 4, 5 then a generic function query to make sure an exact match
        assertThat(query.toString()).isEqualTo("+a:{1 2 3 4 5} #(a = ANY([[[1], [1, 2]], [[3], [4, 5]]]))");
    }

    @Test
    public void test_any_equals_nested_array_literal_with_automatic_array_dimension_leveling() {
        var query = convert("b = any([ [ [1], [1, 2] ], [ [3], [4, 5] ] ])");
        // pre-filter by a terms query with 1, 2, 3, 4, 5 then a generic function query to make sure an exact match
        assertThat(query.toString()).isEqualTo("+b:{1 2 3 4 5} #(b = ANY([[1], [1, 2], [3], [4, 5]]))");
    }

    @Test
    public void test_any_not_equals_on_array_literal_and_nested_array_ref_with_automatic_array_dimension_leveling() {
        var query = convert("[1] != any(c)");
        assertThat(query.toString()).isEqualTo("([1] <> ANY(array_unnest(c)))");
    }

    @Test
    public void test_lengths_of_nested_array_of_objects() throws Exception {
        QueryTester.Builder builder = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table t (o array(array(object as (a int))))"
        );
        var val = List.of(
            List.of(Map.of("a", 1)),
            List.of(
                Map.of("a", 2),
                Map.of("a", 3),
                Map.of("a", 4)
            )
        );
        builder.indexValue("o", val);
        try (QueryTester tester = builder.build()) {
            assertThat(tester.runQuery("o", "array_length(o, 1) = 1")).isEmpty();
            assertThat(tester.runQuery("o", "array_length(o, 1) = 2")).containsExactly(val);
            assertThat(tester.runQuery("o", "array_length(o, 1) = 3")).isEmpty();

            assertThat(tester.runQuery("o", "array_length(o, 2) = 1")).isEmpty();
            assertThat(tester.runQuery("o", "array_length(o, 2) = 2")).isEmpty();
            assertThat(tester.runQuery("o", "array_length(o, 2) = 3")).containsExactly(val);

            assertThat(tester.runQuery("o", "array_length(o[1], 1) = 1")).containsExactly(val);
            assertThat(tester.runQuery("o", "array_length(o[1], 1) = 2")).isEmpty();
            assertThat(tester.runQuery("o", "array_length(o[1], 1) = 3")).isEmpty();

            assertThat(tester.runQuery("o", "array_length(o[2], 1) = 1")).isEmpty();
            assertThat(tester.runQuery("o", "array_length(o[2], 1) = 2")).isEmpty();
            assertThat(tester.runQuery("o", "array_length(o[2], 1) = 3")).containsExactly(val);

            assertThat(tester.runQuery("o", "array_length(o['a'], 1) = 1")).isEmpty();
            assertThat(tester.runQuery("o", "array_length(o['a'], 1) = 2")).containsExactly(val);
            assertThat(tester.runQuery("o", "array_length(o['a'], 1) = 3")).isEmpty();
            assertThat(tester.runQuery("o", "array_length(o['a'], 1) = 4")).isEmpty();

            assertThat(tester.runQuery("o", "array_length(o['a'], 2) = 1")).isEmpty();
            assertThat(tester.runQuery("o", "array_length(o['a'], 2) = 2")).isEmpty();
            assertThat(tester.runQuery("o", "array_length(o['a'], 2) = 3")).containsExactly(val);
            assertThat(tester.runQuery("o", "array_length(o['a'], 2) = 4")).isEmpty();
        }
    }

    @Test
    public void test_length_of_arrays_within_object_arrays() throws Exception {
        QueryTester.Builder builder = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table tbl (o_array array(object as (xs int[], o_array_2 array(object as (xs2 int[])))))"
        );
        var o_array = List.of(
            Map.of(
                "xs", // length = 2
                List.of(1, 2),
                "o_array_2", // length = 4
                List.of(
                    Map.of(
                        "xs2", // length = 5
                        List.of(1, 2, 3, 4, 5)),
                    Map.of("xs2", List.of(1, 2, 3, 4, 5)),
                    Map.of("xs2", List.of(1, 2, 3, 4, 5)),
                    Map.of("xs2", List.of(1, 2, 3, 4, 5))
                )
            )
        );
        builder.indexValue("o_array", o_array);
        try (QueryTester tester = builder.build()) {
            assertThat(tester.runQuery("o_array", "array_length(o_array, 1) = 1")).containsExactly(o_array);
            assertThat(tester.runQuery("o_array", "array_length(o_array, 1) != 1")).isEmpty();

            assertThat(tester.runQuery("o_array", "array_length(o_array['xs'], 1) = 1")).containsExactly(o_array);
            assertThat(tester.runQuery("o_array", "array_length(o_array['xs'], 1) != 1")).isEmpty();

            assertThat(tester.runQuery("o_array", "array_length(o_array['xs'][1], 1) = 2")).containsExactly(o_array);
            assertThat(tester.runQuery("o_array", "array_length(o_array['xs'][1], 1) != 2")).isEmpty();

            assertThat(tester.runQuery("o_array", "array_length(o_array['o_array_2'], 1) = 1")).containsExactly(o_array);
            assertThat(tester.runQuery("o_array", "array_length(o_array['o_array_2'], 1) != 1")).isEmpty();

            assertThat(tester.runQuery("o_array", "array_length(o_array['o_array_2'][1], 1) = 4")).containsExactly(o_array);
            assertThat(tester.runQuery("o_array", "array_length(o_array['o_array_2'][1], 1) != 4")).isEmpty();

            assertThat(tester.runQuery("o_array", "array_length(o_array['o_array_2']['xs2'], 1) = 1")).containsExactly(o_array);
            assertThat(tester.runQuery("o_array", "array_length(o_array['o_array_2']['xs2'], 1) != 1")).isEmpty();

            assertThat(tester.runQuery("o_array", "array_length(o_array['o_array_2']['xs2'][1], 1) = 4")).containsExactly(o_array);
            assertThat(tester.runQuery("o_array", "array_length(o_array['o_array_2']['xs2'][1], 1) != 4")).isEmpty();

            assertThat(tester.runQuery("o_array", "array_length(o_array['o_array_2']['xs2'][1][1], 1) = 5")).containsExactly(o_array);
            assertThat(tester.runQuery("o_array", "array_length(o_array['o_array_2']['xs2'][1][1], 1) != 5")).isEmpty();

            // array lengths of the children of the root level array are equal to the array length of the root level array
            assertThat(tester.toQuery("array_length(o_array, 1) = 1").toString()).isEqualTo("_array_length_o_array:[1 TO 1]");
            assertThat(tester.toQuery("array_length(o_array['xs'], 1) = 1").toString()).isEqualTo("_array_length_o_array:[1 TO 1]");
            assertThat(tester.toQuery("array_length(o_array['o_array_2'], 1) = 1").toString()).isEqualTo("_array_length_o_array:[1 TO 1]");
            assertThat(tester.toQuery("array_length(o_array['o_array_2']['xs2'], 1) = 1").toString()).isEqualTo("_array_length_o_array:[1 TO 1]");

            // array length fields are not indexed for arrays within arrays, utilize generic function queries
            assertThat(tester.toQuery("array_length(o_array['xs'][1], 1) = 2").toString()).isEqualTo("(array_length(o_array[1]['xs'], 1) = 2)");
            assertThat(tester.toQuery("array_length(o_array['o_array_2'][1], 1) = 4").toString()).isEqualTo("(array_length(o_array[1]['o_array_2'], 1) = 4)");
            assertThat(tester.toQuery("array_length(o_array['o_array_2']['xs2'][1], 1) = 4").toString()).isEqualTo("(array_length(o_array[1]['o_array_2']['xs2'], 1) = 4)");
            assertThat(tester.toQuery("array_length(o_array['o_array_2']['xs2'][1][1], 1) = 5").toString()).isEqualTo("(array_length(o_array[1]['o_array_2']['xs2'][1], 1) = 5)");
        }
    }
}
