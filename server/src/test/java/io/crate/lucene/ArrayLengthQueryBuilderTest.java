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

package io.crate.lucene;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.lucene.search.Query;
import org.assertj.core.api.Assertions;
import org.elasticsearch.Version;
import org.junit.Test;

import io.crate.testing.QueryTester;

public class ArrayLengthQueryBuilderTest extends LuceneQueryBuilderTest {

    @Test
    public void testArrayLengthGtColumnIsNotOptimized() {
        Query query = convert("array_length(y_array, 1) > x");
        assertThat(query).hasToString("(x < array_length(y_array, 1))");
    }

    @Test
    public void testArrayLengthGt0UsesExistsQuery() {
        Query query = convert("array_length(y_array, 1) > 0");
        assertThat(query).hasToString("_array_length_y_array:[1 TO 2147483647]");
    }

    @Test
    public void testArrayLengthGtNULLDoesNotMatch() {
        Query query = convert("array_length(y_array, 1) > NULL");
        assertThat(query).hasToString("MatchNoDocsQuery(\"WHERE null -> no match\")");
    }

    @Test
    public void testArrayLengthGte1UsesNumTermsPerDocQuery() {
        Query query = convert("array_length(y_array, 1) >= 1");
        assertThat(query).hasToString("_array_length_y_array:[1 TO 2147483647]");
    }

    @Test
    public void testArrayLengthGt1UsesNumTermsPerOrAndGenericFunction() {
        Query query = convert("array_length(y_array, 1) > 1");
        assertThat(query).hasToString("_array_length_y_array:[2 TO 2147483647]");
    }

    @Test
    public void testArrayLengthLt1IsNoMatch() {
        Query query = convert("array_length(y_array, 1) < 1");
        assertThat(query).hasToString(
            "MatchNoDocsQuery(\"array_length([], 1) is NULL, so array_length([], 1) < 0 or < 1 can't match\")");
    }

    @Test
    public void testArrayLengthLte0IsNoMatch() {
        Query query = convert("array_length(y_array, 1) <= 0");
        assertThat(query).hasToString(
            "MatchNoDocsQuery(\"array_length([], 1) is NULL, so array_length([], 1) <= 0 can't match\")");
    }

    @Test
    public void test_array_length_with_dimension_arg_exceeding_array_dimension_is_no_match() {
        Query query = convert("array_length(y_array, 2) = 3");
        assertThat(query).hasToString(
            "MatchNoDocsQuery(\"Dimension argument <= 0 or exceeding the dimension of the array cannot match\")");

        query = convert("array_length(y_array, 0) = 3");
        assertThat(query).hasToString(
            "MatchNoDocsQuery(\"Dimension argument <= 0 or exceeding the dimension of the array cannot match\")");
    }

    @Test
    public void test_NumTermsPerDocQuery_maps_column_idents_to_oids() throws Exception {
        final long oid = 123;
        try (QueryTester tester = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table t (int_array array(int))",
            () -> oid
        ).indexValues("int_array", List.of(), List.of(1)).build()) {
            Query query = tester.toQuery("array_length(int_array, 1) >= 1");
            assertThat(query).hasToString(String.format("_array_length_%s:[1 TO 2147483647]", oid));
            Assertions.assertThat(tester.runQuery("int_array", "array_length(int_array, 1) >= 1"))
                .containsExactly(List.of(1));
        }
    }

    public void test_array_length_on_object_array() {
        Query query = convert("array_length(o_array, 1) >= 1");
        assertThat(query).hasToString(
            "_array_length_o_array:[1 TO 2147483647]");
    }

    @Test
    public void test_array_length_on_object_arrays_subcolumn() {
        // 'o_array' is indexed but 'o_array['xs']' is not indexed. We can use the fact that
        // for every element of 'o_array' there exists its sub-column 'xs' such that
        // array_length(o_array, 1) = array_length(o_array['xs'], 1)
        Query query = convert("array_length(o_array['xs'], 1) >= 1");
        assertThat(query).hasToString("_array_length_o_array:[1 TO 2147483647]");

        query = convert("array_length(o_array['obj']['x'], 1) >= 1");
        assertThat(query).hasToString("_array_length_o_array:[1 TO 2147483647]");

        query = convert("array_length(o_array['o_array_2']['x'], 1) >= 1");
        assertThat(query).hasToString("_array_length_o_array:[1 TO 2147483647]");

        // 'obj' is not array(object)
        query = convert("array_length(obj['o_array']['x'], 1) >= 1");
        assertThat(query).hasToString("_array_length_o_array:[1 TO 2147483647]");

        // when the dimension argument is > 1, then fall back to GenericFunctionQuery
        query = convert("array_length(o_array['xs'], 2) >= 1");
        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);
        assertThat(query).hasToString("(array_length(o_array['xs'], 2) >= 1)");

        // when the ref symbol is not a Reference but a function(subscript function in this case), then fall back to GenericFunctionQuery
        query = convert("array_length(o_array['xs'][1], 1) >= 1");
        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);
        assertThat(query).hasToString("(array_length(o_array[1]['xs'], 1) >= 1)");
    }
}
