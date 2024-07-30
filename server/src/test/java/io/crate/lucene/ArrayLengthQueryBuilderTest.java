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

    @Test
    public void test_array_length_on_array_sub_columns_of_array_of_objects() {
        Query query = convert("array_length(o_array['xs'], 1) > 0");
        assertThat(query).hasToString("_array_length_xs:[1 TO 2147483647]");
    }

    public void test_array_length_on_object_array() {
        Query query = convert("array_length(o_array, 1) >= 1");
        assertThat(query).hasToString(
            "_array_length_o_array:[1 TO 2147483647]");
    }
}
