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

import java.util.List;

import org.apache.lucene.search.Query;
import org.assertj.core.api.Assertions;
import org.elasticsearch.Version;
import org.junit.Test;

import io.crate.testing.IndexVersionCreated;
import io.crate.testing.QueryTester;

@IndexVersionCreated(value = 8_08_00_99) // V_5_8_0
public class ArrayLengthQueryBuilderBeforeV590Test extends ArrayLengthQueryBuilderTest {

    @Test
    public void testArrayLengthGt0UsesExistsQuery() {
        Query query = convert("array_length(y_array, 1) > 0");
        assertThat(query).hasToString("(NumTermsPerDoc: y_array (array_length(y_array, 1) > 0))~1");
    }

    @Test
    public void testArrayLengthGte1UsesNumTermsPerDocQuery() {
        Query query = convert("array_length(y_array, 1) >= 1");
        assertThat(query).hasToString("(NumTermsPerDoc: y_array (array_length(y_array, 1) >= 1))~1");
    }

    @Test
    public void testArrayLengthGt1UsesNumTermsPerOrAndGenericFunction() {
        Query query = convert("array_length(y_array, 1) > 1");
        assertThat(query).hasToString("(NumTermsPerDoc: y_array (array_length(y_array, 1) > 1))~1");
    }

    @Test
    public void test_NumTermsPerDocQuery_maps_column_idents_to_oids() throws Exception {
        final long oid = 123;
        try (QueryTester tester = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.V_5_8_0,
            "create table t (int_array array(int))",
            () -> oid
        ).indexValues("int_array", List.of(), List.of(1)).build()) {
            Query query = tester.toQuery("array_length(int_array, 1) >= 1");
            assertThat(query).hasToString(String.format("(NumTermsPerDoc: %s (array_length(int_array, 1) >= 1))~1", oid));
            Assertions.assertThat(tester.runQuery("int_array", "array_length(int_array, 1) >= 1"))
                .containsExactly(List.of(1));
        }
    }
}
