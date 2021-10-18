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

import org.apache.lucene.search.Query;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class ArrayLengthQueryBuilderTest extends LuceneQueryBuilderTest {

    @Test
    public void testArrayLengthGtColumnIsNotOptimized() {
        Query query = convert("array_length(y_array, 1) > x");
        assertThat(query.toString(), is("(x < array_length(y_array, 1))"));
    }

    @Test
    public void testArrayLengthGt0UsesExistsQuery() {
        Query query = convert("array_length(y_array, 1) > 0");
        assertThat(
            query.toString(),
            is("ConstantScore(DocValuesFieldExistsQuery [field=y_array])"));
    }

    @Test
    public void testArrayLengthGtNULLDoesNotMatch() {
        Query query = convert("array_length(y_array, 1) > NULL");
        assertThat(query.toString(), is("MatchNoDocsQuery(\"WHERE null -> no match\")"));
    }

    @Test
    public void testArrayLengthGte1UsesNumTermsPerDocQuery() {
        Query query = convert("array_length(y_array, 1) >= 1");
        assertThat(
            query.toString(),
            is("NumTermsPerDoc: y_array")
        );
    }

    @Test
    public void testArrayLengthGt1UsesNumTermsPerDocAndGenericFunction() {
        Query query = convert("array_length(y_array, 1) > 1");
        assertThat(
            query.toString(),
            is("+NumTermsPerDoc: y_array #(array_length(y_array, 1) > 1)")
        );
    }

    @Test
    public void testArrayLengthLt1IsNoMatch() {
        Query query = convert("array_length(y_array, 1) < 1");
        assertThat(
            query.toString(),
            is("MatchNoDocsQuery(\"array_length([], 1) is NULL, so array_length([], 1) < 0 or < 1 can't match\")"));
    }

    @Test
    public void testArrayLengthLte0IsNoMatch() {
        Query query = convert("array_length(y_array, 1) <= 0");
        assertThat(
            query.toString(),
            is("MatchNoDocsQuery(\"array_length([], 1) is NULL, so array_length([], 1) <= 0 can't match\")")
        );
    }
}
