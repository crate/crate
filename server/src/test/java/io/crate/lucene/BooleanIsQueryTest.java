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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;

import io.crate.exceptions.ConversionException;
import io.crate.exceptions.UnsupportedFunctionException;

public class BooleanIsQueryTest extends LuceneQueryBuilderTest {
    @Override
    protected String createStmt() {
        return "create table m (a1 boolean, a2 boolean index off, obj_ignored object (ignored))";
    }

    @Test
    public void test_is_true_query() {
        Query query = convert("a1 IS TRUE");
        assertThat(query).isExactlyInstanceOf(TermQuery.class);
        assertThat(query).hasToString("a1:T");

        query = convert("a2 IS TRUE");
        // SortedNumericDocValuesRangeQuery.class is not public
        assertThat(query.getClass().getName()).endsWith("SortedNumericDocValuesRangeQuery");
        assertThat(query).hasToString("a2:[1 TO 1]");
    }

    @Test
    public void test_is_false_query() {
        Query query = convert("a1 IS FALSE");
        assertThat(query).isExactlyInstanceOf(TermQuery.class);
        assertThat(query).hasToString("a1:F");

        query = convert("a2 IS FALSE");
        // SortedNumericDocValuesRangeQuery.class is not public
        assertThat(query.getClass().getName()).endsWith("SortedNumericDocValuesRangeQuery");
        assertThat(query).hasToString("a2:[0 TO 0]");
    }

    @Test
    public void test_is_true_and_is_false_on_null_literal_returns_no_match() {
        Query query = convert("null IS TRUE");
        assertThat(query).isExactlyInstanceOf(MatchNoDocsQuery.class);

        query = convert("null IS FALSE");
        assertThat(query).isExactlyInstanceOf(MatchNoDocsQuery.class);
    }

    @Test
    public void test_is_not_true_and_is_not_false_on_null_literal_returns_match_all() {
        Query query = convert("null IS NOT TRUE");
        assertThat(query).isExactlyInstanceOf(MatchAllDocsQuery.class);

        query = convert("null IS NOT FALSE");
        assertThat(query).isExactlyInstanceOf(MatchAllDocsQuery.class);
    }

    @Test
    public void test_is_true_falls_back_to_generic_function_query_on_ignored_objects() {
        Query query = convert("obj_ignored['x'] IS TRUE");

        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);
        assertThat(query.toString()).contains("(_doc['obj_ignored']['x'] IS true)");
    }

    @Test
    public void test_is_not_true_on_column_generates_correct_boolean_query() {
        Query query = convert("a1 IS NOT TRUE");
        assertThat(query.getClass().getName()).endsWith("BooleanQuery");

        String queryString = query.toString();
        assertThat(queryString).contains("-a1:T");
        assertThat(queryString).doesNotContain("FieldExistsQuery");
    }

    @Test
    public void test_is_true_on_invalid_type_throws_exception() {
        assertThatThrownBy(() -> convert("[] IS TRUE"))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessageContaining("Valid types: (boolean, boolean)");

        assertThatThrownBy(() -> convert("'some_text' IS FALSE"))
            .isExactlyInstanceOf(ConversionException.class)
            .hasMessageContaining("Cannot cast `'some_text'` of type `text` to type `boolean`");
    }
}
