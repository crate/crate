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

package io.crate.expression.scalar.string;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.junit.Test;

import io.crate.lucene.LuceneQueryBuilderTest;

public class StartsWithFunctionQueryTest extends LuceneQueryBuilderTest {

    @Override
    protected String createStmt() {
        return """
            create table m (
                a1 text,
                a2 text index off,
                a3 text storage with (columnstore = false),
                a4 text index off storage with (columnstore = false)
            )
            """;
    }

    @Test
    public void test_starts_with_creates_prefix_query() {
        Query query = convert("starts_with(a1, 'abc')");
        assertThat(query).isExactlyInstanceOf(PrefixQuery.class);
        assertThat(query).hasToString("a1:abc*");
    }

    // https://github.com/crate/crate/issues/19923
    // Every non-NULL value starts with '', therefore all rows which have a value must match
    @Test
    public void test_starts_with_empty_prefix_creates_field_exists_query() {
        Query query = convert("starts_with(a1, '')");
        assertThat(query).isExactlyInstanceOf(FieldExistsQuery.class);
        assertThat(query).hasToString("FieldExistsQuery [field=a1]");
    }

    @Test
    public void test_starts_with_empty_prefix_on_columnstore_disabled_uses_field_names() {
        // Without doc values the existence of a value is resolved via the _field_names column
        Query query = convert("starts_with(a3, '')");
        assertThat(query).hasToString("ConstantScore(_field_names:a3)");
    }

    @Test
    public void test_starts_with_on_non_indexed_column_returns_generic_func_query() {
        Query query = convert("starts_with(a2, 'abc')");
        assertThat(query).hasToString("starts_with(a2, 'abc')");
    }

    @Test
    public void test_starts_with_on_columnstore_disabled_creates_prefix_query() {
        Query query = convert("starts_with(a3, 'abc')");
        assertThat(query).isExactlyInstanceOf(PrefixQuery.class);
        assertThat(query).hasToString("a3:abc*");
    }

    @Test
    public void test_starts_with_on_non_indexed_and_columnstore_disabled_returns_generic_func_query() {
        Query query = convert("starts_with(a4, 'abc')");
        assertThat(query).hasToString("starts_with(a4, 'abc')");
    }

    @Test
    public void test_starts_with_on_non_literal_argument_returns_generic_func_query() {
        Query query = convert("starts_with(a1, a2)");
        assertThat(query).hasToString("starts_with(a1, a2)");
    }
}
