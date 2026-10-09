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

import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.junit.Test;

import io.crate.lucene.LuceneQueryBuilderTest;

public class EndsWithFunctionQueryTest extends LuceneQueryBuilderTest {

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
    public void test_ends_with_creates_automaton_query() {
        Query query = convert("ends_with(a1, 'abc')");
        assertThat(query).isExactlyInstanceOf(AutomatonQuery.class);
        assertSuffixMatches(query, "abc");
    }

    // Every non-NULL value ends with '', therefore all rows which have a value must match
    @Test
    public void test_ends_with_empty_suffix_creates_field_exists_query() {
        Query query = convert("ends_with(a1, '')");
        assertThat(query).isExactlyInstanceOf(FieldExistsQuery.class);
        assertThat(query).hasToString("FieldExistsQuery [field=a1]");
    }

    @Test
    public void test_ends_with_empty_suffix_on_columnstore_disabled_uses_field_names() {
        // Without doc values the existence of a value is resolved via the _field_names column
        Query query = convert("ends_with(a3, '')");
        assertThat(query).hasToString("ConstantScore(_field_names:a3)");
    }

    @Test
    public void test_ends_with_on_non_indexed_column_returns_generic_func_query() {
        Query query = convert("ends_with(a2, 'abc')");
        assertThat(query).hasToString("ends_with(a2, 'abc')");
    }

    @Test
    public void test_ends_with_on_columnstore_disabled_creates_automaton_query() {
        Query query = convert("ends_with(a3, 'abc')");
        assertThat(query).isExactlyInstanceOf(AutomatonQuery.class);
        assertSuffixMatches(query, "abc");
    }

    @Test
    public void test_ends_with_on_non_indexed_and_columnstore_disabled_returns_generic_func_query() {
        Query query = convert("ends_with(a4, 'abc')");
        assertThat(query).hasToString("ends_with(a4, 'abc')");
    }

    @Test
    public void test_ends_with_on_non_literal_argument_returns_generic_func_query() {
        Query query = convert("ends_with(a1, a2)");
        assertThat(query).hasToString("ends_with(a1, a2)");
    }

    @Test
    public void test_ends_with_treats_special_characters_as_literal_suffixes() {
        for (String suffix : new String[] {"a*b", "a?b", "a\\b", "a\\*?b", "é😀", "aaa"}) {
            Query query = convert("ends_with(a1, $1)", suffix);
            assertThat(query).isExactlyInstanceOf(AutomatonQuery.class);
            assertSuffixMatches(query, suffix);
        }
    }

    private static void assertSuffixMatches(Query query, String suffix) {
        var automaton = new CharacterRunAutomaton(((AutomatonQuery) query).getAutomaton());
        for (String text : new String[] {suffix, "prefix" + suffix, "😀" + suffix, "", "unrelated",
                                         suffix + "x", suffix.replace("*", "XYZ").replace("?", "Q")}) {
            assertThat(automaton.run(text)).as("text=%s, suffix=%s", text, suffix).isEqualTo(text.endsWith(suffix));
        }
    }
}
