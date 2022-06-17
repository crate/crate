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

package io.crate.sql;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import java.util.List;

import org.junit.Test;

public class IdentifiersTest {

    @Test
    public void test_number_of_keywords() {
        // If this test is failing you are introducing a new reserved keyword which is a breaking change.
        // Either add the new term to `nonReserved` in `SqlBase.4g` or add a breaking changes entry and adapt this test.
        assertThat(
            (int) Identifiers.KEYWORDS.stream().filter(Identifiers.Keyword::isReserved).count(),
            is(94)
        );
    }

    @Test
    public void testEscape() {
        assertThat(Identifiers.escape(""), is(""));
        assertThat(Identifiers.escape("\""), is("\"\""));
        assertThat(Identifiers.escape("ABC\""), is("ABC\"\""));
        assertThat(Identifiers.escape("\"\uD83D\uDCA9"), is("\"\"\uD83D\uDCA9"));
        assertThat(Identifiers.escape("abcDEF"), is("abcDEF"));
        assertThat(Identifiers.escape("œ"), is("œ"));
    }

    @Test
    public void testQuote() {
        assertThat(Identifiers.quote(""), is("\"\""));
        assertThat(Identifiers.quote("\""), is("\"\"\"\""));
        assertThat(Identifiers.quote("ABC"), is("\"ABC\""));
        assertThat(Identifiers.quote("\uD83D\uDCA9"), is("\"\uD83D\uDCA9\""));
    }

    @Test
    public void testQuoteIfNeeded() {
        assertThat(Identifiers.quoteIfNeeded(""), is("\"\""));
        assertThat(Identifiers.quoteIfNeeded("\""), is("\"\"\"\""));
        assertThat(Identifiers.quoteIfNeeded("fhjgadhjgfhs"), is("fhjgadhjgfhs"));
        assertThat(Identifiers.quoteIfNeeded("fhjgadhjgfhsÖ"), is("\"fhjgadhjgfhsÖ\""));
        assertThat(Identifiers.quoteIfNeeded("ABC"), is("\"ABC\""));
        assertThat(Identifiers.quoteIfNeeded("abc\""), is("\"abc\"\"\""));
        assertThat(Identifiers.quoteIfNeeded("select"), is("\"select\"")); // keyword
        assertThat(Identifiers.quoteIfNeeded("1column"), is("\"1column\""));
        assertThat(Identifiers.quoteIfNeeded("column name"), is("\"column name\""));
        assertThat(Identifiers.quoteIfNeeded("col1a"), is("col1a"));
        assertThat(Identifiers.quoteIfNeeded("_col"), is("_col"));
        assertThat(Identifiers.quoteIfNeeded("col_1"), is("col_1"));
        assertThat(Identifiers.quoteIfNeeded("col['a']"), is("\"col['a']\""));
    }

    @Test
    public void test_maybe_quote_expression_behaves_like_quote_if_needed_for_non_subscripts() throws Exception {
        for (String candidate : List.of(
            (""),
            ("\""),
            ("fhjgadhjgfhs"),
            ("fhjgadhjgfhsÖ"),
            ("ABC"),
            ("abc\""),
            ("select"),
            ("1column"),
            ("column name"),
            ("col1a"),
            ("_col"),
            ("col_1")
        )) {
            assertThat(Identifiers.maybeQuoteExpression(candidate), is(Identifiers.quoteIfNeeded(candidate)));
        }
    }

    @Test
    public void test_quote_expression_quotes_only_base_part_of_subscript_expression() throws Exception {
        assertThat(Identifiers.maybeQuoteExpression("col['a']"), is("col['a']"));
        assertThat(Identifiers.maybeQuoteExpression("Col['a']"), is("\"Col\"['a']"));
        assertThat(Identifiers.maybeQuoteExpression("col with space['a']"), is("\"col with space\"['a']"));
    }

    @Test
    public void test_quote_expression_quotes_keywords() throws Exception {
        assertThat(Identifiers.maybeQuoteExpression("select"), is("\"select\"")); // keyword
    }
}
