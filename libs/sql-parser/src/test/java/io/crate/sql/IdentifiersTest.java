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


import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class IdentifiersTest {

    @Test
    public void test_number_of_keywords() {
        // If this test is failing you are introducing a new reserved keyword which is a breaking change.
        // Either add the new term to `nonReserved` in `SqlBase.4g` or add a breaking changes entry and adapt this test.
        assertThat(
            (int) Identifiers.KEYWORDS.stream().filter(Identifiers.Keyword::isReserved).count())
            .isEqualTo(96);
    }

    @Test
    public void testEscape() {
        assertThat(Identifiers.escape("")).isEqualTo("");
        assertThat(Identifiers.escape("\"")).isEqualTo("\"\"");
        assertThat(Identifiers.escape("ABC\"")).isEqualTo("ABC\"\"");
        assertThat(Identifiers.escape("\"\uD83D\uDCA9")).isEqualTo("\"\"\uD83D\uDCA9");
        assertThat(Identifiers.escape("abcDEF")).isEqualTo("abcDEF");
        assertThat(Identifiers.escape("œ")).isEqualTo("œ");
    }

    @Test
    public void testQuote() {
        assertThat(Identifiers.quote("")).isEqualTo("\"\"");
        assertThat(Identifiers.quote("\"")).isEqualTo("\"\"\"\"");
        assertThat(Identifiers.quote("ABC")).isEqualTo("\"ABC\"");
        assertThat(Identifiers.quote("\uD83D\uDCA9")).isEqualTo("\"\uD83D\uDCA9\"");
    }

    @Test
    public void testQuoteIfNeeded() {
        assertThat(Identifiers.quoteIfNeeded("\"")).isEqualTo("\"\"\"\"");
        assertThat(Identifiers.quoteIfNeeded("fhjgadhjgfhs")).isEqualTo("fhjgadhjgfhs");
        assertThat(Identifiers.quoteIfNeeded("fhjgadhjgfhsÖ")).isEqualTo("\"fhjgadhjgfhsÖ\"");
        assertThat(Identifiers.quoteIfNeeded("ABC")).isEqualTo("\"ABC\"");
        assertThat(Identifiers.quoteIfNeeded("abC")).isEqualTo("\"abC\""); // mixed case
        assertThat(Identifiers.quoteIfNeeded("abc\"")).isEqualTo("\"abc\"\"\"");
        assertThat(Identifiers.quoteIfNeeded("select")).isEqualTo("\"select\""); // keyword
        assertThat(Identifiers.quoteIfNeeded("1column")).isEqualTo("\"1column\"");
        assertThat(Identifiers.quoteIfNeeded("column name")).isEqualTo("\"column name\"");
        assertThat(Identifiers.quoteIfNeeded("col1a")).isEqualTo("col1a");
        assertThat(Identifiers.quoteIfNeeded("_col")).isEqualTo("_col");
        assertThat(Identifiers.quoteIfNeeded("col_1")).isEqualTo("col_1");
        assertThat(Identifiers.quoteIfNeeded("col['a']")).isEqualTo("\"col['a']\"");
    }
}
