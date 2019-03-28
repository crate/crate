/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.sql;


import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class IdentifiersTest {

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
}
