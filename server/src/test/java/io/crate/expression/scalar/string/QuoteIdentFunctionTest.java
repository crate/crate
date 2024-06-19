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

package io.crate.expression.scalar.string;

import static io.crate.testing.Asserts.isFunction;
import static io.crate.testing.Asserts.isLiteral;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.Test;

import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;
import io.crate.sql.Identifiers;
import io.crate.types.DataTypes;

public class QuoteIdentFunctionTest extends ScalarTestCase {

    @Test
    public void testZeroArguments() {
        assertThatThrownBy(() -> {
            assertEvaluateNull("quote_ident()");

        })
                .isExactlyInstanceOf(UnsupportedFunctionException.class)
                .hasMessage("Unknown function: quote_ident()." +
                        " Possible candidates: pg_catalog.quote_ident(text):text");
    }

    @Test
    public void testQuoteIdentEvaluateNullInput() {
        assertEvaluateNull("quote_ident(null)");
    }

    @Test
    public void testQuoteIdentEvaluateWithNonIdentifierCharacters() {
        assertEvaluate("quote_ident('Foo')", "\"Foo\"");
        assertEvaluate("quote_ident('Foo bar')", "\"Foo bar\"");
        assertEvaluate("quote_ident('foo\"bar')", "\"foo\"\"bar\"");
    }

    @Test
    public void testQuoteIdentEvaluateWithRef() {
        assertEvaluate(
            "quote_ident(name)", "\"foo bar\"",
            Literal.of(DataTypes.STRING, "foo bar"));
    }

    @Test
    public void testQuoteIdentNormalize() {
        assertNormalize("quote_ident('foo')", isLiteral("foo"));
    }

    @Test
    public void testQuoteIdentNormalizeWithRefs() {
        assertNormalize(
            "quote_ident(name)",
            isFunction("quote_ident", List.of(DataTypes.STRING)));
    }

    @Test
    public void test_maybe_quote_expression_behaves_like_quote_if_needed_for_non_subscripts() {
        for (String candidate : List.of(
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
            assertThat(QuoteIdentFunction.maybeQuoteExpression(candidate)).isEqualTo(Identifiers.quoteIfNeeded(candidate));
        }
    }

    @Test
    public void test_quote_expression_quotes_only_base_part_of_subscript_expression() {
        assertThat(QuoteIdentFunction.maybeQuoteExpression("col['a']")).isEqualTo("col['a']");
        assertThat(QuoteIdentFunction.maybeQuoteExpression("Col['a']")).isEqualTo("\"Col\"['a']");
        assertThat(QuoteIdentFunction.maybeQuoteExpression("col with space['a']")).isEqualTo("\"col with space\"['a']");
    }

    @Test
    public void test_quote_expression_quotes_keywords() {
        assertThat(QuoteIdentFunction.maybeQuoteExpression("select")).isEqualTo("\"select\""); // keyword
    }

    @Test
    public void test_quote_expression_quotes_empty_ident() {
        assertThat(QuoteIdentFunction.maybeQuoteExpression("")).isEqualTo("\"\"");
    }
}
