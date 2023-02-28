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

import java.util.List;

import org.junit.Test;

import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;
import io.crate.types.DataTypes;

public class QuoteIdentFunctionTest extends ScalarTestCase {

    @Test
    public void testZeroArguments() {
        expectedException.expect(UnsupportedFunctionException.class);
        expectedException.expectMessage("Unknown function: quote_ident()." +
                                        " Possible candidates: pg_catalog.quote_ident(text):text");
        assertEvaluateNull("quote_ident()");
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
}
