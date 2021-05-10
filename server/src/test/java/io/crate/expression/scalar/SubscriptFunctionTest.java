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

package io.crate.expression.scalar;

import io.crate.exceptions.ConversionException;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.SelectSymbol;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.List;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;


public class SubscriptFunctionTest extends ScalarTestCase {

    @Test
    public void test_long_can_be_used_as_array_index() {
        assertEvaluate("['Youri', 'Ruben'][x]", "Youri", Literal.of(1L));
    }

    @Test
    public void test_subscript_can_retrieve_items_of_objects_within_array() {
        assertEvaluate("[{x=10}, {x=2}]['x']", List.of(10, 2));
    }

    @Test
    public void test_subscript_can_be_used_on_subqueries_returning_objects() {
        assertNormalize(
            "(select {x=10})['x']",
            isFunction("subscript", Matchers.instanceOf(SelectSymbol.class), isLiteral("x"))
        );
    }

    @Test
    public void testEvaluate() throws Exception {
        assertNormalize("subscript(['Youri', 'Ruben'], cast(1 as integer))", isLiteral("Youri"));
    }

    @Test
    public void testNormalizeSymbol() throws Exception {
        assertNormalize("subscript(tags, cast(1 as integer))", isFunction("subscript"));
    }

    @Test
    public void testIndexOutOfRange() throws Exception {
        assertNormalize("subscript(['Youri', 'Ruben'], cast(3 as integer))", isLiteral(null));
    }

    @Test
    public void testIndexExpressionIsNotInteger() throws Exception {
        expectedException.expect(ConversionException.class);
        expectedException.expectMessage("Cannot cast `'foo'` of type `text` to type `integer`");
        assertNormalize("subscript(['Youri', 'Ruben'], 'foo')", isLiteral("Ruben"));
    }
}
