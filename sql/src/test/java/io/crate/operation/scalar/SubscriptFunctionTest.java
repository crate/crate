/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.scalar;

import com.google.common.collect.ImmutableSet;
import io.crate.analyze.symbol.Literal;
import io.crate.types.DataTypes;
import io.crate.types.SetType;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;


public class SubscriptFunctionTest extends AbstractScalarFunctionsTest {

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
    public void testNotRegisteredForSets() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("unknown function: subscript(long_set, integer)");
        assertEvaluate("subscript(long_set, a)", 3L,
            Literal.of(ImmutableSet.of(3L, 7L), new SetType(DataTypes.LONG)),
            Literal.of(DataTypes.INTEGER, 1)
        );
    }

    @Test
    public void testIndexExpressionIsNotInteger() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("unknown function: subscript(string_array, long)");
        assertNormalize("subscript(['Youri', 'Ruben'], 1 + 1)", isLiteral("Ruben"));
    }
}
