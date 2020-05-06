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

package io.crate.expression.scalar;

import io.crate.expression.symbol.Literal;
import org.junit.Test;

import java.util.Map;

import static io.crate.testing.SymbolMatchers.isLiteral;

public class SubscriptObjectFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testEvaluate() throws Exception {
        assertEvaluate("subscript_obj({x=10}, 'x')", 10L);
        assertEvaluate("subscript_obj(subscript_obj({x={y=10}}, 'x'), 'y')", 10L);
    }

    @Test
    public void testSubscriptOnObjectLiteralWithMultipleSubscriptParts() throws Exception {
        assertNormalize("{\"x\" = 'test'}['x']", isLiteral("test"));
        assertNormalize("{\"x\" = { \"y\" = 'test'}}['x']['y']", isLiteral("test"));
        assertNormalize("{\"x\" = {\"y\" = {\"z\" = 'test'}}}['x']['y']['z']", isLiteral("test"));
    }

    @Test
    public void testSubscriptOnCastToObjectLiteral() throws Exception {
        assertNormalize("subscript_obj('{\"x\": 1.0}'::object, 'x')", isLiteral(1.0));
    }

    @Test
    public void testEvaluateOnObjectReference() throws Exception {
        assertEvaluate("subscript_obj(obj, 'x')", 10L, Literal.of(Map.of("x", 10L)));
    }

    @Test
    public void testSubscriptOnObjectLiteralWithNonExistingKey() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        assertEvaluate("subscript_obj(obj, 'y')", 10L, Literal.of(Map.of("x", 10L)));
    }

    @Test
    public void testFunctionCanBeUsedAsIndexInSubscript() {
        assertNormalize("{\"x\" = 10}['x' || '']", isLiteral(10L));
    }

    @Test
    public void testSubscriptOnObjectWithPath() {
        assertEvaluate("subscript_obj({x={y=10}}, 'x', 'y')", 10L);
    }
}
