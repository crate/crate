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

import static io.crate.testing.Asserts.isLiteral;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.Literal;

public class SubscriptObjectFunctionTest extends ScalarTestCase {

    @Test
    public void testEvaluate() throws Exception {
        assertEvaluate("subscript_obj({x=10}, 'x')", 10);
        assertEvaluate("subscript_obj(subscript_obj({x={y=10}}, 'x'), 'y')", 10);
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
        assertThatThrownBy(() -> assertEvaluate("subscript_obj(obj, 'y')", 10L, Literal.of(Map.of("x", 10L))))
            .isExactlyInstanceOf(ColumnUnknownException.class);
    }

    @Test
    public void testFunctionCanBeUsedAsIndexInSubscript() {
        assertNormalize("{\"x\" = 10}['x' || '']", isLiteral(10));
    }

    @Test
    public void testSubscriptOnObjectWithPath() {
        assertEvaluate("subscript_obj({x={y=10}}, 'x', 'y')", 10);
    }

    @Test
    public void test_subscript_obj_with_null_argument() throws Exception {
        assertEvaluateNull("subscript_obj(null, 'x', 'y')");
    }

    @Test
    public void test_subscript_obj_with_null_child() throws Exception {
        assertEvaluateNull("subscript_obj({x=null}, 'x', 'y')");
    }

    @Test
    public void testEvaluateNestedObjectWithUnknownObjectkeysWithSessionSetting() throws Exception {
        // missing key in the very front
        sqlExpressions.setErrorOnUnknownObjectKey(true);
        Assertions.assertThatThrownBy(() -> assertEvaluate("subscript_obj(subscript_obj({x={y=10}}, 'y'), 'y')", null))
            .isExactlyInstanceOf(ColumnUnknownException.class)
            .hasMessageContaining("The object `{x={y=10}}` does not contain the key `y`");
        sqlExpressions.setErrorOnUnknownObjectKey(false);
        assertEvaluateNull("subscript_obj(subscript_obj({x={y=10}}, 'y'), 'y')");
        // missing key in the middle
        sqlExpressions.setErrorOnUnknownObjectKey(true);
        Assertions.assertThatThrownBy(() -> assertEvaluate("{\"x\" = {\"y\" = {\"z\" = 'test'}}}['x']['x']['z']", null))
            .isExactlyInstanceOf(ColumnUnknownException.class)
            .hasMessageContaining("The object `{y={z=test}}` does not contain the key `x`");
        sqlExpressions.setErrorOnUnknownObjectKey(false);
        assertEvaluateNull("{\"x\" = {\"y\" = {\"z\" = 'test'}}}['x']['x']['z']");
    }
}
