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

package io.crate.expression.operator.any;

import io.crate.expression.operator.input.ObjectInput;
import io.crate.expression.scalar.AbstractScalarFunctionsTest;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.TransactionContext;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import org.junit.Test;

import java.util.Arrays;

import static io.crate.testing.SymbolMatchers.isLiteral;

public class AnyEqOperatorTest extends AbstractScalarFunctionsTest {

    private TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();

    private Boolean anyEq(Object value, Object arrayExpr) {
        AnyOperator anyOperator = new AnyOperator(
            new FunctionInfo(
                new FunctionIdent("any_=", Arrays.asList(ObjectType.untyped(), new ArrayType(ObjectType.untyped()))),
                DataTypes.BOOLEAN),
            cmp -> cmp == 0
        );
        return anyOperator.evaluate(txnCtx, new ObjectInput(value), new ObjectInput(arrayExpr));
    }

    @Test
    public void testEvaluate() throws Exception {
        assertEvaluate("1 = ANY([1])", true);
        assertEvaluate("1 = ANY([2])", false);

        assertEvaluate("{i=1, b=true} = ANY([{i=1, b=true}])", true);
        assertEvaluate("{i=1, b=true} = ANY([{i=2, b=true}])", false);
    }

    @Test
    public void testEvaluateNull() throws Exception {
        assertNull(anyEq(null, null));
        assertNull(anyEq(42, null));
        assertNull(anyEq(null, new Object[]{1}));
    }

    @Test
    public void testNormalizeSymbolNull() throws Exception {
        assertNormalize("null = ANY([null])", isLiteral(null));
        assertNormalize("42 = ANY([null])", isLiteral(null));
        assertNormalize("null = ANY([1])", isLiteral(null));
    }

    @Test
    public void testNormalizeSymbol() throws Exception {
        assertNormalize("42 = ANY([42])", isLiteral(true));
        assertNormalize("42 = ANY([1, 42, 2])", isLiteral(true));
        assertNormalize("42 = ANY([42])", isLiteral(true));
        assertNormalize("42 = ANY([41, 43, -42])", isLiteral(false));
    }

    @Test
    public void testExceptionForwardingForIllegalInput() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("cannot cast bar to Iterable");
        anyEq(1, "bar");
    }

    @Test
    public void testArrayEqAnyNestedArrayMatches() {
        assertNormalize("['foo', 'bar'] = ANY([ ['foobar'], ['foo', 'bar'], [] ])", isLiteral(true));
    }

    @Test
    public void testArrayEqAnyNestedArrayDoesNotMatch() {
        assertNormalize("['foo', 'bar'] = ANY([ ['foobar'], ['foo', 'ar'], [] ])", isLiteral(false));
    }
}
