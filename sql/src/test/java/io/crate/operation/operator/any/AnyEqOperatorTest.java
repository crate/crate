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

package io.crate.operation.operator.any;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.operator.input.ObjectInput;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.Arrays;

import static io.crate.testing.SymbolMatchers.isLiteral;

public class AnyEqOperatorTest extends AbstractScalarFunctionsTest {

    private Boolean anyEq(Object value, Object arrayExpr) {

        AnyEqOperator anyEqOperator = new AnyEqOperator(
            new FunctionInfo(
                new FunctionIdent("any_=", Arrays.<DataType>asList(DataTypes.INTEGER, new ArrayType(DataTypes.INTEGER))),
                DataTypes.BOOLEAN)
        );
        return anyEqOperator.evaluate(new ObjectInput(value), new ObjectInput(arrayExpr));
    }

    @Test
    public void testEvaluate() throws Exception {
        assertEvaluate("1 = ANY([1])", true);
        assertEvaluate("1 = ANY([2])", false);

        /*
        below is the same as - but the ExpressionAnalyzer prohibits this currently
        assertEvaluate("{i=1, b=true} = ANY([{i=1, b=true}])", true);
        assertEvaluate("{i=1, b=true} = ANY([{i=2, b=true}])", false);
        */
        assertTrue(anyEq(
            ImmutableMap.<String, Object>builder()
                .put("int", 1)
                .put("boolean", true)
                .build(),
            new Object[]{
                ImmutableMap.<String, Object>builder()
                    .put("int", 1)
                    .put("boolean", true)
                    .build()
            }
        ));
        assertFalse(anyEq(
            ImmutableMap.<String, Object>builder()
                .put("int", 1)
                .put("boolean", true)
                .build(),
            new Object[]{
                ImmutableMap.<String, Object>builder()
                    .put("int", 2)
                    .put("boolean", false)
                    .build()
            }
        ));
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
    public void testExceptionForwarding() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        anyEq(1, "bar");
    }
}
