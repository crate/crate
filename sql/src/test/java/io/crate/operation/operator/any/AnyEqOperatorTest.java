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
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.Arrays;

public class AnyEqOperatorTest extends CrateUnitTest {

    private Boolean anyEq(Object value, Object arrayExpr) {

        AnyEqOperator anyEqOperator = new AnyEqOperator(
                new FunctionInfo(
                        new FunctionIdent("any_=", Arrays.<DataType>asList(new ArrayType(DataTypes.INTEGER), DataTypes.INTEGER)),
                        DataTypes.BOOLEAN)
        );
        return anyEqOperator.evaluate(new ObjectInput(value), new ObjectInput(arrayExpr));

    }

    private Boolean anyEqNormalizeSymbol(Object value, Object arrayExpr) {
        AnyEqOperator anyEqOperator = new AnyEqOperator(
                new FunctionInfo(
                        new FunctionIdent("any_=", Arrays.<DataType>asList(new ArrayType(DataTypes.INTEGER), DataTypes.INTEGER)),
                        DataTypes.BOOLEAN)
        );
        Function function = new Function(
                anyEqOperator.info(),
                Arrays.<Symbol>asList(Literal.newLiteral(new ArrayType(DataTypes.INTEGER), arrayExpr),
                        Literal.newLiteral(DataTypes.INTEGER, value))
        );
        return (Boolean)((Literal)anyEqOperator.normalizeSymbol(function)).value();
    }

    @Test
    public void testEvaluate() throws Exception {
        assertTrue(anyEq(new Object[]{1}, 1));
        assertFalse(anyEq(new Object[]{2L}, 1L));
        assertTrue(anyEq(
                new Object[]{
                        ImmutableMap.<String, Object>builder()
                                .put("int", 1)
                                .put("boolean", true)
                                .build()
                },
                ImmutableMap.<String, Object>builder()
                        .put("int", 1)
                        .put("boolean", true)
                        .build()
        ));
        assertFalse(anyEq(
                new Object[]{
                        ImmutableMap.<String, Object>builder()
                                .put("int", 2)
                                .put("boolean", false)
                                .build()
                },
                ImmutableMap.<String, Object>builder()
                        .put("int", 1)
                        .put("boolean", true)
                        .build()
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
        assertNull(anyEqNormalizeSymbol(null, null));
        assertNull(anyEqNormalizeSymbol(42, null));
        assertNull(anyEqNormalizeSymbol(null, new Object[]{1}));
    }

    @Test
    public void testNormalizeSymbol() throws Exception {
        assertTrue(anyEqNormalizeSymbol(42, new Object[]{42}));
        assertTrue(anyEqNormalizeSymbol(42, new Object[]{1, 42, 2}));
        assertFalse(anyEqNormalizeSymbol(42, new Object[]{}));
        assertFalse(anyEqNormalizeSymbol(42, new Object[]{41, 43, -42}));
    }

}
