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

package io.crate.operation.scalar.arithmetic;

import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.StmtCtx;
import io.crate.operation.Input;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;

import static io.crate.testing.TestingHelpers.createReference;
import static io.crate.testing.TestingHelpers.isLiteral;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class AbsFunctionTest extends AbstractScalarFunctionsTest {

    private AbsFunction getFunction(DataType type) {
        return (AbsFunction) functions.get(new FunctionIdent(AbsFunction.NAME, Arrays.asList(type)));
    }

    private Number evaluate(Number number, DataType type) {
        return getFunction(type).evaluate((Input) Literal.newLiteral(type, number));
    }

    private Symbol normalize(Number number, DataType type) {
        AbsFunction function = getFunction(type);
        return function.normalizeSymbol(new Function(function.info(),
                Arrays.<Symbol>asList(Literal.newLiteral(type, number))), new StmtCtx());
    }

    @Test
    public void testEvaluate() throws Exception {
        Number posVal;
        Number negVal;

        for (DataType type : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            posVal = (Number)type.value(1);
            assertThat(evaluate((Number)type.value(posVal), type), is(type.value(1)));

            assertThat(evaluate((Number)type.value(0), type), is(type.value(0)));

            negVal = (Number)type.value(-1);
            assertThat(evaluate((Number)type.value(negVal), type), is(type.value(1)));
        }
    }

    @Test
    public void testEvaluateNull() throws Exception {
        for (DataType type : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            assertThat(evaluate(null, type), nullValue());
        }

    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongType() throws Exception {
        getFunction(DataTypes.STRING);
    }

    @Test
    public void testNormalizeValueSymbol() throws Exception {
        Number posVal;
        Number zeroVal;
        Number negVal;
        for (DataType type : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            posVal = (Number)type.value(1);
            assertThat(normalize(posVal, type), isLiteral(type.value(1), type));

            zeroVal = (Number)type.value(0);
            assertThat(normalize(zeroVal, type), isLiteral(type.value(0), type));

            negVal = (Number)type.value(-1);
            assertThat(normalize(negVal, type), isLiteral(type.value(1), type));
        }
    }

    @Test
    public void testNormalizeNull() throws Exception {
        for (DataType type : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            assertThat(normalize(null, type), isLiteral(null, type));
        }
    }

    @Test
    public void testNormalizeReference() throws Exception {
        Reference height = createReference("height", DataTypes.DOUBLE);
        AbsFunction abs = getFunction(DataTypes.DOUBLE);
        Function function = new Function(abs.info(), Arrays.<Symbol>asList(height));
        Function normalized = (Function) abs.normalizeSymbol(function, new StmtCtx());
        assertThat(normalized, Matchers.sameInstance(function));
    }
}
