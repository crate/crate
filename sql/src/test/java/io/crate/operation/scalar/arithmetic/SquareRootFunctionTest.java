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
import java.util.List;

import static io.crate.testing.TestingHelpers.createReference;
import static io.crate.testing.TestingHelpers.isLiteral;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;

public class SquareRootFunctionTest extends AbstractScalarFunctionsTest {

    private Number evaluate(Number number, DataType dataType) {
        List<DataType> dataTypes = Arrays.asList(dataType);
        Input input = (Input) Literal.newLiteral(dataType, number);
        return getFunction(dataTypes).evaluate(input);
    }

    private Double evaluate(Double number) {
        return evaluate(number, DataTypes.DOUBLE).doubleValue();
    }

    private Double evaluate(Float number) {
        return evaluate(number, DataTypes.FLOAT).doubleValue();
    }

    private Double evaluate(Integer number) {
        return evaluate(number, DataTypes.INTEGER).doubleValue();
    }

    private Double evaluate(Long number) {
        return evaluate(number, DataTypes.LONG).doubleValue();
    }

    private SquareRootFunction getFunction(List<DataType> dataTypes) {
        return (SquareRootFunction)functions.get(new FunctionIdent(SquareRootFunction.NAME, dataTypes));
    }

    private final StmtCtx stmtCtx = new StmtCtx();

    private Symbol normalize(Number number, DataType type) {
        SquareRootFunction function = getFunction(Arrays.asList(type));
        return function.normalizeSymbol(new Function(function.info(),
                Arrays.<Symbol>asList(Literal.newLiteral(type, number))), stmtCtx);
    }

    @Test
    public void testEvaluateOnFloat() throws Exception {
        assertThat(evaluate(25.f), is(5.d));
        assertThat(evaluate(0.f), is(0.d));
        assertThat(evaluate(null, DataTypes.FLOAT), nullValue());
    }

    @Test
    public void testEvaluateOnDouble() throws Exception {
        assertThat(evaluate(25.d), is(5.d));
        assertThat(evaluate(null, DataTypes.DOUBLE), nullValue());
    }

    @Test
    public void testEvaluateOnInteger() throws Exception {
        assertThat(evaluate(25), is(5.d));
        assertThat(evaluate(null, DataTypes.INTEGER), nullValue());
    }

    @Test
    public void testEvaluateOnLong() throws Exception {
        assertThat(evaluate(25L), is(5.d));
        assertThat(evaluate(null, DataTypes.LONG), nullValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSmallerThanZero() throws Exception {
        evaluate(-25.0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidType() throws Exception {
        functions.get(new FunctionIdent(SquareRootFunction.NAME,Arrays.<DataType>asList(DataTypes.STRING) ));
    }

    @Test
    public void testNormalizeValueSymbol() throws Exception {
        assertThat(normalize(25.0, DataTypes.DOUBLE), isLiteral(5.d));
        assertThat(normalize(25.f, DataTypes.FLOAT), isLiteral(5.d));
        assertThat(normalize(25, DataTypes.INTEGER), isLiteral(5.d));
        assertThat(normalize(25L, DataTypes.LONG), isLiteral(5.d));
    }

    @Test
    public void testNormalizeReference() throws Exception {
        Reference height = createReference("height", DataTypes.DOUBLE);
        SquareRootFunction sqrt = getFunction(Arrays.<DataType>asList(DataTypes.DOUBLE));
        Function function = new Function(sqrt.info(), Arrays.<Symbol>asList(height));
        Function normalized = (Function) sqrt.normalizeSymbol(function, stmtCtx);
        assertThat(normalized, Matchers.sameInstance(function));
    }
}
