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
import static org.hamcrest.core.IsNull.nullValue;

public class LogFunctionTest extends AbstractScalarFunctionsTest {

    private StmtCtx stmtCtx = new StmtCtx();

    private LogFunction getFunction(String name, DataType value) {
        return (LogFunction) functions.get(new FunctionIdent(name, Arrays.asList(value)));
    }

    private LogFunction getFunction(String name, DataType value, DataType base) {
        return (LogFunction) functions.get(new FunctionIdent(name, Arrays.asList(value, base)));
    }

    private Symbol normalizeLog(Number value, DataType valueType) {
        LogFunction function = getFunction(LogFunction.NAME, valueType);
        return normalize(value, valueType, function);
    }

    private Symbol normalizeLn(Number value, DataType valueType) {
        LogFunction function = getFunction(LogFunction.LnFunction.NAME, valueType);
        return normalize(value, valueType, function);
    }

    private Symbol normalize(Number value, DataType valueType, LogFunction function) {
        return function.normalizeSymbol(new Function(function.info(),
                        Arrays.<Symbol>asList(Literal.newLiteral(valueType, value))), stmtCtx);
    }

    private Symbol normalizeLog(Number value, DataType valueType, Number base, DataType baseType) {
        LogFunction function = getFunction(LogFunction.NAME, valueType, baseType);
        return function.normalizeSymbol(new Function(function.info(),
                Arrays.<Symbol>asList(Literal.newLiteral(valueType, value), Literal.newLiteral(baseType, base))), stmtCtx);
    }

    private Number evaluateLog(Number value, DataType valueType) {
        return getFunction(LogFunction.NAME, valueType).evaluate((Input) Literal.newLiteral(valueType, value));
    }

    private Number evaluateLn(Number value, DataType valueType) {
        return getFunction(LogFunction.LnFunction.NAME, valueType).evaluate((Input) Literal.newLiteral(valueType, value));
    }

    private Number evaluateLog(Number value, DataType valueType, Number base, DataType baseType) {
        return getFunction(LogFunction.NAME, valueType, baseType).evaluate(
                (Input) Literal.newLiteral(valueType, value),
                (Input) Literal.newLiteral(baseType, base)
        );
    }

    @Test
    public void testNormalizeValueSymbol() throws Exception {
        // test log(x) ... implicit base of 10
        assertThat(normalizeLog(10.0, DataTypes.DOUBLE), isLiteral(1.0));
        assertThat(normalizeLog(10f, DataTypes.FLOAT), isLiteral(1.0));
        assertThat(normalizeLog(10L, DataTypes.LONG), isLiteral(1.0));
        assertThat(normalizeLog(10, DataTypes.INTEGER), isLiteral(1.0));
        assertThat(normalizeLog(null, DataTypes.DOUBLE), isLiteral(null, DataTypes.DOUBLE));

        // test ln(x)
        assertThat(normalizeLn(1.0, DataTypes.DOUBLE), isLiteral(0.0));
        assertThat(normalizeLn(1f, DataTypes.FLOAT), isLiteral(0.0));
        assertThat(normalizeLn(1L, DataTypes.LONG), isLiteral(0.0));
        assertThat(normalizeLn(1, DataTypes.INTEGER), isLiteral(0.0));
        assertThat(normalizeLn(null, DataTypes.DOUBLE), isLiteral(null, DataTypes.DOUBLE));

        // test log(x, b) ... explicit base
        assertThat(normalizeLog(10.0, DataTypes.DOUBLE, 10.0, DataTypes.DOUBLE), isLiteral(1.0));
        assertThat(normalizeLog(10f, DataTypes.FLOAT, 10.0, DataTypes.DOUBLE), isLiteral(1.0));
        assertThat(normalizeLog(10.0, DataTypes.DOUBLE, 10.0f, DataTypes.FLOAT), isLiteral(1.0));
        assertThat(normalizeLog(10f, DataTypes.FLOAT, 10.0f, DataTypes.FLOAT), isLiteral(1.0));

        assertThat(normalizeLog(10L, DataTypes.LONG, 10.0, DataTypes.DOUBLE), isLiteral(1.0));
        assertThat(normalizeLog(10.0, DataTypes.DOUBLE, 10.0f, DataTypes.FLOAT), isLiteral(1.0));
        assertThat(normalizeLog(10f, DataTypes.FLOAT, 10.0f, DataTypes.FLOAT), isLiteral(1.0));

        assertThat(normalizeLog(10L, DataTypes.LONG, 10L, DataTypes.LONG), isLiteral(1.0));
        assertThat(normalizeLog(10, DataTypes.INTEGER, 10L, DataTypes.LONG), isLiteral(1.0));
        assertThat(normalizeLog(10L, DataTypes.LONG, (short) 10, DataTypes.SHORT), isLiteral(1.0));
        assertThat(normalizeLog(10, DataTypes.INTEGER, 10, DataTypes.INTEGER), isLiteral(1.0));

        assertThat(normalizeLog(null, DataTypes.DOUBLE, 10, DataTypes.INTEGER), isLiteral(null, DataTypes.DOUBLE));
        assertThat(normalizeLog(10, DataTypes.INTEGER, null, DataTypes.DOUBLE), isLiteral(null, DataTypes.DOUBLE));
        assertThat(normalizeLog(null, DataTypes.INTEGER, null, DataTypes.DOUBLE), isLiteral(null, DataTypes.DOUBLE));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNormalizeLogZero() throws Exception {
        // -Infinity
        normalizeLog(0.0, DataTypes.DOUBLE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNormalizeLogNegative() throws Exception {
        // NaN
        normalizeLog(-10.0, DataTypes.DOUBLE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNormalizeLnZero() throws Exception {
        // -Infinity
        normalizeLn(0.0, DataTypes.DOUBLE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNormalizeLnNegative() throws Exception {
        // NaN
        normalizeLn(-10.0, DataTypes.DOUBLE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNormalizeLogDivisionByZero() throws Exception {
        // division by zero
        normalizeLog(10.0, DataTypes.DOUBLE, 1.0, DataTypes.DOUBLE);
    }

    @Test
    public void testNormalizeString() throws Exception {
        assertThat(getFunction(LogFunction.NAME, DataTypes.STRING), Matchers.nullValue());
        assertThat(getFunction(LogFunction.NAME, DataTypes.STRING, DataTypes.STRING), Matchers.nullValue());
        assertThat(getFunction(LogFunction.LnFunction.NAME, DataTypes.STRING), Matchers.nullValue());
    }

    @Test
    public void testNormalizeReference() throws Exception {
        Reference dB = createReference("dB", DataTypes.DOUBLE);

        LogFunction log10 = getFunction(LogFunction.NAME, DataTypes.DOUBLE);
        Function function = new Function(log10.info(), Arrays.<Symbol>asList(dB));
        Function normalized = (Function) log10.normalizeSymbol(function, stmtCtx);
        assertThat(normalized, Matchers.sameInstance(function));

        LogFunction ln = getFunction(LogFunction.LnFunction.NAME, DataTypes.DOUBLE);
        function = new Function(ln.info(), Arrays.<Symbol>asList(dB));
        normalized = (Function) ln.normalizeSymbol(function, stmtCtx);
        assertThat(normalized, Matchers.sameInstance(function));

        LogFunction logBase = getFunction(LogFunction.NAME, DataTypes.DOUBLE, DataTypes.LONG);
        function = new Function(logBase.info(), Arrays.<Symbol>asList(dB, Literal.newLiteral(10L)));
        normalized = (Function) logBase.normalizeSymbol(function, stmtCtx);
        assertThat(normalized, Matchers.sameInstance(function));

        Reference base = createReference("base", DataTypes.INTEGER);
        function = new Function(logBase.info(), Arrays.<Symbol>asList(dB, base));
        normalized = (Function) logBase.normalizeSymbol(function, stmtCtx);
        assertThat(normalized, Matchers.sameInstance(function));
    }

    @Test
    public void testEvaluateLog10() throws Exception {
        assertThat((Double) evaluateLog(100, DataTypes.INTEGER), is(2.0));
        assertThat((Double) evaluateLog(100.0, DataTypes.DOUBLE), is(2.0));
        assertThat((Double) evaluateLog(100f, DataTypes.FLOAT), is(2.0));
        assertThat((Double) evaluateLog(100L, DataTypes.LONG), is(2.0));
        assertThat((Double) evaluateLog((short) 100, DataTypes.SHORT), is(2.0));

        assertThat(evaluateLog(null, DataTypes.DOUBLE), nullValue());
    }

    @Test
    public void testEvaluateLogBase() throws Exception {
        assertThat((Double) evaluateLog((short) 10, DataTypes.SHORT, 100, DataTypes.INTEGER), is(0.5));
        assertThat((Double) evaluateLog(10f, DataTypes.FLOAT, 100.0, DataTypes.DOUBLE), is(0.5));
        assertThat((Double) evaluateLog(10L, DataTypes.LONG, 100f, DataTypes.FLOAT), is(0.5));
        assertThat((Double) evaluateLog(10, DataTypes.INTEGER, 100L, DataTypes.LONG), is(0.5));
        assertThat((Double) evaluateLog(10.0f, DataTypes.FLOAT, (short) 100, DataTypes.SHORT), is(0.5));

        assertThat(evaluateLog(null, DataTypes.DOUBLE, (short) 10, DataTypes.SHORT), nullValue());
        assertThat(evaluateLog(10.0, DataTypes.DOUBLE, null, DataTypes.DOUBLE), nullValue());
        assertThat(evaluateLog(null, DataTypes.DOUBLE, null, DataTypes.DOUBLE), nullValue());
    }

    @Test
    public void testEvaluateLn() throws Exception {
        assertThat((Double) evaluateLn(1, DataTypes.INTEGER), is(0.0));
        assertThat((Double) evaluateLn(1.0, DataTypes.DOUBLE), is(0.0));
        assertThat((Double) evaluateLn(1f, DataTypes.FLOAT), is(0.0));
        assertThat((Double) evaluateLn(1L, DataTypes.LONG), is(0.0));
        assertThat((Double) evaluateLn((short) 1, DataTypes.SHORT), is(0.0));

        assertThat(evaluateLn(null, DataTypes.DOUBLE), nullValue());
    }

}
