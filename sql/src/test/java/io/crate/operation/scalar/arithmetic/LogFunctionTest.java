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

import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Functions;
import io.crate.operation.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.nullValue;

public class LogFunctionTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private Functions functions;

    @Before
    public void setUp() throws Exception {
        functions = new ModulesBuilder().add(new ScalarFunctionModule())
                .createInjector().getInstance(Functions.class);
    }

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
                        Arrays.<Symbol>asList(Literal.newLiteral(valueType, value)))
        );
    }

    private Symbol normalizeLog(Number value, DataType valueType, Number base, DataType baseType) {
        LogFunction function = getFunction(LogFunction.NAME, valueType, baseType);
        return function.normalizeSymbol(new Function(function.info(),
                Arrays.<Symbol>asList(Literal.newLiteral(valueType, value), Literal.newLiteral(baseType, base))));
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
        TestingHelpers.assertLiteralSymbol(normalizeLog(10.0, DataTypes.DOUBLE), 1.0);
        TestingHelpers.assertLiteralSymbol(normalizeLog(10f, DataTypes.FLOAT), 1.0);
        TestingHelpers.assertLiteralSymbol(normalizeLog(10L, DataTypes.LONG), 1.0);
        TestingHelpers.assertLiteralSymbol(normalizeLog(10, DataTypes.INTEGER), 1.0);
        TestingHelpers.assertLiteralSymbol(normalizeLog(null, DataTypes.DOUBLE), null, DataTypes.DOUBLE);

        // test ln(x)
        TestingHelpers.assertLiteralSymbol(normalizeLn(1.0, DataTypes.DOUBLE), 0.0);
        TestingHelpers.assertLiteralSymbol(normalizeLn(1f, DataTypes.FLOAT), 0.0);
        TestingHelpers.assertLiteralSymbol(normalizeLn(1L, DataTypes.LONG), 0.0);
        TestingHelpers.assertLiteralSymbol(normalizeLn(1, DataTypes.INTEGER), 0.0);
        TestingHelpers.assertLiteralSymbol(normalizeLn(null, DataTypes.DOUBLE), null, DataTypes.DOUBLE);

        // test log(x, b) ... explicit base
        TestingHelpers.assertLiteralSymbol(normalizeLog(10.0, DataTypes.DOUBLE, 10.0, DataTypes.DOUBLE), 1.0);
        TestingHelpers.assertLiteralSymbol(normalizeLog(10f, DataTypes.FLOAT, 10.0, DataTypes.DOUBLE), 1.0);
        TestingHelpers.assertLiteralSymbol(normalizeLog(10.0, DataTypes.DOUBLE, 10.0f, DataTypes.FLOAT), 1.0);
        TestingHelpers.assertLiteralSymbol(normalizeLog(10f, DataTypes.FLOAT, 10.0f, DataTypes.FLOAT), 1.0);

        TestingHelpers.assertLiteralSymbol(normalizeLog(10L, DataTypes.LONG, 10.0, DataTypes.DOUBLE), 1.0);
        TestingHelpers.assertLiteralSymbol(normalizeLog(10.0, DataTypes.DOUBLE, 10.0f, DataTypes.FLOAT), 1.0);
        TestingHelpers.assertLiteralSymbol(normalizeLog(10f, DataTypes.FLOAT, 10.0f, DataTypes.FLOAT), 1.0);

        TestingHelpers.assertLiteralSymbol(normalizeLog(10L, DataTypes.LONG, 10L, DataTypes.LONG), 1.0);
        TestingHelpers.assertLiteralSymbol(normalizeLog(10, DataTypes.INTEGER, 10L, DataTypes.LONG), 1.0);
        TestingHelpers.assertLiteralSymbol(normalizeLog(10L, DataTypes.LONG, (short) 10, DataTypes.SHORT), 1.0);
        TestingHelpers.assertLiteralSymbol(normalizeLog(10, DataTypes.INTEGER, 10, DataTypes.INTEGER), 1.0);

        TestingHelpers.assertLiteralSymbol(normalizeLog(null, DataTypes.DOUBLE, 10, DataTypes.INTEGER), null, DataTypes.DOUBLE);
        TestingHelpers.assertLiteralSymbol(normalizeLog(10, DataTypes.INTEGER, null, DataTypes.DOUBLE), null, DataTypes.DOUBLE);
        TestingHelpers.assertLiteralSymbol(normalizeLog(null, DataTypes.INTEGER, null, DataTypes.DOUBLE), null, DataTypes.DOUBLE);
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
        Reference dB = TestingHelpers.createReference("dB", DataTypes.DOUBLE);

        LogFunction log10 = getFunction(LogFunction.NAME, DataTypes.DOUBLE);
        Function function = new Function(log10.info(), Arrays.<Symbol>asList(dB));
        Function normalized = (Function) log10.normalizeSymbol(function);
        assertThat(normalized, Matchers.sameInstance(function));

        LogFunction ln = getFunction(LogFunction.LnFunction.NAME, DataTypes.DOUBLE);
        function = new Function(ln.info(), Arrays.<Symbol>asList(dB));
        normalized = (Function) ln.normalizeSymbol(function);
        assertThat(normalized, Matchers.sameInstance(function));

        LogFunction logBase = getFunction(LogFunction.NAME, DataTypes.DOUBLE, DataTypes.LONG);
        function = new Function(logBase.info(), Arrays.<Symbol>asList(dB, Literal.newLiteral(10L)));
        normalized = (Function) logBase.normalizeSymbol(function);
        assertThat(normalized, Matchers.sameInstance(function));

        Reference base = TestingHelpers.createReference("base", DataTypes.INTEGER);
        function = new Function(logBase.info(), Arrays.<Symbol>asList(dB, base));
        normalized = (Function) logBase.normalizeSymbol(function);
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
