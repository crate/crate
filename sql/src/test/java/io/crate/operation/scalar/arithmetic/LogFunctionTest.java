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

import io.crate.action.sql.SessionContext;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.Input;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;

import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.TestingHelpers.createReference;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.nullValue;

public class LogFunctionTest extends AbstractScalarFunctionsTest {

    private TransactionContext transactionContext = new TransactionContext(SessionContext.SYSTEM_SESSION);

    private LogFunction getFunction(String name, DataType value) {
        return (LogFunction) getFunction(name, Arrays.asList(value));
    }

    private LogFunction getFunction(String name, DataType value, DataType base) {
        return (LogFunction) getFunction(name, Arrays.asList(value, base));
    }

    private Symbol normalizeLog(Number value, DataType valueType) {
        return normalize(LogFunction.NAME, value, valueType);
    }

    private Symbol normalizeLn(Number value, DataType valueType) {
        return normalize(LogFunction.LnFunction.NAME, value, valueType);
    }

    private Symbol normalizeLog(Number value, DataType valueType, Number base, DataType baseType) {
        return normalize(LogFunction.NAME, Literal.of(valueType, value), Literal.of(baseType, base));
    }

    private Number evaluateLog(Number value, DataType valueType) {
        return getFunction(LogFunction.NAME, valueType).evaluate((Input) Literal.of(valueType, value));
    }

    private Number evaluateLn(Number value, DataType valueType) {
        return getFunction(LogFunction.LnFunction.NAME, valueType).evaluate((Input) Literal.of(valueType, value));
    }

    private Number evaluateLog(Number value, DataType valueType, Number base, DataType baseType) {
        return getFunction(LogFunction.NAME, valueType, baseType).evaluate(
            (Input) Literal.of(valueType, value),
            (Input) Literal.of(baseType, base)
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
    public void testLogZero() throws Exception {
        // -Infinity
        assertEvaluate("log(0.0)", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLogNegative() throws Exception {
        // NaN
        assertEvaluate("log(-10.0)", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLnZero() throws Exception {
        // -Infinity
        assertEvaluate("ln(0.0)", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLnNegative() throws Exception {
        // NaN
        assertEvaluate("ln(-10.0)", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLogDivisionByZero() throws Exception {
        // division by zero
        assertEvaluate("log(10.0, 1.0)", null);
    }

    @Test
    public void testNormalizeString() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("unknown function: log(string)");
        assertNormalize("log('foo')", Matchers.nullValue());
    }

    @Test
    public void testNormalizeReference() throws Exception {
        Reference dB = createReference("dB", DataTypes.DOUBLE);

        LogFunction log10 = getFunction(LogFunction.NAME, DataTypes.DOUBLE);
        Function function = new Function(log10.info(), Arrays.<Symbol>asList(dB));
        Function normalized = (Function) log10.normalizeSymbol(function, transactionContext);
        assertThat(normalized, Matchers.sameInstance(function));

        LogFunction ln = getFunction(LogFunction.LnFunction.NAME, DataTypes.DOUBLE);
        function = new Function(ln.info(), Arrays.<Symbol>asList(dB));
        normalized = (Function) ln.normalizeSymbol(function, transactionContext);
        assertThat(normalized, Matchers.sameInstance(function));

        LogFunction logBase = getFunction(LogFunction.NAME, DataTypes.DOUBLE, DataTypes.LONG);
        function = new Function(logBase.info(), Arrays.<Symbol>asList(dB, Literal.of(10L)));
        normalized = (Function) logBase.normalizeSymbol(function, transactionContext);
        assertThat(normalized, Matchers.sameInstance(function));

        Reference base = createReference("base", DataTypes.INTEGER);
        function = new Function(logBase.info(), Arrays.<Symbol>asList(dB, base));
        normalized = (Function) logBase.normalizeSymbol(function, transactionContext);
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
