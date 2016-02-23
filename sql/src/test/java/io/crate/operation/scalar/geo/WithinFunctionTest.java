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

package io.crate.operation.scalar.geo;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolType;
import io.crate.metadata.Functions;
import io.crate.operation.Input;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static io.crate.testing.TestingHelpers.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;

public class WithinFunctionTest extends AbstractScalarFunctionsTest {

    public static final String FNAME = WithinFunction.NAME;

    private Boolean evaluate(List<Symbol> symbols) {
        Input[] args = new Input[symbols.size()];
        int idx = 0;
        for (Symbol symbol : symbols) {
            args[idx] = (Input) symbol;
            idx++;
        }
        WithinFunction withinFunction = getFunctionFromArgs(FNAME, symbols);
        assertThat(String.format(Locale.ENGLISH, "within function for %s not found", symbols), withinFunction, not(nullValue()));
        return withinFunction.evaluate(args);
    }

    private Boolean evaluate(Symbol... symbols) {
        Input[] args = new Input[symbols.length];
        for (int i = 0; i < args.length; i++) {
            args[i] = (Input) symbols[i];
        }
        WithinFunction fn = getFunctionFromArgs(FNAME, Arrays.asList(symbols));
        return fn.evaluate(args);
    }

    private Symbol normalize(Symbol... arguments) {
        return normalize(Arrays.asList(arguments));
    }

    private Symbol normalize(List<Symbol> arguments) {
        WithinFunction withinFunction = getFunctionFromArgs(FNAME, arguments);
        return withinFunction.normalizeSymbol(new Function(withinFunction.info(), arguments));
    }

    @Test
    public void testEvaluateWithNullArgs() throws Exception {
        assertNull(evaluate(Literal.newGeoPoint(null), Literal.newGeoShape("POINT (10 10)")));
        assertNull(evaluate(Literal.newGeoPoint("POINT (10 10)"), Literal.newGeoShape(null)));
    }

    @Test
    public void testEvaluatePointLiteralWithinPolygonLiteral() throws Exception {
        boolean isWithin = evaluate(Arrays.<Symbol>asList(
                Literal.newLiteral(DataTypes.GEO_POINT, DataTypes.GEO_POINT.value("POINT (10 10)")),
                Literal.newLiteral(DataTypes.GEO_SHAPE, DataTypes.GEO_SHAPE.value(
                        "POLYGON ((5 5, 20 5, 30 30, 5 30, 5 5))"))));
        assertTrue(isWithin);
    }

    @Test
    public void testEvaluateShapeLiteralWithinShapeLiteral() throws Exception {
        assertThat(getFunction(FNAME, Arrays.<DataType>asList(DataTypes.GEO_SHAPE, DataTypes.GEO_SHAPE)), is(nullValue()));
    }

    @Test
    public void testNormalizeWithReferenceAndLiteral() throws Exception {
        Symbol normalizedSymbol = normalize(FNAME, createReference("foo", DataTypes.GEO_POINT),
                Literal.newGeoShape("POLYGON ((5 5, 20 5, 30 30, 5 30, 5 5))"));
        assertThat(normalizedSymbol, isFunction(FNAME));
    }

    @Test
    public void testNormalizeNull() throws Exception {
        Symbol normalizedSymbol = normalize(FNAME,
                Literal.newLiteral(DataTypes.GEO_POINT, null),
                createReference("foo", DataTypes.GEO_SHAPE));
        assertThat(normalizedSymbol, isLiteral(null));
    }

    @Test
    public void testNormalizeWithTwoLiterals() throws Exception {
        Symbol normalized = normalize(Arrays.<Symbol>asList(
                Literal.newGeoPoint("POINT (10 10)"),
                Literal.newGeoShape("POLYGON ((5 5, 20 5, 30 30, 5 30, 5 5))")));
        assertThat(normalized, isLiteral(true));
    }

    @Test
    public void testNormalizeWithTwoStringLiterals() throws Exception {
        Symbol normalized = normalize(
                Literal.newLiteral("POINT (10 10)"),
                Literal.newLiteral("POLYGON ((5 5, 20 5, 30 30, 5 30, 5 5))"));
        assertThat(normalized, isLiteral(true));
    }

    @Test
    public void testNormalizeWithStringLiteralAndReference() throws Exception {
        Symbol normalized = normalize(
                createReference("point", DataTypes.GEO_POINT),
                Literal.newLiteral("POLYGON ((5 5, 20 5, 30 30, 5 30, 5 5))"));
        assertThat(normalized, instanceOf(Function.class));
        Function function = (Function) normalized;
        Symbol symbol = function.arguments().get(1);
        assertThat(symbol.valueType(), equalTo((DataType) DataTypes.GEO_SHAPE));
    }

    @Test
    public void testNormalizeWithFirstArgAsStringReference() throws Exception {
        Symbol normalized = normalize(Arrays.<Symbol>asList(
                createReference("location", DataTypes.STRING),
                Literal.newGeoShape("POLYGON ((5 5, 20 5, 30 30, 5 30, 5 5))")));
        assertThat(normalized.symbolType(), is(SymbolType.FUNCTION));
    }

    @Test
    public void testNormalizeWithSecondArgAsStringReference() throws Exception {
        Symbol normalized = normalize(Arrays.asList(
                Literal.newLiteral(DataTypes.GEO_POINT, new Double[] {0.0d, 0.0d}),
                createReference("location", DataTypes.STRING)));
        assertThat(normalized.symbolType(), is(SymbolType.FUNCTION));
        assertThat(((Function)normalized).info().ident().name(), is(WithinFunction.NAME));
    }

    @Test
    public void testFirstArgumentWithInvalidType() throws Exception {
        assertThat(getFunction(FNAME, DataTypes.LONG, DataTypes.GEO_POINT), is(nullValue()));
    }

    @Test
    public void testSecondArgumentWithInvalidType() throws Exception {
        assertThat(getFunction(FNAME, DataTypes.GEO_POINT, DataTypes.LONG), is(nullValue()));
    }

    @Test
    public void testNormalizeFromObject() throws Exception {
        Symbol normalized = normalize(
                Literal.newLiteral("POINT (1.0 0.0)"),
                Literal.newLiteral(ImmutableMap.<String, Object>of("type", "Point", "coordinates", new double[]{0.0, 1.0})));
        assertThat(normalized.symbolType(), is(SymbolType.LITERAL));
        assertThat(((Literal)normalized).value(), is((Object)Boolean.FALSE));
    }
}