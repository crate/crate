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

import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Functions;
import io.crate.operation.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;

import static io.crate.testing.TestingHelpers.createReference;
import static io.crate.testing.TestingHelpers.isLiteral;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;

public class WithinFunctionTest {

    private Functions functions;

    private WithinFunction functionFromArgs(List<? extends Symbol> args) {
        return getFunction(Symbols.extractTypes(args));
    }

    private WithinFunction getFunction(List<DataType> types) {
        WithinFunction withinFunction = (WithinFunction) functions.get(
                new FunctionIdent(WithinFunction.NAME, types));
        assertNotNull(withinFunction);
        return withinFunction;
    }

    private Boolean evaluate(List<Symbol> symbols) {
        Input[] args = new Input[symbols.size()];
        int idx = 0;
        for (Symbol symbol : symbols) {
            args[idx] = (Input) symbol;
            idx++;
        }
        return functionFromArgs(symbols).evaluate(args);
    }

    private Boolean evaluate(Symbol... symbols) {
        Input[] args = new Input[symbols.length];
        for (int i = 0; i < args.length; i++) {
            args[i] = (Input) symbols[i];
        }
        return functionFromArgs(Arrays.asList(symbols)).evaluate(args);
    }

    private Symbol normalize(Symbol... arguments) {
        return normalize(Arrays.asList(arguments));
    }

    private Symbol normalize(List<Symbol> arguments) {
        WithinFunction withinFunction = functionFromArgs(arguments);
        return withinFunction.normalizeSymbol(new Function(withinFunction.info(), arguments));
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        ModulesBuilder modules = new ModulesBuilder();
        modules.add(new ScalarFunctionModule());
        Injector injector = modules.createInjector();
        functions = injector.getInstance(Functions.class);
    }

    @Test
    public void testEvaluateWithFirstArgNull() throws Exception {
        assertNull(evaluate(Literal.newGeoShape(null), Literal.newGeoShape("POINT (10 10)")));
        assertNull(evaluate(Literal.newGeoShape("POINT (10 10)"), Literal.newGeoShape(null)));
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
        boolean isWithin = evaluate(Arrays.<Symbol>asList(
                Literal.newGeoShape("POLYGON ((10 10, 11 10, 11 11, 10 11, 10 10))"),
                Literal.newGeoShape("POLYGON ((5 5, 20 5, 30 30, 5 30, 5 5))")));
        assertTrue(isWithin);
    }

    @Test
    public void testNormalizeWithReferenceAndLiteral() throws Exception {
        List<Symbol> arguments = Arrays.<Symbol>asList(
                createReference("foo", DataTypes.GEO_POINT),
                Literal.newGeoShape("POLYGON ((5 5, 20 5, 30 30, 5 30, 5 5))"));
        WithinFunction withinFunction = functionFromArgs(arguments);
        Symbol function = new Function(withinFunction.info(), arguments);
        Symbol normalizedSymbol = withinFunction.normalizeSymbol((Function) function);
        assertThat(normalizedSymbol, Matchers.sameInstance(function));
    }

    @Test
    public void testNormalizeWithTwoLiterals() throws Exception {
        Symbol normalized = normalize(Arrays.<Symbol>asList(
                Literal.newGeoShape("POLYGON ((10 10, 11 10, 11 11, 10 11, 10 10))"),
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
        assertThat(normalized, Matchers.instanceOf(Function.class));
        Function function = (Function) normalized;
        Symbol symbol = function.arguments().get(1);
        assertThat(symbol.valueType(), equalTo((DataType) DataTypes.GEO_SHAPE));
    }

    @Test
    public void testNormalizeWithFirstArgAsStringReference() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("\"within\" doesn't support references of type \"string\"");
        normalize(Arrays.<Symbol>asList(
                createReference("location", DataTypes.STRING),
                Literal.newGeoShape("POLYGON ((5 5, 20 5, 30 30, 5 30, 5 5))")));
    }

    @Test
    public void testNormalizeWithSecondArgAsStringReference() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("\"within\" doesn't support references of type \"string\"");
        normalize(Arrays.<Symbol>asList(
                Literal.newGeoShape("POLYGON ((5 5, 20 5, 30 30, 5 30, 5 5))"),
                createReference("location", DataTypes.STRING)));
    }

    @Test
    public void testFirstArgumentWithInvalidType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
                "within doesn't take an argument of type \"long\" as first argument");
        getFunction(Arrays.<DataType>asList(DataTypes.LONG, DataTypes.GEO_POINT));
    }

    @Test
    public void testSecondArgumentWithInvalidType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
                "within doesn't take an argument of type \"long\" as second argument");
        getFunction(Arrays.<DataType>asList(DataTypes.GEO_POINT, DataTypes.LONG));
    }
}