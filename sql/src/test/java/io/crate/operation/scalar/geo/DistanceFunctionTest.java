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

import com.google.common.collect.Lists;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.metadata.FunctionIdent;
import io.crate.operation.Input;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.crate.testing.TestingHelpers.createReference;
import static io.crate.testing.TestingHelpers.isLiteral;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class DistanceFunctionTest extends AbstractScalarFunctionsTest {

    private DistanceFunction getFunction(List<DataType> types) {
        return (DistanceFunction)functions.get(
                new FunctionIdent(DistanceFunction.NAME, types));
    }

    private DistanceFunction functionFromArgs(List<? extends Symbol> args) {
        return getFunction(Symbols.extractTypes(args));
    }

    @SuppressWarnings("unchecked")
    private Double evaluate(List<Literal> args) {
        return functionFromArgs(args).evaluate(args.toArray(new Input[args.size()]));
    }

    @SuppressWarnings("unchecked")
    private Symbol normalize(List<? extends Symbol> args) {
        DistanceFunction distanceFunction = functionFromArgs(args);
        return distanceFunction.normalizeSymbol(new Function(distanceFunction.info(), (List<Symbol>)args));
    }

    @Test
    public void testResolveWithTooManyArguments() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("distance takes 2 arguments, not 4");
        functions.get(new FunctionIdent(DistanceFunction.NAME,
                Arrays.<DataType>asList(DataTypes.STRING, DataTypes.STRING, DataTypes.STRING, DataTypes.STRING)));
    }

    @Test
    public void testResolveWithInvalidType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("distance can't handle arguments of type \"long\"");
        functions.get(new FunctionIdent(DistanceFunction.NAME,
                Arrays.<DataType>asList(DataTypes.LONG, DataTypes.GEO_POINT)));
    }

    @Test
    public void testEvaluateWithTwoGeoPointLiterals() throws Exception {
        Double distance = evaluate(Arrays.<Literal>asList(
                Literal.newLiteral(DataTypes.GEO_POINT, new Double[]{10.04, 28.02}),
                Literal.newLiteral(DataTypes.GEO_POINT, DataTypes.GEO_POINT.value("POINT(10.30 29.3)"))));
        assertThat(distance, is(144623.6842773458));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testNormalizeWithStringTypes() throws Exception {
        Symbol symbol = normalize(Arrays.<Symbol>asList(
                Literal.newLiteral("POINT (10 20)"),
                Literal.newLiteral("POINT (11 21)")
        ));
        assertThat(symbol, isLiteral(152462.70754934277));
    }

    @Test
    public void testNormalizeWithDoubleArray() throws Exception {
        DataType type = new ArrayType(DataTypes.DOUBLE);
        Symbol symbol = normalize(Arrays.<Symbol>asList(
                Literal.newLiteral(type, new Double[]{10.0, 20.0}),
                Literal.newLiteral(type, new Double[]{11.0, 21.0})
        ));
        assertThat(symbol, isLiteral(152462.70754934277));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testNormalizeWithInvalidReferences() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot convert foo to a geo point");

        normalize(Arrays.<Symbol>asList(
                createReference("foo", DataTypes.STRING),
                Literal.newLiteral(DataTypes.GEO_POINT, new Double[]{10.04, 28.02})
        ));
    }

    @Test
    public void testNormalizeWithValidRefAndStringLiteral() throws Exception {
        Function symbol = (Function) normalize(Arrays.<Symbol>asList(
                createReference("foo", DataTypes.GEO_POINT),
                Literal.newLiteral("POINT(10 20)")
        ));
        assertThat(symbol.arguments().get(1),
                isLiteral(new Double[]{10.0d, 20.0d}, DataTypes.GEO_POINT));

        // args reversed
        symbol = (Function) normalize(Arrays.<Symbol>asList(
                Literal.newLiteral("POINT(10 20)"),
                createReference("foo", DataTypes.GEO_POINT)
        ));
        assertThat(symbol.arguments().get(1),
                isLiteral(new Double[] { 10.0d, 20.0d }, DataTypes.GEO_POINT));
    }

    @Test
    public void testNormalizeWithValidRefAndGeoPointLiteral() throws Exception {
        Function symbol = (Function) normalize(Arrays.<Symbol>asList(
                createReference("foo", DataTypes.GEO_POINT),
                Literal.newLiteral(DataTypes.GEO_POINT, DataTypes.GEO_POINT.value("POINT (10 20)"))
        ));
        assertThat(symbol.arguments().get(1),
                isLiteral(new Double[]{10.0d, 20.0d}, DataTypes.GEO_POINT));

        // args reversed
        symbol = (Function) normalize(Arrays.<Symbol>asList(
                Literal.newLiteral(DataTypes.GEO_POINT, DataTypes.GEO_POINT.value("POINT (10 20)")),
                createReference("foo", DataTypes.GEO_POINT)
        ));
        assertThat(symbol.arguments().get(1),
                isLiteral(new Double[] { 10.0d, 20.0d }, DataTypes.GEO_POINT));
    }

    @Test
    public void testNormalizeWithValidGeoPointLiterals() throws Exception {
        Literal symbol = (Literal) normalize(Arrays.<Symbol>asList(
                Literal.newLiteral(DataTypes.GEO_POINT, DataTypes.GEO_POINT.value("POINT (10 20)")),
                Literal.newLiteral(DataTypes.GEO_POINT, DataTypes.GEO_POINT.value("POINT (30 40)"))
        ));
        assertThat(symbol.value(), instanceOf(Double.class));

        // args reversed
        symbol = (Literal) normalize(Arrays.<Symbol>asList(
                Literal.newLiteral(DataTypes.GEO_POINT, DataTypes.GEO_POINT.value("POINT (30 40)")),
                Literal.newLiteral(DataTypes.GEO_POINT, DataTypes.GEO_POINT.value("POINT (10 20)"))
        ));
        assertThat(symbol.value(), instanceOf(Double.class));
    }

    @Test
    public void testNormalizeWithTwoValidRefs() throws Exception {
        List<Symbol> args = Arrays.<Symbol>asList(
                createReference("foo", DataTypes.GEO_POINT),
                createReference("foo2", DataTypes.GEO_POINT));
        DistanceFunction distanceFunction = functionFromArgs(args);
        Function functionSymbol = new Function(distanceFunction.info(), args);
        Function normalizedFunction = (Function)distanceFunction.normalizeSymbol(functionSymbol);
        assertThat(functionSymbol, Matchers.sameInstance(normalizedFunction));
    }

    @Test
    public void testWithNullValue() throws Exception {
        List<Literal> args = Arrays.<Literal>asList(
                Literal.newLiteral(DataTypes.GEO_POINT, null),
                Literal.newLiteral(DataTypes.GEO_POINT, DataTypes.GEO_POINT.value("POINT (10 20)"))
        );
        Double distance = evaluate(args);
        assertNull(distance);
        Symbol distanceSymbol = normalize(args);
        assertNull(((Literal) distanceSymbol).value());

        distanceSymbol = normalize(Lists.reverse(args));
        assertNull(((Literal) distanceSymbol).value());
    }
}
