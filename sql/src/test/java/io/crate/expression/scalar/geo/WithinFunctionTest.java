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

package io.crate.expression.scalar.geo;

import com.google.common.collect.ImmutableMap;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.expression.scalar.AbstractScalarFunctionsTest;
import io.crate.types.DataTypes;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.TestingHelpers.createReference;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;

public class WithinFunctionTest extends AbstractScalarFunctionsTest {

    private static final String FNAME = WithinFunction.NAME;

    @Test
    public void testEvaluateWithNullArgs() throws Exception {
        assertEvaluate("within(geopoint, geoshape)", null, Literal.newGeoPoint(null), Literal.newGeoShape("POINT (10 10)"));
        assertEvaluate("within(geopoint, geoshape)", null, Literal.newGeoPoint("POINT (10 10)"), Literal.newGeoShape(null));
    }

    @Test
    public void testEvaluatePointLiteralWithinPolygonLiteral() {
        assertEvaluate("within(geopoint, geoshape)", true,
            Literal.of(DataTypes.GEO_SHAPE, DataTypes.GEO_SHAPE.value("POINT (10 10)")),
            Literal.of(DataTypes.GEO_SHAPE, DataTypes.GEO_SHAPE.value("POLYGON ((5 5, 20 5, 30 30, 5 30, 5 5))"))
        );
    }

    @Test
    public void testEvaluateShapeWithinShape() {
        assertEvaluate("within(geoshape, geoshape)", true,
            Literal.of(DataTypes.GEO_SHAPE, DataTypes.GEO_SHAPE.value("LINESTRING (8 15, 13 24)")),
            Literal.of(DataTypes.GEO_SHAPE, DataTypes.GEO_SHAPE.value("POLYGON ((5 5, 20 5, 30 30, 5 30, 5 5))"))
        );
    }

    @Test
    public void testEvaluateShapeIsNotWithinShape() {
        assertEvaluate("within(geoshape, geoshape)", false,
            Literal.of(DataTypes.GEO_SHAPE, DataTypes.GEO_SHAPE.value("LINESTRING (8 15, 40 74)")),
            Literal.of(DataTypes.GEO_SHAPE, DataTypes.GEO_SHAPE.value("POLYGON ((5 5, 20 5, 30 30, 5 30, 5 5))"))
        );
    }

    @Test
    public void testEvaluateObjectWithinShape() {
        assertEvaluate("within(geopoint, geoshape)", true,
            Literal.of(ImmutableMap.<String, Object>of("type", "Point", "coordinates", new double[]{10.0, 10.0})),
            Literal.of(DataTypes.GEO_SHAPE, DataTypes.GEO_SHAPE.value("POLYGON ((5 5, 20 5, 30 30, 5 30, 5 5))"))
        );
    }

    @Test
    public void testNormalizeWithReferenceAndLiteral() throws Exception {
        Symbol normalizedSymbol = normalize(FNAME, createReference("foo", DataTypes.GEO_POINT),
            Literal.newGeoShape("POLYGON ((5 5, 20 5, 30 30, 5 30, 5 5))"));
        assertThat(normalizedSymbol, isFunction(FNAME));
    }

    @Test
    public void testNormalizeWithTwoStringLiterals() throws Exception {
        assertNormalize("within('POINT (10 10)', 'POLYGON ((5 5, 20 5, 30 30, 5 30, 5 5))')", isLiteral(true));
    }

    @Test
    public void testNormalizeWithStringLiteralAndReference() throws Exception {
        Symbol normalized = normalize(FNAME,
            createReference("point", DataTypes.GEO_POINT),
            Literal.of("POLYGON ((5 5, 20 5, 30 30, 5 30, 5 5))"));
        assertThat(normalized, instanceOf(Function.class));
        Function function = (Function) normalized;
        Symbol symbol = function.arguments().get(1);
        assertThat(symbol.valueType(), equalTo(DataTypes.GEO_SHAPE));
    }

    @Test
    public void testNormalizeWithFirstArgAsStringReference() throws Exception {
        Symbol normalized = normalize(FNAME,
            createReference("location", DataTypes.STRING),
            Literal.newGeoShape("POLYGON ((5 5, 20 5, 30 30, 5 30, 5 5))"));
        assertThat(normalized.symbolType(), is(SymbolType.FUNCTION));
    }

    @Test
    public void testNormalizeWithSecondArgAsStringReference() throws Exception {
        Symbol normalized = normalize(FNAME,
            Literal.of(DataTypes.GEO_POINT, new Double[]{0.0d, 0.0d}),
            createReference("location", DataTypes.STRING));
        assertThat(normalized.symbolType(), is(SymbolType.FUNCTION));
        assertThat(((Function) normalized).info().ident().name(), is(WithinFunction.NAME));
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
        Symbol normalized = normalize(FNAME,
            Literal.of("POINT (1.0 0.0)"),
            Literal.of(ImmutableMap.<String, Object>of("type", "Point", "coordinates", new double[]{0.0, 1.0})));
        assertThat(normalized.symbolType(), is(SymbolType.LITERAL));
        assertThat(((Literal) normalized).value(), is(Boolean.FALSE));
    }
}
