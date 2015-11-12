/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.scalar.geo;

import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.ConversionException;
import io.crate.geo.GeoJSONUtils;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.types.DataTypes;
import org.junit.Test;

import static io.crate.testing.TestingHelpers.*;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class IntersectsFunctionTest extends AbstractScalarFunctionsTest {

    public static final String FUNCTION_NAME = IntersectsFunction.NAME;

    @Test
    public void testNormalizeFromStringLiterals() throws Exception {
        Symbol normalized = normalize(FUNCTION_NAME,
                Literal.newLiteral("LINESTRING (0 0, 10 10)"),
                Literal.newLiteral("LINESTRING (0 2, 0 -2)"));
        assertThat(normalized, isLiteral(Boolean.TRUE));
    }

    @Test
    public void testNormalizeFromGeoShapeLiterals() throws Exception {
        Symbol normalized = normalize(FUNCTION_NAME,
                Literal.newGeoShape("POLYGON ((0 0, 10 10, 10 0, 0 0), (5 1, 7 1, 7 2, 5 2, 5 1))"),
                Literal.newGeoShape("LINESTRING (0 2, 0 -2)"));
        assertThat(normalized, isLiteral(Boolean.TRUE));
    }

    @Test
    public void testNormalizeFromMixedLiterals() throws Exception {
        Symbol normalized = normalize(FUNCTION_NAME,
                Literal.newLiteral(DataTypes.OBJECT, jsonMap("{type:\"LineString\", coordinates:[[0, 0], [10, 10]]}")),
                Literal.newLiteral("LINESTRING (0 2, 0 -2)"));
        assertThat(normalized, isLiteral(Boolean.TRUE));
    }

    @Test
    public void testNormalizeFromInvalidLiteral() throws Exception {
        expectedException.expect(ConversionException.class);
        expectedException.expectMessage("Cannot convert \"{coordinates=[0, 0], type=LineString}\" to geo_shape");
        Symbol normalized = normalize(FUNCTION_NAME,
                Literal.newLiteral(DataTypes.OBJECT, jsonMap("{type:\"LineString\", coordinates:[0, 0]}")),
                Literal.newLiteral("LINESTRING (0 2, 0 -2)"));
        assertThat(normalized, isLiteral(Boolean.TRUE));
    }

    @Test
    public void testNormalizeWithReference() throws Exception {
        Symbol normalized = normalize(FUNCTION_NAME,
                Literal.newGeoShape("POLYGON ((0 0, 10 10, 10 0, 0 0), (5 1, 7 1, 7 2, 5 2, 5 1))"),
                sqlExpressions.asSymbol("shape"));
        assertThat(normalized, isFunction(FUNCTION_NAME));
    }

    @Test
    public void testNormalizeWithNull() throws Exception {
        Symbol normalized = normalize(FUNCTION_NAME,
                Literal.newLiteral(DataTypes.GEO_SHAPE, null),
                sqlExpressions.asSymbol("shape"));
        assertThat(normalized, isLiteral(null));

    }

    @Test
    public void testResolveInvalidTypes() throws Exception {
        assertThat(getFunction(FUNCTION_NAME, DataTypes.INTEGER, DataTypes.GEO_SHAPE), is(nullValue()));
        assertThat(getFunction(FUNCTION_NAME, DataTypes.STRING, DataTypes.BOOLEAN), is(nullValue()));
    }

    @Test
    public void testEvaluateFromString() throws Exception {
        IntersectsFunction stringFn = getFunction(FUNCTION_NAME, DataTypes.STRING, DataTypes.STRING);
        Object value = stringFn.evaluate(
                Literal.newLiteral(DataTypes.STRING, "POINT (0 0)"),
                Literal.newLiteral(DataTypes.STRING, "POLYGON ((1 1, 1 -1, -1 -1, -1 1, 1 1))"));
        assertThat(value, is((Object)Boolean.TRUE));
    }

    @Test
    public void testEvaluateMixed() throws Exception {
        IntersectsFunction stringFn = getFunction(FUNCTION_NAME, DataTypes.STRING, DataTypes.GEO_SHAPE); // actual types don't matter here
        Object value = stringFn.evaluate(
                Literal.newLiteral(DataTypes.STRING, "POINT (100 0)"),
                Literal.newLiteral(DataTypes.GEO_SHAPE, GeoJSONUtils.wkt2Map("POLYGON ((1 1, 1 -1, -1 -1, -1 1, 1 1))")));
        assertThat(value, is((Object)Boolean.FALSE));
    }

    @Test
    public void testEvaluateLowercaseGeoJSON() throws Exception {
        IntersectsFunction stringFn = getFunction(FUNCTION_NAME, DataTypes.STRING, DataTypes.GEO_SHAPE); // actual types don't matter here
        Object value = stringFn.evaluate(
                Literal.newLiteral(DataTypes.STRING, "POINT (100 0)"),
                Literal.newLiteral(DataTypes.GEO_SHAPE, jsonMap("{type:\"linestring\", coordinates:[[0, 0], [10, 10]]}")));
        assertThat(value, is((Object)Boolean.FALSE));
    }

    @Test
    public void testExactIntersect() throws Exception {
        // validate how exact the intersection detection is
        IntersectsFunction stringFn = getFunction(FUNCTION_NAME, DataTypes.STRING, DataTypes.GEO_SHAPE); // actual types don't matter here
        Object value = stringFn.evaluate(
                Literal.newLiteral(DataTypes.STRING, "POINT (100.00000000000001 0.0)"),
                Literal.newLiteral(DataTypes.GEO_SHAPE, jsonMap("{type:\"linestring\", coordinates:[[100.00000000000001, 0.0], [10, 10]]}")));
        assertThat(value, is((Object)Boolean.TRUE));

        Object value2 = stringFn.evaluate(
                Literal.newLiteral(DataTypes.STRING, "POINT (100.00000000000001 0.0)"),
                Literal.newLiteral(DataTypes.GEO_SHAPE, jsonMap("{type:\"linestring\", coordinates:[[100.00000000000003, 0.0], [10, 10]]}")));
        assertThat(value2, is((Object)Boolean.FALSE));

    }
}
