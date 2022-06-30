/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;
import io.crate.geo.GeoJSONUtils;
import io.crate.types.DataTypes;
import org.junit.Test;
import org.locationtech.spatial4j.shape.Shape;

import java.util.Map;

import static io.crate.expression.scalar.geo.AreaFunction.getArea;
import static io.crate.testing.SymbolMatchers.isLiteral;

/**
 * Tests for {@link AreaFunction}.
 */
public class AreaFunctionTest extends ScalarTestCase {

    @Test
    public void testEvaluateWithRectangularGeoShapeLiteral() throws Exception {
        assertEvaluate("area(geoShape)", 20.996801695711337,
                       Literal.of(DataTypes.GEO_SHAPE,
                                  DataTypes.GEO_SHAPE.implicitCast("POLYGON ((-2 -1, -2 2, 5 2, 5 -1, -2 -1))")));
    }

    @Test
    public void testWithMapShape() throws Exception {

        String wkt = "POLYGON ((-2 -1, -2 2, 5 2, 5 -1, -2 -1))";
        Shape shape = GeoJSONUtils.wkt2Shape(wkt);
        Map<String, Object> map = GeoJSONUtils.shape2Map(shape);

        assertEquals(20.996801695711337, getArea(map), 0);
    }

    @Test
    public void testEvaluateRoundWithRectangularGeoShapeLiteral() throws Exception {
        assertEvaluate("round(area(geoShape))", 21L,
                       Literal.of(DataTypes.GEO_SHAPE,
                                  DataTypes.GEO_SHAPE.implicitCast("POLYGON ((-2 -1, -2 2, 5 2, 5 -1, -2 -1))")));
    }

    @Test
    public void testWithNullValue() throws Exception {
        assertEvaluate("area(geoShape)", null,
                       Literal.of(DataTypes.GEO_SHAPE, null));
    }

    @Test
    public void testNormalizeWithStringTypes() throws Exception {
        assertNormalize("area('POLYGON ((5 5, 10 5, 10 10, 5 10, 5 5))')", isLiteral(24.7782574034212), false);
    }

    @Test
    public void testWithTooManyArguments() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(
            "Unknown function: area(doc.users.geoshape, 'foo'), no overload found for matching argument types: (geo_shape, text). Possible candidates: area(geo_shape):double precision");
        assertNormalize("area(geoShape, 'foo')", null);
    }

    @Test
    public void testResolveWithInvalidType() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(
            "Unknown function: area(1), no overload found for matching argument types: (integer). Possible candidates: area(geo_shape):double precision");
        assertEvaluate("area(1)", null);
    }

    @Test
    public void testWithInvalidStringReferences() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot cast `'POLYGON (foo)'` of type `text` to type `geo_shape`");
        assertEvaluate("area('POLYGON (foo)')", null);
    }
}
