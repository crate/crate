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

import static io.crate.testing.Asserts.isFunction;
import static io.crate.testing.Asserts.isLiteral;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;
import io.crate.types.DataTypes;

public class WithinFunctionTest extends ScalarTestCase {

    private static final String FNAME = WithinFunction.NAME;

    @Test
    public void testEvaluateWithNullArgs() throws Exception {
        assertEvaluateNull(
            "within(geopoint, geoshape)",
            Literal.of(DataTypes.GEO_POINT, null),
            Literal.of(
                DataTypes.GEO_POINT,
                DataTypes.GEO_POINT.implicitCast("POINT (10 10)")
            )
        );
        assertEvaluateNull(
            "within(geopoint, geoshape)",
            Literal.of(
                DataTypes.GEO_POINT,
                DataTypes.GEO_POINT.implicitCast("POINT (10 10)")
            ),
            Literal.of(DataTypes.GEO_SHAPE, null)
        );
    }

    @Test
    public void testEvaluatePointLiteralWithinPolygonLiteral() {
        assertEvaluate(
            "within(geopoint, geoshape)",
            true,
            Literal.of(
                DataTypes.GEO_SHAPE,
                DataTypes.GEO_SHAPE.implicitCast("POINT (10 10)")
            ),
            Literal.of(
                DataTypes.GEO_SHAPE,
                DataTypes.GEO_SHAPE.implicitCast("POLYGON ((5 5, 20 5, 30 30, 5 30, 5 5))")
            )
        );
    }

    @Test
    public void testEvaluateShapeWithinShape() {
        assertEvaluate(
            "within(geoshape, geoshape)",
            true,
            Literal.of(
                DataTypes.GEO_SHAPE,
                DataTypes.GEO_SHAPE.implicitCast("LINESTRING (8 15, 13 24)")
            ),
            Literal.of(
                DataTypes.GEO_SHAPE,
                DataTypes.GEO_SHAPE.implicitCast("POLYGON ((5 5, 20 5, 30 30, 5 30, 5 5))")
            )
        );
    }

    @Test
    public void testEvaluateShapeIsNotWithinShape() {
        assertEvaluate(
            "within(geoshape, geoshape)",
            false,
            Literal.of(
                DataTypes.GEO_SHAPE,
                DataTypes.GEO_SHAPE.implicitCast("LINESTRING (8 15, 40 74)")
            ),
            Literal.of(
                DataTypes.GEO_SHAPE,
                DataTypes.GEO_SHAPE.implicitCast("POLYGON ((5 5, 20 5, 30 30, 5 30, 5 5))")
            )
        );
    }

    @Test
    public void testEvaluateObjectWithinShape() {
        assertEvaluate("within(geopoint, geoshape)", true,
            Literal.of(Map.of("type", "Point", "coordinates", new double[]{10.0, 10.0})),
            Literal.of(
                DataTypes.GEO_SHAPE,
                DataTypes.GEO_SHAPE.implicitCast("POLYGON ((5 5, 20 5, 30 30, 5 30, 5 5))")
            )
        );
    }

    @Test
    public void testNormalizeWithReferenceAndLiteral() throws Exception {
        assertNormalize("within(geopoint, 'POLYGON ((5 5, 20 5, 30 30, 5 30, 5 5))')", isFunction(FNAME));
    }

    @Test
    public void testNormalizeWithTwoStringLiterals() throws Exception {
        assertNormalize("within('POINT (10 10)', 'POLYGON ((5 5, 20 5, 30 30, 5 30, 5 5))')", isLiteral(true));
    }

    @Test
    public void testNormalizeWithFirstArgAsStringReference() throws Exception {
        assertNormalize("within(geostring, 'POLYGON ((5 5, 20 5, 30 30, 5 30, 5 5))')", isFunction(FNAME));
    }

    @Test
    public void testNormalizeWithSecondArgAsStringReference() throws Exception {
        assertNormalize("within('POLYGON ((5 5, 20 5, 30 30, 5 30, 5 5))', geostring)", isFunction(FNAME));
    }

    @Test
    public void testFirstArgumentWithInvalidType() throws Exception {
        assertThatThrownBy(() -> getFunction(FNAME, List.of(DataTypes.LONG, DataTypes.GEO_POINT)))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessageStartingWith("Unknown function: within(INPUT(0), INPUT(0)), " +
                                    "no overload found for matching argument types: (bigint, geo_point).");
    }

    @Test
    public void testSecondArgumentWithInvalidType() throws Exception {
        assertThatThrownBy(() -> getFunction(FNAME, List.of(DataTypes.GEO_POINT, DataTypes.LONG)))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessageStartingWith("Unknown function: within(INPUT(0), INPUT(0)), " +
                                    "no overload found for matching argument types: (geo_point, bigint).");
    }

    @Test
    public void testNormalizeFromObject() throws Exception {
        assertNormalize("within('POINT (1.0 0.0)', {type='Point', coordinates=[0.0, 1.0]})",
                        isLiteral(Boolean.FALSE));
    }
}
