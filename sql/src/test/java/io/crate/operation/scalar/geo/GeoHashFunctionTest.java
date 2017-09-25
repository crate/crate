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

import io.crate.analyze.symbol.Literal;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.types.DataTypes;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isLiteral;

public class GeoHashFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testEvaluateWithGeoPointLiterals() throws Exception {
        assertEvaluate("geohash(geopoint)", "u0qvtty6jk7x",
            Literal.of(DataTypes.GEO_POINT, DataTypes.GEO_POINT.value("POINT(9.7427 47.4050)")));
    }

    @Test
    public void testWithNullValue() throws Exception {
        assertEvaluate("geohash(geopoint)", null,
            Literal.of(DataTypes.GEO_POINT, null));
    }

    @Test
    public void testNormalizeWithStringTypes() throws Exception {
        assertNormalize("geohash('POINT (10 20)')", isLiteral("s5x1g8cu2yhr"), false);
    }

    @Test
    public void testNormalizeWithDoubleArray() throws Exception {
        assertNormalize("geohash([10.0, 20.0])", isLiteral("s5x1g8cu2yhr"), false);
    }

    @Test
    public void testWithTooManyArguments() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("unknown function: geohash(string, string)");
        assertNormalize("geohash('POINT (10 20)', 'foo')", null);
    }

    @Test
    public void testResolveWithInvalidType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot convert \"1\" to geo_point. Unknown Shape definition");
        assertEvaluate("geohash(1)", null);
    }

    @Test
    public void testWithInvalidStringReferences() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot convert \"POINT (foo)\" to geo_point. Expected a number");
        assertEvaluate("geohash('POINT (foo)')", null);
    }
}
