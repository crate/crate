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

package io.crate.operation.scalar.cast;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.*;

public class ToGeoShapeFunctionTest extends AbstractScalarFunctionsTest{

    public static final String FUNCTION_NAME = CastFunctionResolver.FunctionNames.TO_GEO_SHAPE;

    public static final String VALID_STR = "POINT (0.0 0.1)";
    public static final String INVALID_STR = "POINTE ()";
    public static final Map<String, Object> VALID_OBJECT = ImmutableMap.of(
            "type", "Polygon",
            "coordinates", Collections.singletonList(
                    Arrays.asList(
                        Arrays.asList(0.0d, 0.0d),
                        Arrays.asList(42.0d, 0.0d),
                        Arrays.asList(42.0d, 1.0d),
                        Arrays.asList(42.0d, 42.0d),
                        Arrays.asList(1.0d, 42.0d),
                        Arrays.asList(0.0d, 0.0d)
                    )
            )
    );
    public static final Map<String, Object> INVALID_OBJECT = ImmutableMap.<String, Object>of(
            "type", "Polygon",
            "coordinates", new double[][] {
                    {0, 0},
                    {42, 0},
                    {42, 1},
                    {42, 42},
                    {1, 42},
                    {0, 0}
            }
        );

    @Test
    public void testEvaluateCastFromString() throws Exception {
        ToGeoFunction fn = getFunction(FUNCTION_NAME, DataTypes.STRING);
        Object val = fn.evaluate(Literal.newLiteral(DataTypes.STRING, VALID_STR));
        assertThat(val, instanceOf(Map.class));
        assertThat((Map<String, Object>)val, allOf(
                hasEntry("type", (Object)"Point"),
                hasEntry("coordinates", (Object)new double[]{0.0, 0.1})));
    }

    @Test
    public void testEvaluateCastFromInvalidString() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot convert \"POINTE ()\" to geo_shape");
        ToGeoFunction fn = getFunction(FUNCTION_NAME, DataTypes.STRING);
        fn.evaluate(Literal.newLiteral(DataTypes.STRING, INVALID_STR));
    }

    @Test
    public void testEvaluateCastFromObject() throws Exception {
        ToGeoFunction fn = getFunction(FUNCTION_NAME, DataTypes.OBJECT);

        Object val = fn.evaluate(Literal.newLiteral(DataTypes.OBJECT, VALID_OBJECT));
        assertThat(val, instanceOf(Map.class));
        Map<String, Object> valMap = (Map<String, Object>)val;
        assertThat(valMap.size(), is(2));
        assertThat(valMap, Matchers.allOf(
                Matchers.<String, Object>hasEntry("type", "Polygon"),
                Matchers.hasEntry("coordinates", VALID_OBJECT.get("coordinates"))));
    }

    @Test
    public void testEvaluateCastFromInvalidObject() throws Exception {
        ToGeoFunction fn = getFunction(FUNCTION_NAME, DataTypes.OBJECT);

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(allOf(startsWith("Cannot convert"), endsWith("to geo_shape")));
        fn.evaluate(Literal.newLiteral(DataTypes.OBJECT, INVALID_OBJECT));
    }

    @Test
    public void testNormalizeFromString() throws Exception {
        Symbol normalized = normalize(FUNCTION_NAME, VALID_STR, DataTypes.STRING);
        assertThat(normalized, instanceOf(Literal.class));
        assertThat(normalized.valueType(), is((DataType)DataTypes.GEO_SHAPE));
        Map<String, Object> value = ((Literal<Map<String, Object>>)normalized).value();
        assertThat(value, hasEntry("type", (Object)"Point"));
        assertThat(value, hasKey("coordinates"));
        double[] coords = (double[])value.get("coordinates");
        assertThat(coords.length, is(2));
        assertThat(coords[0], is(0.0d));
        assertThat(coords[1], is(0.1d));
    }

    @Test
    public void testNormalizeFromObject() throws Exception {
        Symbol normalized = normalize(FUNCTION_NAME, VALID_OBJECT, DataTypes.OBJECT);
        assertThat(normalized, instanceOf(Literal.class));
        assertThat(normalized.valueType(), is((DataType)DataTypes.GEO_SHAPE));
        Map<String, Object> value = ((Literal<Map<String, Object>>)normalized).value();
        assertThat(value, hasEntry("type", (Object)"Polygon"));
        assertThat(value, hasEntry("coordinates", VALID_OBJECT.get("coordinates")));
    }

    @Test
    public void testNormalizeFromInvalidString() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("cannot cast 'POINTE ()' to geo_shape");
        normalize(FUNCTION_NAME, INVALID_STR, DataTypes.STRING);
    }

    @Test
    public void testNormalizeFromInvalidObject() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("cannot cast {'coordinates': [[0.0, 0.0], [42.0, 0.0], [42.0, 1.0], [42.0, 42.0], [1.0, 42.0], [0.0, 0.0]], 'type': 'Polygon'} to geo_shape");
        normalize(FUNCTION_NAME, INVALID_OBJECT, DataTypes.OBJECT);
    }

    @Test
    public void testCastFromInvalidType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("type 'boolean' not supported for conversion to 'geo_shape'");
        getFunction(FUNCTION_NAME, DataTypes.BOOLEAN);

    }
}
