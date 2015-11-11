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

import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.FunctionIdent;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.instanceOf;

public class ToGeoPointFunctionTest extends AbstractScalarFunctionsTest {

    private ToGeoFunction getFunction(DataType argType) {
        return (ToGeoFunction)functions.get(new FunctionIdent(CastFunctionResolver.FunctionNames.TO_GEO_POINT, Collections.singletonList(argType)));
    }

    @Test
    public void testEvaluateCastFromString() throws Exception {
        ToGeoFunction fn = getFunction(DataTypes.STRING);
        Object val = fn.evaluate(Literal.newLiteral(DataTypes.STRING, "POINT (0.0 0.1)"));
        assertThat(val, instanceOf(Double[].class));
        assertThat((Double[])val, arrayContaining(0.0d, 0.1d));
    }

    @Test
    public void testNormalizeCastFromString() throws Exception {
        ToGeoFunction fn = getFunction(DataTypes.STRING);
        Symbol normalized = fn.normalizeSymbol(new Function(fn.info(),
                Collections.<Symbol>singletonList(Literal.newLiteral(DataTypes.STRING, "POINT (0 0)"))));
        assertThat(normalized, TestingHelpers.isLiteral(new Double[]{0.0, 0.0}));
    }

    @Test
    public void testEvaluateCastFromInvalidString() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot convert \"POINT ((0 0), (1 1))\" to geo_point");
        ToGeoFunction fn = getFunction(DataTypes.STRING);
        fn.evaluate(Literal.newLiteral(DataTypes.STRING, "POINT ((0 0), (1 1))"));
    }

    @Test
    public void testInvalidType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("type 'integer' not supported for conversion to 'geo_point'");
        getFunction(DataTypes.INTEGER);
    }
}
