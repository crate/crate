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

package io.crate.execution.engine.aggregation.impl;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.math.BigDecimal;
import java.util.List;

import org.elasticsearch.Version;
import org.hamcrest.Matchers;
import org.junit.Test;

import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.aggregation.impl.average.AverageAggregation;
import io.crate.execution.engine.aggregation.impl.average.numeric.NumericAverageState;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.SearchPath;
import io.crate.metadata.functions.Signature;
import io.crate.operation.aggregation.AggregationTestCase;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.NumericType;

public class AverageAggregationTest extends AggregationTestCase {

    private static final Signature NUMERIC_AVG_SIGNATURE = Signature.aggregate(
        AverageAggregation.NAME,
        DataTypes.NUMERIC.getTypeSignature(),
        DataTypes.NUMERIC.getTypeSignature()
    );

    private Object executeAggregation(DataType<?> argumentType, Object[][] data) throws Exception {
        return executeAggregation(
            Signature.aggregate(
                AverageAggregation.NAME,
                argumentType.getTypeSignature(),
                DataTypes.DOUBLE.getTypeSignature()
            ),
            data,
            List.of()
        );
    }

    @Test
    public void test_function_implements_doc_values_aggregator_for_numeric_types() {
        for (var dataType : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            assertHasDocValueAggregator(AverageAggregation.NAME, List.of(dataType));
        }
    }

    @Test
    public void testReturnType() throws Exception {
        // Return type is fixed to Double
        assertEquals(DataTypes.DOUBLE, getFunction("avg").boundSignature().returnType());
        assertEquals(DataTypes.DOUBLE, getFunction("mean").boundSignature().returnType());
    }

    private FunctionImplementation getFunction(String name) {
        return nodeCtx.functions().get(
            null, name, List.of(Literal.of(DataTypes.INTEGER, null)), SearchPath.pathWithPGCatalogAndDoc());
    }

    @Test
    public void testDouble() throws Exception {
        Object[][] data = new Object[100][];
        for (int i = 0; i < 100; i++) {
            data[i] = new Object[] {10000.1d};
        }

        // AverageAggregation returns double
        double result = (double) executeAggregation(DataTypes.DOUBLE, data);

        assertEquals(10000.1d, result, 0d);
    }

    @Test
    public void testFloat() throws Exception {
        Object[][] data = new Object[100][];
        for (int i = 0; i < 100; i++) {
            data[i] = new Object[]{10000.1f};
        }

        //AverageAggregation returns double
        double result = (double) executeAggregation(DataTypes.FLOAT, data);

        assertEquals(10000.1f, result, 0f);
    }

    @Test
    public void testInteger() throws Exception {
        Object result = executeAggregation(DataTypes.INTEGER, new Object[][]{{7}, {3}});

        assertEquals(5d, result);
    }

    @Test
    public void testLong() throws Exception {
        Object result = executeAggregation(DataTypes.LONG, new Object[][]{{7L}, {3L}});

        assertEquals(5d, result);
    }

    @Test
    public void testShort() throws Exception {
        Object result = executeAggregation(DataTypes.SHORT, new Object[][]{{(short) 7}, {(short) 3}});

        assertEquals(5d, result);
    }

    @Test
    public void test_avg_with_byte_argument_type() throws Exception {
        assertThat(executeAggregation(DataTypes.BYTE, new Object[][]{{(byte) 7}, {(byte) 3}}), is(5d));
    }

    @Test
    public void testUnsupportedType() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Unknown function: avg(INPUT(0))," +
                                        " no overload found for matching argument types: (geo_point).");
        executeAggregation(DataTypes.GEO_POINT, new Object[][]{});
    }

    @Test
    public void test_avg_numeric_on_long_non_doc_values_does_not_overflow() {
        Version minNodeVersion = randomBoolean()
            ? Version.CURRENT
            : Version.V_4_0_9;
        var result = execPartialAggregationWithoutDocValues(
            (AggregationFunction) nodeCtx.functions().getQualified(
                NUMERIC_AVG_SIGNATURE,
                List.of(DataTypes.NUMERIC),
                DataTypes.NUMERIC
            ), new Object[][]{{Long.MAX_VALUE}, {Long.MAX_VALUE}, {Long.MAX_VALUE}},
            true,
            minNodeVersion

        );
        assertThat(((NumericAverageState)result).value(), Matchers.is(BigDecimal.valueOf(Long.MAX_VALUE)));
    }

    @Test
    public void test_avg_numeric_on_double_non_doc_values() {
        Version minNodeVersion = randomBoolean()
            ? Version.CURRENT
            : Version.V_4_0_9;
        var result = execPartialAggregationWithoutDocValues(
            (AggregationFunction) nodeCtx.functions().getQualified(
                NUMERIC_AVG_SIGNATURE,
                List.of(DataTypes.NUMERIC),
                DataTypes.NUMERIC
            ), new Object[][]{{0.3d}, {0.7d}},
            true,
            minNodeVersion
        );
        assertThat(((NumericAverageState)result).value(), Matchers.is(BigDecimal.valueOf(0.5d)));
    }

    @Test
    public void test_avg_numeric_with_precision_and_scale_on_double_non_doc_values() {
        var type = NumericType.of(16, 2);
        var expected = type.implicitCast(12.4357);
        assertThat(expected.toString(), Matchers.is("12.44"));

        Version minNodeVersion = randomBoolean()
            ? Version.CURRENT
            : Version.V_4_0_9;

        var result = execPartialAggregationWithoutDocValues(
            (AggregationFunction) nodeCtx.functions().getQualified(
                NUMERIC_AVG_SIGNATURE,
                List.of(type),
                DataTypes.NUMERIC
            ),
            new Object[][]{{12.4357d}, {12.4357d}},
            true,
            minNodeVersion
        );
        assertThat(((NumericAverageState)result).value().toString(), Matchers.is(expected.toString()));
    }
}
