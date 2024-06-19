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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.List;

import org.elasticsearch.Version;
import org.joda.time.Period;
import org.junit.Test;

import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Scalar;
import io.crate.metadata.SearchPath;
import io.crate.metadata.functions.Signature;
import io.crate.operation.aggregation.AggregationTestCase;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.NumericType;

public class SumAggregationTest extends AggregationTestCase {

    private Object executeAggregation(DataType<?> argumentType,
                                      DataType<?> returnType,
                                      Object[][] data) throws Exception {
        return executeAggregation(
            Signature.aggregate(
                SumAggregation.NAME,
                argumentType.getTypeSignature(),
                returnType.getTypeSignature()
            ).withFeature(Scalar.Feature.DETERMINISTIC),
            data,
            List.of()
        );
    }

    @Test
    public void test_function_implements_doc_values_aggregator_for_numeric_types() {
        for (var dataType : DataTypes.NUMERIC_PRIMITIVE_TYPES) {
            assertHasDocValueAggregator(SumAggregation.NAME, List.of(dataType));
        }
    }

    @Test
    public void testReturnType() throws Exception {
        DataType<?> type = DataTypes.DOUBLE;
        assertThat(getSum(type).boundSignature().returnType().getTypeSignature()).isEqualTo(type.getTypeSignature());

        type = DataTypes.FLOAT;
        assertThat(getSum(type).boundSignature().returnType().getTypeSignature()).isEqualTo(type.getTypeSignature());

        type = DataTypes.LONG;
        assertThat(getSum(type).boundSignature().returnType().getTypeSignature()).isEqualTo(type.getTypeSignature());
        assertThat(getSum(DataTypes.INTEGER).boundSignature().returnType().getTypeSignature()).isEqualTo(type.getTypeSignature());
        assertThat(getSum(DataTypes.SHORT).boundSignature().returnType().getTypeSignature()).isEqualTo(type.getTypeSignature());
        assertThat(getSum(DataTypes.BYTE).boundSignature().returnType().getTypeSignature()).isEqualTo(type.getTypeSignature());
    }

    private FunctionImplementation getSum(DataType<?> type) {
        return nodeCtx.functions().get(
            null,
            "sum",
            List.of(Literal.of(type, null)),
            SearchPath.pathWithPGCatalogAndDoc()
        );
    }

    @Test
    public void testDouble() throws Exception {
        Object result = executeAggregation(DataTypes.DOUBLE, DataTypes.DOUBLE, new Object[][]{{0.7d}, {0.3d}});

        assertThat(result).isEqualTo(1.0d);
    }

    @Test
    public void testDoubleSummationWithoutLosingPrecision() throws Exception {
        Object result = executeAggregation(DataTypes.DOUBLE, DataTypes.DOUBLE, new Object[][]{{0.1d}, {0.3d}, {0.2d}});

        assertThat(result).isEqualTo(0.6d);
    }

    @Test
    public void testFloat() throws Exception {
        Object result = executeAggregation(DataTypes.FLOAT, DataTypes.FLOAT, new Object[][]{{0.7f}, {0.3f}});

        assertThat(result).isEqualTo(1.0f);
    }

    @Test
    public void testFloatSummationWithoutLosingPrecision() throws Exception {
        Object[][] rows = new Object[][] { { 0.8f }, { 0.4f }, { 0.2f } };
        Signature signature = Signature.aggregate(
            SumAggregation.NAME,
            DataTypes.FLOAT.getTypeSignature(),
            DataTypes.FLOAT.getTypeSignature()
        ).withFeature(Scalar.Feature.DETERMINISTIC);
        Object result = executeAggregation(
            signature,
            signature.getArgumentDataTypes(),
            signature.getReturnType().createType(),
            rows,
            false,
            List.of()
        );
        assertThat(result).isEqualTo(1.4f);
    }

    @Test
    public void testLong() throws Exception {
        Object result = executeAggregation(DataTypes.LONG, DataTypes.LONG, new Object[][]{{7L}, {3L}});

        assertThat(result).isEqualTo(10L);
    }

    @Test(expected = ArithmeticException.class)
    public void testLongOverflow() throws Exception {
        executeAggregation(DataTypes.LONG, DataTypes.LONG, new Object[][]{{Long.MAX_VALUE}, {1}});
    }

    @Test(expected = ArithmeticException.class)
    public void testLongUnderflow() throws Exception {
        executeAggregation(DataTypes.LONG, DataTypes.LONG, new Object[][]{{Long.MIN_VALUE}, {-1}});
    }

    @Test
    public void testInteger() throws Exception {
        Object result = executeAggregation(DataTypes.INTEGER, DataTypes.LONG, new Object[][]{{7}, {3}});

        assertThat(result).isEqualTo(10L);
    }

    @Test
    public void testShort() throws Exception {
        Object result = executeAggregation(DataTypes.SHORT, DataTypes.LONG, new Object[][]{{(short) 7}, {(short) 3}});

        assertThat(result).isEqualTo(10L);
    }

    @Test
    public void testByte() throws Exception {
        Object result = executeAggregation(DataTypes.BYTE, DataTypes.LONG, new Object[][]{{(byte) 7}, {(byte) 3}});

        assertThat(result).isEqualTo(10L);
    }

    @Test
    public void testInterval() {
        assertThat(execPartialAggregationWithoutDocValues(
                (IntervalSumAggregation) nodeCtx.functions().getQualified(
                    Signature.aggregate(
                            IntervalSumAggregation.NAME,
                            DataTypes.INTERVAL.getTypeSignature(),
                            DataTypes.INTERVAL.getTypeSignature())
                        .withFeature(Scalar.Feature.DETERMINISTIC),
                    List.of(DataTypes.INTERVAL),
                    DataTypes.INTERVAL
                ),
                new Object[][]{
                    {Period.days(6).withHours(12).withSeconds(10)},
                    {Period.hours(3).withMinutes(2).withMillis(123)},
                    {Period.minutes(20).withSeconds(3).withMillis(321)},
                    {Period.days(2).withHours(4).withMillis(223)}},
                true,
                Version.CURRENT
            )).isEqualTo(
            Period.days(8).withHours(19).withMinutes(22).withSeconds(13).withMillis(667));
    }

    @Test
    public void testUnsupportedType() throws Exception {
        assertThatThrownBy(() -> getSum(DataTypes.GEO_POINT))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessageStartingWith(
                "Unknown function: sum(NULL)," +
                " no overload found for matching argument types: (geo_point).");
    }

    @Test
    public void test_sum_numeric_on_long_non_doc_values_field() {
        Version minNodeVersion = randomBoolean()
            ? Version.CURRENT
            : Version.V_4_0_9;
        var result = execPartialAggregationWithoutDocValues(
            (AggregationFunction<?, ?>) nodeCtx.functions().getQualified(
                NumericSumAggregation.SIGNATURE,
                List.of(DataTypes.NUMERIC),
                DataTypes.NUMERIC
            ), new Object[][]{{1L}, {2L}, {3L}},
            true,
            minNodeVersion

        );
        assertThat(result).isEqualTo(BigDecimal.valueOf(6));
    }

    @Test
    public void test_sum_numeric_on_long_non_doc_values_field_with_overflow() {
        Version minNodeVersion = randomBoolean()
            ? Version.CURRENT
            : Version.V_4_0_9;
        var result = execPartialAggregationWithoutDocValues(
            (AggregationFunction<?, ?>) nodeCtx.functions().getQualified(
                NumericSumAggregation.SIGNATURE,
                List.of(DataTypes.NUMERIC),
                DataTypes.NUMERIC
            ), new Object[][]{{Long.MAX_VALUE}, {10L}},
            true,
            minNodeVersion
        );
        assertThat(result).isEqualTo(BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.TEN));
    }

    @Test
    public void test_sum_numeric_on_floating_point_non_doc_values_field() {
        Version minNodeVersion = randomBoolean()
            ? Version.CURRENT
            : Version.V_4_0_9;
        var result = execPartialAggregationWithoutDocValues(
            (AggregationFunction<?, ?>) nodeCtx.functions().getQualified(
                NumericSumAggregation.SIGNATURE,
                List.of(DataTypes.NUMERIC),
                DataTypes.NUMERIC
            ), new Object[][]{{1d}, {1d}},
            true,
            minNodeVersion
        );
        assertThat(result).isEqualTo(BigDecimal.valueOf(2.0));
    }

    @Test
    public void test_sum_numeric_with_precision_and_scale_on_double_non_doc_values_field() {
        var type = NumericType.of(16, 2);
        var expected = type.implicitCast(12.4357);
        assertThat(expected.toString()).isEqualTo("12.44");

        Version minNodeVersion = randomBoolean()
            ? Version.CURRENT
            : Version.V_4_0_9;
        var result = execPartialAggregationWithoutDocValues(
            (AggregationFunction<?, ?>) nodeCtx.functions().getQualified(
                NumericSumAggregation.SIGNATURE,
                List.of(type),
                DataTypes.NUMERIC
            ),
            new Object[][]{{12d}, {0.4357d}},
            true,
            minNodeVersion
        );
        assertThat(result.toString()).isEqualTo(expected.toString());
    }
}
