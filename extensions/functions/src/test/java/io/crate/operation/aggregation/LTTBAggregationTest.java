/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operation.aggregation;

import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.LIST;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.elasticsearch.Version;
import org.junit.Before;
import org.junit.Test;

import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Scalar;
import io.crate.metadata.SearchPath;
import io.crate.metadata.functions.Signature;
import io.crate.operation.aggregation.LTTBAggregation.LttbState;
import io.crate.testing.PlainRamAccounting;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class LTTBAggregationTest extends AggregationTestCase {

    @Before
    public void prepareFunctions() {
        nodeCtx = createNodeContext(null, null);
    }

    @SuppressWarnings("unchecked")
    private AggregationFunction<LttbState, Map<String, List<? extends Number>>> getFunc() {
        return (AggregationFunction<LttbState, Map<String, List<? extends Number>>>) nodeCtx
                .functions().get(
                        null,
                        LTTBAggregation.NAME,
                        List.of(Literal.of(1L), Literal.of(2.0), Literal.of(3)),
                        SearchPath.pathWithPGCatalogAndDoc());
    }

    private Object executeAggregation(DataType<?> xType, Object[][] data) throws Exception {
        return executeAggregation(
                Signature.builder(LTTBAggregation.NAME, FunctionType.AGGREGATE)
                        .argumentTypes(
                                xType.getTypeSignature(),
                                DataTypes.DOUBLE.getTypeSignature(),
                                DataTypes.INTEGER.getTypeSignature())
                        .returnType(DataTypes.UNTYPED_OBJECT.getTypeSignature())
                        .features(Scalar.Feature.DETERMINISTIC)
                        .build(),
                data,
                List.of());
    }

    private Object[][] createTestData(int numRows, Function<Integer, Long> timestampGenerator,
            Function<Integer, Double> valueGenerator, int threshold) {
        Object[][] data = new Object[numRows][3];
        for (int i = 0; i < numRows; i++) {
            data[i][0] = timestampGenerator.apply(i);
            data[i][1] = valueGenerator.apply(i);
            data[i][2] = threshold;
        }
        return data;
    }

    @Test
    public void test_should_execute_with_timestamp() throws Exception {
        Object[][] data = createTestData(1000, i -> System.currentTimeMillis() + (i * 1000),
                i -> Math.sin(i * 0.1) * 100 + 50, 100);
        Object result = executeAggregation(DataTypes.TIMESTAMP, data);
        assertThat(result).extracting("x").asInstanceOf(LIST).hasSize(100);
        assertThat(result).extracting("y").asInstanceOf(LIST).hasSize(100);
    }

    @Test
    public void test_should_execute_with_timestampz() throws Exception {
        Object[][] data = createTestData(1000, i -> System.currentTimeMillis() + (i * 1000),
                i -> Math.sin(i * 0.1) * 100 + 50, 100);
        Object result = executeAggregation(DataTypes.TIMESTAMPZ, data);
        assertThat(result).extracting("x").asInstanceOf(LIST).hasSize(100);
        assertThat(result).extracting("y").asInstanceOf(LIST).hasSize(100);
    }

    @Test
    public void test_raise_if_x_type_is_not_timestamp() throws Exception {
        Object[][] data = new Object[3][3];
        data[0][0] = "invalid type - not a timestamp";
        data[0][1] = 2.0;
        data[0][2] = 1;
        assertThatThrownBy(() -> executeAggregation(DataTypes.TIMESTAMP, data))
                .isExactlyInstanceOf(ClassCastException.class);
    }

    @Test
    public void test_raise_if_y_type_is_not_numeric() throws Exception {
        Object[][] data = new Object[3][3];
        data[0][0] = 20L;
        data[0][1] = "invalid type - not numeric";
        data[0][2] = 1;
        assertThatThrownBy(() -> executeAggregation(DataTypes.TIMESTAMP, data))
                .isExactlyInstanceOf(ClassCastException.class);
    }

    @Test
    public void test_should_succeed_even_with_null_records() throws Exception {
        Object[][] data = createTestData(10, i -> i == 1 ? null : System.currentTimeMillis() + (i * 1000),
                i -> i == 5 ? null : Math.sin(i * 0.1) * 100 + 50, 5);
        Object result = executeAggregation(DataTypes.TIMESTAMPZ, data);
        assertThat(result).extracting("x").asInstanceOf(LIST).hasSize(5);
        assertThat(result).extracting("y").asInstanceOf(LIST).hasSize(5);
    }

    @Test
    public void test_should_not_downsample_if_threshold_zero() throws Exception {
        Object[][] data = createTestData(10, i -> System.currentTimeMillis() + (i * 1000),
                i -> Math.sin(i * 0.1) * 100 + 50, 0);
        Object result = executeAggregation(DataTypes.TIMESTAMPZ, data);
        assertThat(result).extracting("x").asInstanceOf(LIST).hasSize(10);
        assertThat(result).extracting("y").asInstanceOf(LIST).hasSize(10);
    }

    @Test
    public void test_should_not_downsample_if_threshold_larger_than_amount_of_data() throws Exception {
        Object[][] data = createTestData(10, i -> System.currentTimeMillis() + (i * 1000),
                i -> Math.sin(i * 0.1) * 100 + 50, 5000);
        Object result = executeAggregation(DataTypes.TIMESTAMPZ, data);
        assertThat(result).extracting("x").asInstanceOf(LIST).hasSize(10);
        assertThat(result).extracting("y").asInstanceOf(LIST).hasSize(10);
    }

    @Test
    public void test_should_select_points_that_form_largest_triangle() throws Exception {
        List<Double> values = List.of(-10.0, 12.0, 50.0, 15.0, 11.0);
        Object[][] data = createTestData(5, i -> System.currentTimeMillis() + (i * 1000),
                i -> values.get(i), 3);
        Object result = executeAggregation(DataTypes.TIMESTAMPZ, data);
        assertThat(result).extracting("y").asInstanceOf(LIST).containsExactly(-10.0, 50.0, 11.0);

    }

    @Test
    public void test_should_account_for_memory_when_creating_new_state() throws Exception {
        AggregationFunction<LttbState, Map<String, List<? extends Number>>> func = getFunc();
        RamAccounting ramAccounting = new PlainRamAccounting();
        func.newState(ramAccounting, Version.CURRENT, memoryManager);
        assertThat(ramAccounting.totalBytes()).isEqualTo(LttbState.SHALLOW_SIZE);
    }

    @Test
    public void test_raises_when_memory_exceeds_threshold() throws Exception {
        AggregationFunction<LttbState, Map<String, List<? extends Number>>> func = getFunc();
        LttbState state = func.newState(
                RamAccounting.NO_ACCOUNTING, Version.CURRENT, memoryManager);

        // store the amount of memory used for one iteration
        RamAccounting startRamAccounting = new PlainRamAccounting();
        func.iterate(startRamAccounting, memoryManager, state, Literal.of(1L),
                Literal.of(1.0), Literal.of(3));

        // allow enough memory to be used for one more iteration, after which all
        // allotted memory for this aggregation is used
        RamAccounting limitRamAccounting = new PlainRamAccounting(startRamAccounting.totalBytes());
        func.iterate(limitRamAccounting, memoryManager, state, Literal.of(1L),
                Literal.of(1.0), Literal.of(3));

        assertThatThrownBy(
                () -> func.iterate(limitRamAccounting, memoryManager, state, Literal.of(1L), Literal.of(1.0),
                        Literal.of(3)))
                .isExactlyInstanceOf(RuntimeException.class);
    }
}
