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

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

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
import io.crate.operation.aggregation.LargestTriangleThreeBucketsAggregation.LttbState;
import io.crate.testing.PlainRamAccounting;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class LargestTriangleThreeBucketsAggregationTest extends AggregationTestCase {

    @Before
    public void prepareFunctions() {
        nodeCtx = createNodeContext(null, null);
    }

    @SuppressWarnings("unchecked")
    private AggregationFunction<LttbState, Map<String, List<? extends Number>>> getFunc() {
        return (AggregationFunction<LttbState, Map<String, List<? extends Number>>>) nodeCtx
                .functions().get(
                        null,
                        LargestTriangleThreeBucketsAggregation.NAME,
                        List.of(Literal.of(1L), Literal.of(2.0), Literal.of(3)),
                        SearchPath.pathWithPGCatalogAndDoc());
    }

    private Object executeAggregation(DataType<?> xType, Object[][] data) throws Exception {
        return executeAggregation(
                Signature.builder(LargestTriangleThreeBucketsAggregation.NAME, FunctionType.AGGREGATE)
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

    @Test
    public void test_iterate_should_initialize_state() throws Exception {
        AggregationFunction<LttbState, Map<String, List<? extends Number>>> func = getFunc();
        LttbState state = func.newState(RamAccounting.NO_ACCOUNTING, Version.CURRENT, memoryManager);
        assertThat(state.isInitialized()).isFalse();
        func.iterate(RamAccounting.NO_ACCOUNTING, memoryManager, state, Literal.of(1L), Literal.of(1.0), Literal.of(3));
        assertThat(state.isInitialized()).isTrue();
    }

    @Test
    public void test_iterate_should_add_point() throws Exception {
        AggregationFunction<LttbState, Map<String, List<? extends Number>>> func = getFunc();
        LttbState state = func.newState(RamAccounting.NO_ACCOUNTING, Version.CURRENT, memoryManager);
        func.iterate(RamAccounting.NO_ACCOUNTING, memoryManager, state, Literal.of(1L), Literal.of(1.0), Literal.of(3));
        assertThat(state.getPoints()).extracting(i -> i.timestamp).containsExactly(1L);
        assertThat(state.getPoints()).extracting(i -> i.value).containsExactly(1.0);
    }

    @Test
    public void test_iterate_raises_if_less_than_expected_arguments() throws Exception {
        AggregationFunction<LttbState, Map<String, List<? extends Number>>> func = getFunc();
        LttbState state = func.newState(RamAccounting.NO_ACCOUNTING, Version.CURRENT, memoryManager);
        assertThatThrownBy(() -> func.iterate(RamAccounting.NO_ACCOUNTING, memoryManager, state, Literal.of(1.0),
                Literal.of(3)))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("lttb expects 3 arguments: timestamp, value, and threshold");
    }

    @Test
    public void test_iterate_skips_point_if_timestamp_is_null() throws Exception {
        AggregationFunction<LttbState, Map<String, List<? extends Number>>> func = getFunc();
        LttbState state = func.newState(RamAccounting.NO_ACCOUNTING, Version.CURRENT,
                memoryManager);
        func.iterate(RamAccounting.NO_ACCOUNTING, memoryManager, state, Literal.of((Long) null),
                Literal.of(1.0),
                Literal.of(3));
        assertThat(state.getPoints()).hasSize(0);
    }

    @Test
    public void test_iterate_skips_point_if_value_is_null() throws Exception {
        AggregationFunction<LttbState, Map<String, List<? extends Number>>> func = getFunc();
        LttbState state = func.newState(RamAccounting.NO_ACCOUNTING, Version.CURRENT,
                memoryManager);
        func.iterate(RamAccounting.NO_ACCOUNTING, memoryManager, state, Literal.of(1L),
                Literal.of((Double) null),
                Literal.of(3));
        assertThat(state.getPoints()).hasSize(0);
    }

    @Test
    public void test_iterate_skips_point_if_threshold_is_null() throws Exception {
        AggregationFunction<LttbState, Map<String, List<? extends Number>>> func = getFunc();
        LttbState state = func.newState(RamAccounting.NO_ACCOUNTING, Version.CURRENT,
                memoryManager);
        assertThatThrownBy(() -> func.iterate(RamAccounting.NO_ACCOUNTING, memoryManager, state, Literal.of(1L),
                Literal.of(1.0),
                Literal.of((Integer) null))).isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("lttb expects threshold not to be a null");
    }

    @Test
    public void test_terminate_partial_returns_map() throws Exception {
        AggregationFunction<LttbState, Map<String, List<? extends Number>>> func = getFunc();
        LttbState state = func.newState(RamAccounting.NO_ACCOUNTING, Version.CURRENT,
                memoryManager);
        func.iterate(RamAccounting.NO_ACCOUNTING, memoryManager, state, Literal.of(1L),
                Literal.of(90.0),
                Literal.of(3));
        func.iterate(RamAccounting.NO_ACCOUNTING, memoryManager, state, Literal.of(2L),
                Literal.of(40.0),
                Literal.of(3));
        func.iterate(RamAccounting.NO_ACCOUNTING, memoryManager, state, Literal.of(3L),
                Literal.of(10.0),
                Literal.of(3));

        Map<String, List<? extends Number>> result = func.terminatePartial(RamAccounting.NO_ACCOUNTING, state);
        assertThat(result).containsOnlyKeys("x", "y");
        assertThat(result.get("x")).extracting(i -> i.longValue()).containsExactly(1L, 2L, 3L);
        assertThat(result.get("y")).extracting(i -> i.doubleValue()).containsExactly(90.0, 40.0, 10.0);
    }

    @Test
    public void test_should_execute_with_timestamp() throws Exception {
        Object[][] data = new Object[1][3];
        data[0][0] = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
        data[0][1] = 2.0;
        data[0][2] = 1;
        Object result = executeAggregation(DataTypes.TIMESTAMP, data);
        assertThat(result).extracting("x").asInstanceOf(LIST).hasSize(1);
        assertThat(result).extracting("y").asInstanceOf(LIST).hasSize(1);
    }

    @Test
    public void test_should_execute_with_timestampz() throws Exception {
        Object[][] data = new Object[1][3];
        data[0][0] = OffsetDateTime.now().toEpochSecond();
        data[0][1] = 2.0;
        data[0][2] = 1;
        Object result = executeAggregation(DataTypes.TIMESTAMPZ, data);
        assertThat(result).extracting("x").asInstanceOf(LIST).hasSize(1);
        assertThat(result).extracting("y").asInstanceOf(LIST).hasSize(1);
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
    public void test_reduce_should_handle_uninitialized_state1() throws Exception {
        AggregationFunction<LttbState, Map<String, List<? extends Number>>> func = getFunc();
        LttbState state1 = new LttbState();
        LttbState state2 = func.newState(
                RamAccounting.NO_ACCOUNTING, Version.CURRENT, memoryManager);
        func.iterate(RamAccounting.NO_ACCOUNTING, memoryManager, state2, Literal.of(1L),
                Literal.of(1.0), Literal.of(3));
        assertThat(state1.isInitialized()).isFalse();

        LttbState newState = func.reduce(RamAccounting.NO_ACCOUNTING, state1, state2);

        assertThat(newState.getPoints()).extracting(i -> i.timestamp).containsExactly(1L);
        assertThat(newState.getPoints()).extracting(i -> i.value).containsExactly(1.0);
        assertThat(newState.getThreshold()).isEqualTo(3);
    }

    @Test
    public void test_reduce_should_merge_states() throws Exception {
        AggregationFunction<LttbState, Map<String, List<? extends Number>>> func = getFunc();
        LttbState state1 = func.newState(
                RamAccounting.NO_ACCOUNTING, Version.CURRENT, memoryManager);
        LttbState state2 = func.newState(
                RamAccounting.NO_ACCOUNTING, Version.CURRENT, memoryManager);

        func.iterate(RamAccounting.NO_ACCOUNTING, memoryManager, state1, Literal.of(1L),
                Literal.of(1.0), Literal.of(3));
        func.iterate(RamAccounting.NO_ACCOUNTING, memoryManager, state2, Literal.of(2L),
                Literal.of(2.0), Literal.of(3));

        LttbState newState = func.reduce(RamAccounting.NO_ACCOUNTING, state1, state2);

        assertThat(newState.getPoints()).extracting(i -> i.timestamp).containsExactly(1L, 2L);
        assertThat(newState.getPoints()).extracting(i -> i.value).containsExactly(1.0, 2.0);
        assertThat(newState.getThreshold()).isEqualTo(3);
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
