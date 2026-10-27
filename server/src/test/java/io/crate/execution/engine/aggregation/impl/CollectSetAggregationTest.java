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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import io.crate.data.Input;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionType;
import io.crate.metadata.SearchPath;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.functions.Signature.Feature;
import io.crate.operation.aggregation.AggregationTestCase;
import io.crate.testing.DataTypeTesting;
import io.crate.testing.PlainRamAccounting;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.NumericType;

public class CollectSetAggregationTest extends AggregationTestCase {

    private Object executeAggregation(DataType<?> argumentType, Object[][] data) throws Exception {
        return executeAggregation(
                Signature.builder("collect_set", FunctionType.AGGREGATE)
                        .argumentTypes(argumentType.getTypeSignature())
                        .returnType(new ArrayType<>(argumentType).getTypeSignature())
                        .features(Feature.DETERMINISTIC)
                        .build(),
                data,
                List.of()
        );
    }

    @Test
    public void testReturnType() throws Exception {
        FunctionImplementation collectSet = nodeCtx.functions().get(
            null, "collect_set", List.of(Literal.of(DataTypes.INTEGER, null)), SearchPath.pathWithPGCatalogAndDoc());
        assertThat(collectSet.boundSignature().returnType()).isEqualTo(new ArrayType<>(DataTypes.INTEGER));
        collectSet = nodeCtx.functions().get(
            null, "collect_set", List.of(Literal.of(DataTypes.NUMERIC, null)), SearchPath.pathWithPGCatalogAndDoc());
        assertThat(collectSet.boundSignature().returnType()).isEqualTo(new ArrayType<>(DataTypes.NUMERIC));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testNumeric() throws Exception {
        assertThat(
            (List<BigDecimal>) executeAggregation(new NumericType(4, 3), new Object[][]{{
                new BigDecimal("0.713")}, {new BigDecimal("0.312")}, {new BigDecimal("0.713")}}))
            .containsExactlyInAnyOrder(new BigDecimal("0.312"), new BigDecimal("0.713"));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testLongSerialization() throws Exception {
        var impl = (AggregationFunction) nodeCtx.functions().get(
                null, "collect_set", List.of(Literal.of(DataTypes.LONG, null)),
            SearchPath.pathWithPGCatalogAndDoc());

        Object state = impl.newState(RAM_ACCOUNTING, Version.CURRENT, memoryManager);

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        impl.partialType().streamer().writeValueTo(streamOutput, state);

        Object newState = impl.partialType().streamer().readValueFrom(streamOutput.bytes().streamInput());
        assertThat(newState).isEqualTo(state);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test_value_adding_and_removal() {
        var aggregationFunction = (AggregationFunction<Object, Object>) nodeCtx.functions().get(
            null, "collect_set", List.of(Literal.of(DataTypes.LONG, null)), SearchPath.pathWithPGCatalogAndDoc());

        RamAccounting ramAccounting = new PlainRamAccounting();
        Object state = aggregationFunction.newState(ramAccounting, Version.CURRENT, memoryManager);
        assertThat(ramAccounting.totalBytes()).isEqualTo(64L);
        state = aggregationFunction.iterate(ramAccounting, memoryManager, state, Literal.of(10L));
        state = aggregationFunction.iterate(ramAccounting, memoryManager, state, Literal.of(20L));
        state = aggregationFunction.iterate(ramAccounting, memoryManager, state, Literal.of(10L));
        assertThat(ramAccounting.totalBytes()).isEqualTo(256L);

        Object values = aggregationFunction.terminatePartial(ramAccounting, state);
        assertThat(ramAccounting.totalBytes()).isEqualTo(280L);
        assertThat((List<Object>) values).containsExactlyInAnyOrder(10L, 20L);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test_value_adding_and_removal_window_function_opt() {
        var impl = (AggregationFunction<Object, Object>) nodeCtx.functions().get(
            null, "collect_set", List.of(Literal.of(DataTypes.LONG, null)), SearchPath.pathWithPGCatalogAndDoc());
        var aggregationFunction = (AggregationFunction<Object, Object>) impl.optimizeForExecutionAsWindowFunction(Version.CURRENT);

        RamAccounting ramAccounting = new PlainRamAccounting();
        Object state = aggregationFunction.newState(ramAccounting, Version.CURRENT, memoryManager);
        assertThat(ramAccounting.totalBytes()).isEqualTo(64L);
        state = aggregationFunction.iterate(ramAccounting, memoryManager, state, Literal.of(10L));
        state = aggregationFunction.iterate(ramAccounting, memoryManager, state, Literal.of(10L));
        assertThat(ramAccounting.totalBytes()).isEqualTo(136L);

        aggregationFunction.removeFromAggregatedState(ramAccounting, state, new Input[] { Literal.of(10L) });
        aggregationFunction.removeFromAggregatedState(ramAccounting, state, new Input[] { Literal.of(10L) });
        assertThat(ramAccounting.totalBytes()).isEqualTo(64L);

        Object values = aggregationFunction.terminatePartial(ramAccounting, state);
        assertThat(ramAccounting.totalBytes()).isEqualTo(88L);
        assertThat((List<Object>) values).isEmpty();
    }

    @SuppressWarnings("unchecked")
    private <T> void assert_collect_set_on_primitive_type(DataType<T> type) throws Exception {
        Supplier<T> dataGenerator = DataTypeTesting.getDataGenerator(type);
        ArrayList<T> values = new ArrayList<>();
        T val1 = dataGenerator.get();
        values.add(val1);
        values.add(val1); // ensure there is one duplicate
        for (int i = 0; i < 4; i++) {
            values.add(dataGenerator.get());
        }
        Object[][] payload = values.stream()
            .map(x -> new Object[] { x })
            .toArray(Object[][]::new);
        Object result = executeAggregation(type, payload);
        Object[] expected = values.stream().distinct().toArray(Object[]::new);
        assertThat((List<Object>) result).containsExactlyInAnyOrder(expected);
    }

    @Test
    public void test_collect_set_on_primitive_types() throws Exception {
        for (var type : DataTypes.PRIMITIVE_TYPES) {
            assert_collect_set_on_primitive_type(type);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testNullValue() throws Exception {
        assertThat((List<Object>) executeAggregation(DataTypes.STRING, new Object[][]{{"Youri"}, {"Ruben"}, {null}}))
            .as("null values currently ignored")
            .containsExactlyInAnyOrder("Youri", "Ruben");
    }
}
