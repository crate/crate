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

import static io.crate.testing.Asserts.assertThat;

import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import io.crate.data.Input;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Scalar;
import io.crate.metadata.SearchPath;
import io.crate.metadata.functions.Signature;
import io.crate.operation.aggregation.AggregationTestCase;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class CollectSetAggregationTest extends AggregationTestCase {

    private Object executeAggregation(DataType<?> argumentType, Object[][] data) throws Exception {
        return executeAggregation(
            Signature.aggregate(
                "collect_set",
                argumentType.getTypeSignature(),
                new ArrayType<>(argumentType).getTypeSignature()
            ).withFeature(Scalar.Feature.DETERMINISTIC),
            data,
            List.of()
        );
    }

    @Test
    public void testReturnType() throws Exception {
        FunctionImplementation collectSet = nodeCtx.functions().get(
            null, "collect_set", List.of(Literal.of(DataTypes.INTEGER, null)), SearchPath.pathWithPGCatalogAndDoc());
        assertThat(collectSet.boundSignature().returnType()).isEqualTo(new ArrayType<>(DataTypes.INTEGER));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testDouble() throws Exception {
        assertThat(
            (List<Object>) executeAggregation(DataTypes.DOUBLE, new Object[][]{{0.7d}, {0.3d}, {0.3d}}))
            .containsExactlyInAnyOrder(0.3d, 0.7d);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testLongSerialization() throws Exception {
        var impl = (AggregationFunction) nodeCtx.functions().get(
                null, "collect_set", List.of(Literal.of(DataTypes.LONG, null)),
            SearchPath.pathWithPGCatalogAndDoc());

        Object state = impl.newState(RAM_ACCOUNTING, Version.CURRENT, Version.CURRENT, memoryManager);

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        impl.partialType().streamer().writeValueTo(streamOutput, state);

        Object newState = impl.partialType().streamer().readValueFrom(streamOutput.bytes().streamInput());
        assertThat(newState).isEqualTo(state);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test_value_adding_and_removal() {
        var impl = (AggregationFunction<Object, Object>) nodeCtx.functions().get(
            null, "collect_set", List.of(Literal.of(DataTypes.LONG, null)), SearchPath.pathWithPGCatalogAndDoc());
        var aggregationFunction = (AggregationFunction<Object, Object>) impl.optimizeForExecutionAsWindowFunction();

        Object state = aggregationFunction.newState(RAM_ACCOUNTING, Version.CURRENT, Version.CURRENT, memoryManager);
        state = aggregationFunction.iterate(RAM_ACCOUNTING, memoryManager, state, Literal.of(10L));
        state = aggregationFunction.iterate(RAM_ACCOUNTING, memoryManager, state, Literal.of(10L));

        aggregationFunction.removeFromAggregatedState(RAM_ACCOUNTING, state, new Input[] { Literal.of(10L) });
        aggregationFunction.removeFromAggregatedState(RAM_ACCOUNTING, state, new Input[] { Literal.of(10L) });

        Object values = aggregationFunction.terminatePartial(RAM_ACCOUNTING, state);
        assertThat((List<Object>) values).isEmpty();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testFloat() throws Exception {
        assertThat((List<Object>) executeAggregation(DataTypes.FLOAT, new Object[][]{{0.7f}, {0.3f}, {0.3f}}))
            .containsExactlyInAnyOrder(0.3f, 0.7f);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testInteger() throws Exception {
        assertThat((List<Object>) executeAggregation(DataTypes.INTEGER, new Object[][]{{7}, {3}, {3}}))
            .containsExactlyInAnyOrder(3, 7);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testLong() throws Exception {
        assertThat((List<Object>) executeAggregation(DataTypes.LONG, new Object[][]{{7L}, {3L}, {3L}}))
            .containsExactlyInAnyOrder(3L, 7L);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testShort() throws Exception {
        assertThat((List<Object>) executeAggregation(
            DataTypes.SHORT, new Object[][]{{(short) 7}, {(short) 3}, {(short) 3}}))
            .containsExactlyInAnyOrder((short) 3, (short) 7);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testString() throws Exception {
        assertThat((List<Object>) executeAggregation(
            DataTypes.STRING, new Object[][]{{"Youri"}, {"Ruben"}, {"Ruben"}}))
            .containsExactlyInAnyOrder("Youri", "Ruben");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testBoolean() throws Exception {
        assertThat((List<Object>) executeAggregation(DataTypes.BOOLEAN, new Object[][]{{true}, {false}, {false}}))
            .containsExactlyInAnyOrder(true, false);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testNullValue() throws Exception {
        assertThat((List<Object>) executeAggregation(DataTypes.STRING, new Object[][]{{"Youri"}, {"Ruben"}, {null}}))
            .as("null values currently ignored")
            .containsExactlyInAnyOrder("Youri", "Ruben");
    }
}
