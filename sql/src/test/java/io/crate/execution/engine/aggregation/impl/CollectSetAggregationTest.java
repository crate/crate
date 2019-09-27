/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.collect.ImmutableList;
import io.crate.data.Input;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.SearchPath;
import io.crate.operation.aggregation.AggregationTest;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class CollectSetAggregationTest extends AggregationTest {

    private Object executeAggregation(DataType dataType, Object[][] data) throws Exception {
        return executeAggregation("collect_set", dataType, data);
    }

    @Test
    public void testReturnType() throws Exception {
        FunctionImplementation collectSet = functions.get(
            null, "collect_set", ImmutableList.of(Literal.of(DataTypes.INTEGER, null)), SearchPath.pathWithPGCatalogAndDoc());
        assertEquals(new ArrayType<>(DataTypes.INTEGER), collectSet.info().returnType());
    }

    @Test
    public void testDouble() throws Exception {
        assertThat(
            (List<Object>) executeAggregation(DataTypes.DOUBLE, new Object[][]{{0.7d}, {0.3d}, {0.3d}}),
            is(Matchers.containsInAnyOrder(0.3d, 0.7d)));
    }

    @Test
    public void testLongSerialization() throws Exception {
        AggregationFunction impl = (AggregationFunction) functions.get(
                null, "collect_set", ImmutableList.of(Literal.of(DataTypes.LONG, null)), SearchPath.pathWithPGCatalogAndDoc());

        Object state = impl.newState(ramAccountingContext, Version.CURRENT);

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        impl.partialType().streamer().writeValueTo(streamOutput, state);

        Object newState = impl.partialType().streamer().readValueFrom(streamOutput.bytes().streamInput());
        assertEquals(state, newState);
    }

    @Test
    public void test_value_adding_and_removal() {
        AggregationFunction impl = (AggregationFunction) functions.get(
            null, "collect_set", ImmutableList.of(Literal.of(DataTypes.LONG, null)), SearchPath.pathWithPGCatalogAndDoc());
        AggregationFunction aggregationFunction = impl.optimizeForExecutionAsWindowFunction();

        Object state = aggregationFunction.newState(
            ramAccountingContext, Version.CURRENT);
        state = aggregationFunction.iterate(ramAccountingContext, state, Literal.of(10));
        state = aggregationFunction.iterate(ramAccountingContext, state, Literal.of(10));

        aggregationFunction.removeFromAggregatedState(ramAccountingContext, state, new Input[] { Literal.of(10) });
        aggregationFunction.removeFromAggregatedState(ramAccountingContext, state, new Input[] { Literal.of(10) });

        Object values = aggregationFunction.terminatePartial(ramAccountingContext, state);
        assertThat((List<Object>) values, Matchers.empty());
    }

    @Test
    public void testFloat() throws Exception {
        assertThat((List<Object>) executeAggregation(DataTypes.FLOAT, new Object[][]{{0.7f}, {0.3f}, {0.3f}}),
                   is(containsInAnyOrder(0.3f, 0.7f)));
    }

    @Test
    public void testInteger() throws Exception {
        assertThat((List<Object>) executeAggregation(DataTypes.INTEGER, new Object[][]{{7}, {3}, {3}}),
                   is(containsInAnyOrder(3, 7)));
    }

    @Test
    public void testLong() throws Exception {
        assertThat((List<Object>) executeAggregation(DataTypes.LONG, new Object[][]{{7L}, {3L}, {3L}}),
                   is(containsInAnyOrder(3L, 7L)));
    }

    @Test
    public void testShort() throws Exception {
        assertThat((List<Object>) executeAggregation(DataTypes.SHORT,
                                                 new Object[][]{{(short) 7}, {(short) 3}, {(short) 3}}),
                   is(containsInAnyOrder((short) 3, (short) 7)));
    }

    @Test
    public void testString() throws Exception {
        assertThat((List<Object>) executeAggregation(DataTypes.STRING, new Object[][]{{"Youri"}, {"Ruben"}, {"Ruben"}}),
                   is(containsInAnyOrder("Youri", "Ruben")));
    }

    @Test
    public void testBoolean() throws Exception {
        assertThat((List<Object>) executeAggregation(DataTypes.BOOLEAN, new Object[][]{{true}, {false}, {false}}),
                   is(containsInAnyOrder(true, false)));
    }

    @Test
    public void testNullValue() throws Exception {
        assertThat("null values currently ignored",
                   (List<Object>) executeAggregation(DataTypes.STRING, new Object[][]{{"Youri"}, {"Ruben"}, {null}}),
                   is(containsInAnyOrder("Youri", "Ruben")));
    }
}
