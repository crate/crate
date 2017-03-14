/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.aggregation.impl;

import com.google.common.collect.ImmutableList;
import io.crate.Streamer;
import io.crate.operation.aggregation.AggregationTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isLiteral;


public class CountAggregationTest extends AggregationTest {

    private Object[][] executeAggregation(DataType dataType, Object[][] data) throws Exception {
        return executeAggregation("count", dataType, data);
    }

    @Test
    public void testReturnType() throws Exception {
        // Return type is fixed to Long
        assertEquals(DataTypes.LONG,
            getFunction("count", ImmutableList.of(DataTypes.INTEGER)).info().returnType());
    }

    @Test
    public void testDouble() throws Exception {
        Object[][] result = executeAggregation(DataTypes.DOUBLE, new Object[][]{{0.7d}, {0.3d}});

        assertEquals(2L, result[0][0]);
    }

    @Test
    public void testFloat() throws Exception {
        Object[][] result = executeAggregation(DataTypes.FLOAT, new Object[][]{{0.7f}, {0.3f}});

        assertEquals(2L, result[0][0]);
    }

    @Test
    public void testInteger() throws Exception {
        Object[][] result = executeAggregation(DataTypes.INTEGER, new Object[][]{{7}, {3}});

        assertEquals(2L, result[0][0]);
    }

    @Test
    public void testLong() throws Exception {
        Object[][] result = executeAggregation(DataTypes.LONG, new Object[][]{{7L}, {3L}});

        assertEquals(2L, result[0][0]);
    }

    @Test
    public void testShort() throws Exception {
        Object[][] result = executeAggregation(DataTypes.SHORT, new Object[][]{{(short) 7}, {(short) 3}});

        assertEquals(2L, result[0][0]);
    }

    @Test
    public void testString() throws Exception {
        Object[][] result = executeAggregation(DataTypes.STRING, new Object[][]{{"Youri"}, {"Ruben"}});

        assertEquals(2L, result[0][0]);
    }

    @Test
    public void testNormalizeWithNullLiteral() {
        assertThat(normalize("count", null, DataTypes.STRING), isLiteral(0L));
        assertThat(normalize("count", null, DataTypes.UNDEFINED), isLiteral(0L));
    }

    @Test
    public void testNoInput() throws Exception {
        // aka. COUNT(*)
        Object[][] result = executeAggregation(null, new Object[][]{{}, {}});
        assertEquals(2L, result[0][0]);
    }

    @Test
    public void testStreaming() throws Exception {
        CountAggregation.LongState l1 = new CountAggregation.LongState(12345L);
        BytesStreamOutput out = new BytesStreamOutput();
        Streamer streamer = CountAggregation.LongStateType.INSTANCE.streamer();
        streamer.writeValueTo(out, l1);
        StreamInput in = StreamInput.wrap(out.bytes());
        CountAggregation.LongState l2 = (CountAggregation.LongState) streamer.readValueFrom(in);
        assertEquals(l1.value, l2.value);
    }
}
