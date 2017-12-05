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

package io.crate.operation.aggregation.impl;

import com.google.common.collect.ImmutableList;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.operation.aggregation.AggregationTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.SetType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.junit.Test;

import java.util.Set;

import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class CollectSetAggregationTest extends AggregationTest {

    private Object[][] executeAggregation(DataType dataType, Object[][] data) throws Exception {
        return executeAggregation("collect_set", dataType, data);
    }

    @Test
    public void testReturnType() throws Exception {
        assertEquals(new SetType(DataTypes.INTEGER),
            functions.getBuiltin("collect_set", ImmutableList.of(DataTypes.INTEGER)).info().returnType());
    }

    @Test
    public void testDouble() throws Exception {
        Object[][] result = executeAggregation(DataTypes.DOUBLE, new Object[][]{{0.7d}, {0.3d}, {0.3d}});

        assertThat(result[0][0], instanceOf(Set.class));
        assertEquals(2, ((Set) result[0][0]).size());
        assertTrue(((Set) result[0][0]).contains(0.7d));
    }

    @Test
    public void testLongSerialization() throws Exception {
        AggregationFunction impl
            = (AggregationFunction) functions.getBuiltin("collect_set", ImmutableList.of(DataTypes.LONG));

        Object state = impl.newState(ramAccountingContext, Version.CURRENT, BigArrays.NON_RECYCLING_INSTANCE);

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        impl.partialType().streamer().writeValueTo(streamOutput, state);

        Object newState = impl.partialType().streamer().readValueFrom(streamOutput.bytes().streamInput());
        assertEquals(state, newState);
    }

    @Test
    public void testFloat() throws Exception {
        Object[][] result = executeAggregation(DataTypes.FLOAT, new Object[][]{{0.7f}, {0.3f}, {0.3f}});

        assertThat(result[0][0], instanceOf(Set.class));
        assertEquals(2, ((Set) result[0][0]).size());
        assertTrue(((Set) result[0][0]).contains(0.7f));
    }

    @Test
    public void testInteger() throws Exception {
        Object[][] result = executeAggregation(DataTypes.INTEGER, new Object[][]{{7}, {3}, {3}});

        assertThat(result[0][0], instanceOf(Set.class));
        assertEquals(2, ((Set) result[0][0]).size());
        assertTrue(((Set) result[0][0]).contains(7));
    }

    @Test
    public void testLong() throws Exception {
        Object[][] result = executeAggregation(DataTypes.LONG, new Object[][]{{7L}, {3L}, {3L}});

        assertThat(result[0][0], instanceOf(Set.class));
        assertEquals(2, ((Set) result[0][0]).size());
        assertTrue(((Set) result[0][0]).contains(7L));
    }

    @Test
    public void testShort() throws Exception {
        Object[][] result = executeAggregation(DataTypes.SHORT, new Object[][]{{(short) 7}, {(short) 3}, {(short) 3}});

        assertThat(result[0][0], instanceOf(Set.class));
        assertEquals(2, ((Set) result[0][0]).size());
        assertTrue(((Set) result[0][0]).contains((short) 7));
    }

    @Test
    public void testString() throws Exception {
        Object[][] result = executeAggregation(DataTypes.STRING,
            new Object[][]{{new BytesRef("Youri")}, {new BytesRef("Ruben")}, {new BytesRef("Ruben")}});

        assertThat(result[0][0], instanceOf(Set.class));
        assertEquals(2, ((Set) result[0][0]).size());
        assertTrue(((Set) result[0][0]).contains(new BytesRef("Youri")));
    }

    @Test
    public void testBoolean() throws Exception {
        Object[][] result = executeAggregation(DataTypes.BOOLEAN, new Object[][]{{true}, {false}, {false}});

        assertThat(result[0][0], instanceOf(Set.class));
        assertEquals(2, ((Set) result[0][0]).size());
        assertTrue(((Set) result[0][0]).contains(true));
    }

    @Test
    public void testNullValue() throws Exception {
        Object[][] result = executeAggregation(DataTypes.STRING,
            new Object[][]{{new BytesRef("Youri")}, {new BytesRef("Ruben")}, {null}});
        // null values currently ignored
        assertThat(result[0][0], instanceOf(Set.class));
        assertEquals(2, ((Set) result[0][0]).size());
        assertFalse(((Set) result[0][0]).contains(null));
    }
}
