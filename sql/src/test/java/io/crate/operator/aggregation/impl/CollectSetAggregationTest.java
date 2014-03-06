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

package io.crate.operator.aggregation.impl;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.operator.aggregation.AggregationTest;
import io.crate.DataType;
import org.junit.Test;

import java.util.Set;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.*;

public class CollectSetAggregationTest extends AggregationTest {

    private Object[][] executeAggregation(DataType dataType, Object[][] data) throws Exception {
        return executeAggregation("collect_set", dataType, data);
    }

    @Test
    public void testReturnType() throws Exception {
        FunctionIdent fi = new FunctionIdent("collect_set", ImmutableList.of(DataType.INTEGER));
        assertEquals(DataType.INTEGER, functions.get(fi).info().returnType());
    }

    @Test
    public void testDouble() throws Exception {
        Object[][] result = executeAggregation(DataType.DOUBLE, new Object[][]{{0.7d}, {0.3d}, {0.3d}});

        assertThat(result[0][0], instanceOf(Set.class));
        assertEquals(2, ((Set)result[0][0]).size());
        assertTrue(((Set)result[0][0]).contains(0.7d));
    }

    @Test
    public void testFloat() throws Exception {
        Object[][] result = executeAggregation(DataType.FLOAT, new Object[][]{{0.7f}, {0.3f}, {0.3f}});

        assertThat(result[0][0], instanceOf(Set.class));
        assertEquals(2, ((Set)result[0][0]).size());
        assertTrue(((Set)result[0][0]).contains(0.7f));
    }

    @Test
    public void testInteger() throws Exception {
        Object[][] result = executeAggregation(DataType.INTEGER, new Object[][]{{7}, {3}, {3}});

        assertThat(result[0][0], instanceOf(Set.class));
        assertEquals(2, ((Set)result[0][0]).size());
        assertTrue(((Set)result[0][0]).contains(7));
    }

    @Test
    public void testLong() throws Exception {
        Object[][] result = executeAggregation(DataType.LONG, new Object[][]{{7L}, {3L}, {3L}});

        assertThat(result[0][0], instanceOf(Set.class));
        assertEquals(2, ((Set)result[0][0]).size());
        assertTrue(((Set)result[0][0]).contains(7L));
    }

    @Test
    public void testShort() throws Exception {
        Object[][] result = executeAggregation(DataType.SHORT, new Object[][]{{(short) 7}, {(short) 3}, {(short) 3}});

        assertThat(result[0][0], instanceOf(Set.class));
        assertEquals(2, ((Set)result[0][0]).size());
        assertTrue(((Set)result[0][0]).contains((short)7));
    }

    @Test
    public void testString() throws Exception {
        Object[][] result = executeAggregation(DataType.STRING, new Object[][]{{"Youri"}, {"Ruben"}, {"Ruben"}});

        assertThat(result[0][0], instanceOf(Set.class));
        assertEquals(2, ((Set)result[0][0]).size());
        assertTrue(((Set)result[0][0]).contains("Youri"));
    }

    @Test
    public void testBoolean() throws Exception {
        Object[][] result = executeAggregation(DataType.BOOLEAN, new Object[][]{{true}, {false}, {false}});

        assertThat(result[0][0], instanceOf(Set.class));
        assertEquals(2, ((Set)result[0][0]).size());
        assertTrue(((Set)result[0][0]).contains(true));
    }

    @Test
    public void testNullValue() throws Exception {
        Object[][] result = executeAggregation(DataType.STRING, new Object[][]{{"Youri"}, {"Ruben"}, {null}});
        // null values currently ignored
        assertThat(result[0][0], instanceOf(Set.class));
        assertEquals(2, ((Set)result[0][0]).size());
        assertFalse(((Set)result[0][0]).contains(null));
    }

}
