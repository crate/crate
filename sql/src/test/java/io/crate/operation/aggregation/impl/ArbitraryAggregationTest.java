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
import io.crate.operation.aggregation.AggregationTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import static org.hamcrest.Matchers.isOneOf;

public class ArbitraryAggregationTest extends AggregationTest {

    private Object[][] executeAggregation(DataType dataType, Object[][] data) throws Exception {
        return executeAggregation("arbitrary", dataType, data);
    }

    @Test
    public void testReturnType() throws Exception {
        assertEquals(DataTypes.INTEGER,
            getFunction("arbitrary", ImmutableList.of(DataTypes.INTEGER)).info().returnType());
    }

    @Test
    public void testDouble() throws Exception {
        Object[][] data = new Object[][]{{0.8d}, {0.3d}};
        Object[][] result = executeAggregation(DataTypes.DOUBLE, data);

        assertThat(result[0][0], isOneOf(data[0][0], data[1][0]));
    }

    @Test
    public void testFloat() throws Exception {
        Object[][] data = new Object[][]{{0.8f}, {0.3f}};
        Object[][] result = executeAggregation(DataTypes.FLOAT, data);

        assertThat(result[0][0], isOneOf(data[0][0], data[1][0]));
    }

    @Test
    public void testInteger() throws Exception {
        Object[][] data = new Object[][]{{8}, {3}};
        Object[][] result = executeAggregation(DataTypes.INTEGER, data);

        assertThat(result[0][0], isOneOf(data[0][0], data[1][0]));
    }

    @Test
    public void testLong() throws Exception {
        Object[][] data = new Object[][]{{8L}, {3L}};
        Object[][] result = executeAggregation(DataTypes.LONG, data);

        assertThat(result[0][0], isOneOf(data[0][0], data[1][0]));
    }

    @Test
    public void testShort() throws Exception {
        Object[][] data = new Object[][]{{(short) 8}, {(short) 3}};
        Object[][] result = executeAggregation(DataTypes.SHORT, data);

        assertThat(result[0][0], isOneOf(data[0][0], data[1][0]));
    }

    @Test
    public void testString() throws Exception {
        Object[][] data = new Object[][]{{new BytesRef("Youri")}, {new BytesRef("Ruben")}};
        Object[][] result = executeAggregation(DataTypes.STRING, data);

        assertThat(result[0][0], isOneOf(data[0][0], data[1][0]));
    }

    @Test
    public void testBoolean() throws Exception {
        Object[][] data = new Object[][]{{true}, {false}};
        Object[][] result = executeAggregation(DataTypes.BOOLEAN, data);

        assertThat(result[0][0], isOneOf(data[0][0], data[1][0]));
    }

    @Test(expected = NullPointerException.class)
    public void testUnsupportedType() throws Exception {
        Object[][] result = executeAggregation(DataTypes.OBJECT, new Object[][]{{new Object()}});
    }
}
