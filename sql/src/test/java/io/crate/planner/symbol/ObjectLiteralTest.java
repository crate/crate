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

package io.crate.planner.symbol;

import com.google.common.collect.ImmutableMap;
import io.crate.DataType;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

public class ObjectLiteralTest {

    private static Map<String, Object> testMap = new HashMap<String, Object>(){{
        put("int", 1);
        put("boolean", false);
        put("double", 2.8d);
        put("list", Arrays.asList(1, 3, 4));
    }};

    private static Map<String, Object> testCompareMap = new HashMap<String, Object>(){{
        put("int", 2);
        put("boolean", true);
        put("double", 2.9d);
        put("list", Arrays.asList(9, 9, 9, 9));
    }};

    @Test
    public void testConvertToObject() {
        Literal literal = new ObjectLiteral(testMap);
        assertThat(literal.convertTo(DataType.OBJECT), is(literal));
    }

    @Test( expected = IllegalArgumentException.class)
    public void testConvertTo() {
        Literal literal = new ObjectLiteral(testMap);
        literal.convertTo(DataType.BOOLEAN);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConvertToLong() {
        Literal literal = new ObjectLiteral(testMap);
        literal.convertTo(DataType.LONG).value();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConvertToInteger() {
        Literal literal = new ObjectLiteral(testMap);
        literal.convertTo(DataType.INTEGER).value();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConvertToDouble() {
        Literal literal = new ObjectLiteral(testMap);
        literal.convertTo(DataType.DOUBLE).value();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConvertToFloat() {
        Literal literal = new ObjectLiteral(testMap);
        literal.convertTo(DataType.FLOAT).value();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConvertToShort() {
        Literal literal = new ObjectLiteral(testMap);
        literal.convertTo(DataType.SHORT).value();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConvertToByte() {
        Literal literal = new ObjectLiteral(testMap);
        literal.convertTo(DataType.BYTE).value();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConvertToTimestamp() {
        Literal literal = new ObjectLiteral(testMap);
        literal.convertTo(DataType.TIMESTAMP).value();
    }

    @Test
    public void testCompareTo() {
        ObjectLiteral literal1 = new ObjectLiteral(testMap);
        ObjectLiteral literal2 = new ObjectLiteral(ImmutableMap.copyOf(testMap));
        ObjectLiteral literal3 = new ObjectLiteral(ImmutableMap.<String, Object>of());
        ObjectLiteral literal4 = new ObjectLiteral(testCompareMap);

        assertThat(literal1.compareTo(literal2), is(0));
        assertThat(literal2.compareTo(literal1), is(0));

        // first number of argument is checked
        assertThat(literal1.compareTo(literal3), is(1));
        assertThat(literal3.compareTo(literal1), is(-1));

        // then values
        assertThat(literal1.compareTo(literal4), is(1));
        assertThat(literal4.compareTo(literal1), is(1)); // not transitive as Object is not comparable
    }
}
