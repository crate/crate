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

package io.crate;

import org.elasticsearch.test.ESTestCase;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataTypeTest extends ESTestCase {

    @Test
    public void testStreaming() throws Exception {
        String s1 = "hello";
        BytesStreamOutput out = new BytesStreamOutput();
        Streamer streamer = DataTypes.STRING.streamer();
        streamer.writeValueTo(out, s1);
        StreamInput in = out.bytes().streamInput();
        String b2 = (String) streamer.readValueFrom(in);
        assertEquals(s1, b2);
    }

    @Test
    public void testStreamingNull() throws Exception {
        String s1 = null;
        BytesStreamOutput out = new BytesStreamOutput();
        Streamer streamer = DataTypes.STRING.streamer();
        streamer.writeValueTo(out, s1);
        StreamInput in = out.bytes().streamInput();
        String s2 = (String) streamer.readValueFrom(in);
        assertNull(s2);
    }

    @Test
    public void testForValueWithList() {
        List<String> strings = Arrays.asList("foo", "bar");
        DataType dataType = DataTypes.guessType(strings);
        assertEquals(dataType, new ArrayType(DataTypes.STRING));

        List<Integer> integers = Arrays.asList(1, 2, 3);
        dataType = DataTypes.guessType(integers);
        assertEquals(dataType, new ArrayType(DataTypes.INTEGER));
    }

    @Test
    public void testForValueWithArray() {
        Boolean[] booleans = new Boolean[]{true, false};
        DataType dataType = DataTypes.guessType(booleans);
        assertEquals(dataType, new ArrayType(DataTypes.BOOLEAN));
    }

    @Test
    public void testForValueWithTimestampArrayAsString() {
        String[] strings = {"2013-09-10T21:51:43", "2013-11-10T21:51:43"};
        DataType dataType = DataTypes.guessType(strings);
        assertEquals(dataType, new ArrayType(DataTypes.STRING));
    }

    @Test
    public void testForValueWithObjectList() {
        Map<String, Object> objA = new HashMap<>();
        objA.put("a", 1);

        Map<String, Object> objB = new HashMap<>();
        Map<String, Object> objBNested = new HashMap<>();

        objB.put("b", objBNested);
        objBNested.put("bn1", 1);
        objBNested.put("bn2", 2);

        List<Object> objects = List.of(objA, objB);
        DataType<?> dataType = DataTypes.guessType(objects);
        assertEquals(dataType, new ArrayType<>(DataTypes.UNTYPED_OBJECT));
    }

    @Test
    public void testForValueWithArrayWithNullValues() {
        DataType dataType = DataTypes.guessType(new String[]{"foo", null, "bar"});
        assertEquals(dataType, new ArrayType(DataTypes.STRING));
    }

    @Test
    public void testForValueNestedList() {
        List<List<String>> nestedStrings = Arrays.asList(
            Arrays.asList("foo", "bar"),
            Arrays.asList("f", "b"));
        assertEquals(new ArrayType(new ArrayType(DataTypes.STRING)), DataTypes.guessType(nestedStrings));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testForValueMixedDataTypeInList() {
        List<Object> objects = Arrays.<Object>asList("foo", 1);
        DataTypes.guessType(objects);
    }

    @Test
    public void testListCanContainMixedTypeIfSafeCastIsPossible() {
        List<Object> objects = Arrays.asList(1, null, 1.2f, 0);
        Collections.shuffle(objects);
        DataType<?> dataType = DataTypes.guessType(objects);
        assertThat(dataType, Matchers.is(new ArrayType(DataTypes.FLOAT)));
    }

    @Test
    public void testForValueWithEmptyList() {
        List<Object> objects = Arrays.<Object>asList();
        DataType type = DataTypes.guessType(objects);
        assertEquals(type, new ArrayType(DataTypes.UNDEFINED));
    }
}
