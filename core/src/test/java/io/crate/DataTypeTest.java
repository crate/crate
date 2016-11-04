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

package io.crate;

import io.crate.test.integration.CrateUnitTest;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataTypeTest extends CrateUnitTest {

    @Test
    public void testStreaming() throws Exception {
        BytesRef b1 = new BytesRef("hello");
        BytesStreamOutput out = new BytesStreamOutput();
        Streamer streamer = DataTypes.STRING.streamer();
        streamer.writeValueTo(out, b1);
        StreamInput in = out.bytes().streamInput();
        BytesRef b2 = (BytesRef) streamer.readValueFrom(in);
        assertEquals(b1, b2);
    }

    @Test
    public void testStreamingNull() throws Exception {
        BytesRef b1 = null;
        BytesStreamOutput out = new BytesStreamOutput();
        Streamer streamer = DataTypes.STRING.streamer();
        streamer.writeValueTo(out, b1);
        StreamInput in = out.bytes().streamInput();
        BytesRef b2 = (BytesRef) streamer.readValueFrom(in);
        assertNull(b2);
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

        List<Object> objects = Arrays.<Object>asList(objA, objB);
        DataType dataType = DataTypes.guessType(objects);
        assertEquals(dataType, new ArrayType(DataTypes.OBJECT));
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
    public void testForValueWithEmptyList() {
        List<Object> objects = Arrays.<Object>asList();
        DataType type = DataTypes.guessType(objects);
        assertEquals(type, new ArrayType(DataTypes.UNDEFINED));
    }
}
