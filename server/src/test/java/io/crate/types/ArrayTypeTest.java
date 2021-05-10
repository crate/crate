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

package io.crate.types;

import io.crate.Streamer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class ArrayTypeTest extends ESTestCase {

    @Test
    public void test_pg_string_array_literal_can_be_converted_to_values() {
        ArrayType<String> strArray = new ArrayType<>(DataTypes.STRING);
        List<String> values = strArray.implicitCast("{a,abc,A,ABC,null,\"null\",NULL,\"NULL\"}");
        assertThat(
            values,
            contains("a", "abc", "A", "ABC", null, "null", null, "NULL")
        );
    }

    @Test
    public void testArrayTypeSerialization() throws Exception {
        // nested string array: [ ["x"], ["y"] ]
        ArrayType<?> arrayType = new ArrayType<>(new ArrayType<>(StringType.INSTANCE));
        BytesStreamOutput out = new BytesStreamOutput();

        DataTypes.toStream(arrayType, out);

        StreamInput in = out.bytes().streamInput();

        DataType<?> readType = DataTypes.fromStream(in);
        assertThat(readType, instanceOf(ArrayType.class));

        ArrayType<?> readArrayType = (ArrayType<?>) readType;
        assertThat(readArrayType.innerType(), instanceOf(ArrayType.class));

        ArrayType<?> readInnerArrayType = (ArrayType<?>) readArrayType.innerType();
        assertThat(readInnerArrayType.innerType(), is(StringType.INSTANCE));
    }

    @Test
    public void testValueSerialization() throws Exception {
        ArrayType<String> arrayType = new ArrayType<>(StringType.INSTANCE);
        Streamer<List<String>> streamer = arrayType.streamer();
        List<String> serArray = Arrays.asList(
            "foo",
            "bar",
            "foobar"
        );
        BytesStreamOutput out = new BytesStreamOutput();
        streamer.writeValueTo(out, serArray);

        StreamInput in = out.bytes().streamInput();

        assertThat(streamer.readValueFrom(in), is(serArray));
    }

    @Test
    public void testNullValues() throws Exception {
        ArrayType<String> arrayType = new ArrayType<>(StringType.INSTANCE);
        var streamer = arrayType.streamer();

        BytesStreamOutput out = new BytesStreamOutput();

        streamer.writeValueTo(out, null);

        StreamInput in = out.bytes().streamInput();
        assertThat(streamer.readValueFrom(in), is(nullValue()));

        out.reset();
        List<String> listWithNullItem = new ArrayList<>();
        listWithNullItem.add(null);
        streamer.writeValueTo(out, listWithNullItem);

        in = out.bytes().streamInput();
        assertThat(streamer.readValueFrom(in), contains(nullValue()));
    }

    @Test
    public void test_compare_arrays_of_string_that_contain_nulls() {
        int cmp = DataTypes.STRING_ARRAY.compare(
            DataTypes.STRING_ARRAY.sanitizeValue("{'a', null}"),
            DataTypes.STRING_ARRAY.sanitizeValue("{'a', 'b'}")
        );
        assertThat(cmp, is(-1));
    }

    @Test
    public void test_as_array_of_string_with_null_values() {
        var array = new ArrayList<>();
        array.add(null);
        array.add("string");
        assertThat(DataTypes.STRING_ARRAY.fromAnyArray(array), is(array));
    }

    @Test
    public void test_from_any_array_with_nested_into_array_objects_and_arrays() {
        assertThat(
            DataTypes.STRING_ARRAY.fromAnyArray(
                List.of(
                    List.of(Map.of("key", "value")),
                    List.of(List.of("nested"))
                )
            ),
            is(List.of("[{\"key\":\"value\"}]", "[[\"nested\"]]"))
        );
    }

    @Test
    public void test_from_any_array_with_nested_into_object_arrays_and_objects() {
        assertThat(
            DataTypes.STRING_ARRAY.fromAnyArray(
                List.of(
                    Map.of("key1", List.of("test")),
                    Map.of("key2", Map.of("key", "value"))
                )
            ),
            is(List.of("{\"key1\":[\"test\"]}", "{\"key2\":{\"key\":\"value\"}}"))
        );
    }

}
