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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import io.crate.testing.DataTypeTesting;

public class ArrayTypeTest extends DataTypeTestCase<List<Object>> {

    @Override
    @SuppressWarnings("unchecked")
    public DataType<List<Object>> getType() {
        // we don't support arrays of float vectors
        // TODO: maybe make this a property of the DataType itself rather than a check in DataTypeAnalyzer?
        DataType<Object> randomType = (DataType<Object>) DataTypeTesting.randomTypeExcluding(
            Set.of(FloatVectorType.INSTANCE_ONE)
        );
        return new ArrayType<>(randomType);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected DataDef<List<Object>> getDataDef() {
        // Exclude float vectors and objects - we don't support arrays of float vector,
        // and arrays of objects aren't currently handled by data generation code; these
        // are tested in ObjectTypeTest instead.
        DataType<Object> randomType = (DataType<Object>) DataTypeTesting.randomTypeExcluding(
            Set.of(FloatVectorType.INSTANCE_ONE, ObjectType.UNTYPED)
        );
        DataType<List<Object>> type = new ArrayType<>(randomType);
        return new DataDef<>(type, type.getTypeSignature().toString(), DataTypeTesting.getDataGenerator(type));
    }

    @Test
    public void test_pg_string_array_literal_can_be_converted_to_values() {
        ArrayType<String> strArray = new ArrayType<>(DataTypes.STRING);
        List<String> values = strArray.implicitCast("{a,abc,A,ABC,null,\"null\",NULL,\"NULL\"}");
        assertThat(values).containsExactly("a", "abc", "A", "ABC", null, "null", null, "NULL");
    }

    @Test
    public void testNullValues() throws Exception {
        ArrayType<String> arrayType = new ArrayType<>(StringType.INSTANCE);
        var streamer = arrayType.streamer();

        BytesStreamOutput out = new BytesStreamOutput();

        streamer.writeValueTo(out, null);

        StreamInput in = out.bytes().streamInput();
        assertThat(streamer.readValueFrom(in)).isNull();

        out.reset();
        List<String> listWithNullItem = new ArrayList<>();
        listWithNullItem.add(null);
        streamer.writeValueTo(out, listWithNullItem);

        in = out.bytes().streamInput();
        assertThat(streamer.readValueFrom(in)).isEqualTo(listWithNullItem);
    }

    @Test
    public void test_compare_arrays_of_string_that_contain_nulls() {
        int cmp = DataTypes.STRING_ARRAY.compare(
            DataTypes.STRING_ARRAY.sanitizeValue("{'a', null}"),
            DataTypes.STRING_ARRAY.sanitizeValue("{'a', 'b'}")
        );
        assertThat(cmp).isEqualTo(-1);
    }

    @Test
    public void test_as_array_of_string_with_null_values() {
        var array = new ArrayList<>();
        array.add(null);
        array.add("string");
        assertThat(DataTypes.STRING_ARRAY.fromAnyArray(array)).isEqualTo(array);
    }

    @Test
    public void test_from_any_array_with_nested_into_array_objects_and_arrays() {
        assertThat(
            DataTypes.STRING_ARRAY.fromAnyArray(
                List.of(
                    List.of(Map.of("key", "value")),
                    List.of(List.of("nested"))
                )
            )
        ).isEqualTo(List.of("[{\"key\":\"value\"}]", "[[\"nested\"]]"));
    }

    @Test
    public void test_from_any_array_with_nested_into_object_arrays_and_objects() {
        assertThat(
            DataTypes.STRING_ARRAY.fromAnyArray(
                List.of(
                    Map.of("key1", List.of("test")),
                    Map.of("key2", Map.of("key", "value"))
                )
            )
        ).isEqualTo(List.of("{\"key1\":[\"test\"]}", "{\"key2\":{\"key\":\"value\"}}"));
    }
}
