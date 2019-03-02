/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.types;

import com.google.common.collect.Lists;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ObjectTypeTest extends CrateUnitTest {

    @Test
    public void testStreamingWithoutInnerTypes() throws IOException {
        ObjectType type = ObjectType.untyped();
        BytesStreamOutput out = new BytesStreamOutput();
        type.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ObjectType otherType = ObjectType.untyped();
        otherType.readFrom(in);

        assertThat(otherType.innerTypes().size(), is(0));
    }

    @Test
    public void testStreamingWithEmptyInnerTypes() throws IOException {
        ObjectType type = ObjectType.builder().build();
        BytesStreamOutput out = new BytesStreamOutput();
        type.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ObjectType otherType = ObjectType.untyped();
        otherType.readFrom(in);

        assertThat(otherType.innerTypes(), is(type.innerTypes()));
    }

    @Test
    public void testStreamingWithInnerTypes() throws IOException {
        ObjectType type = ObjectType.builder()
            .setInnerType("s", DataTypes.STRING)
            .setInnerType("obj_array", new ArrayType(ObjectType.builder()
                .setInnerType("i", DataTypes.INTEGER)
                .build()))
            .build();
        BytesStreamOutput out = new BytesStreamOutput();
        type.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ObjectType otherType = ObjectType.untyped();
        otherType.readFrom(in);

        assertThat(otherType.innerTypes(), is(type.innerTypes()));
    }

    @Test
    public void testStreamingOfNullValueWithoutInnerTypes() throws IOException {
        ObjectType type = ObjectType.untyped();
        BytesStreamOutput out = new BytesStreamOutput();

        type.writeValueTo(out, null);

        StreamInput in = out.bytes().streamInput();
        ObjectType otherType = ObjectType.untyped();

        Object v = otherType.readValueFrom(in);

        assertThat(v, nullValue());
    }

    @Test
    public void testStreamingOfNullValueWithInnerTypes() throws IOException {
        ObjectType type = ObjectType.builder()
            .setInnerType("s", DataTypes.STRING)
            .setInnerType("obj_array", new ArrayType(ObjectType.builder()
                .setInnerType("i", DataTypes.INTEGER)
                .build()))
            .build();
        BytesStreamOutput out = new BytesStreamOutput();

        type.writeTo(out);
        type.writeValueTo(out, null);

        StreamInput in = out.bytes().streamInput();
        ObjectType otherType = ObjectType.untyped();
        otherType.readFrom(in);

        Object v = otherType.readValueFrom(in);

        assertThat(v, nullValue());
    }

    @Test
    public void testStreamingOfValueWithInnerTypes() throws IOException {
        ObjectType type = ObjectType.builder()
            .setInnerType("s", DataTypes.STRING)
            .setInnerType("obj_array", new ArrayType(ObjectType.builder()
                .setInnerType("i", DataTypes.INTEGER)
                .build()))
            .build();
        BytesStreamOutput out = new BytesStreamOutput();

        ArrayList innerArray = Lists.newArrayList(MapBuilder.newMapBuilder()
            .put("i", 1)
            .map());
        HashMap<String, Object> map = new HashMap<>();
        map.put("s", "foo");
        map.put("obj_array", innerArray);
        type.writeTo(out);
        type.writeValueTo(out,  map);

        StreamInput in = out.bytes().streamInput();
        ObjectType otherType = ObjectType.untyped();
        otherType.readFrom(in);

        Map<String, Object> v = otherType.readValueFrom(in);

        assertThat(v.get("s"), is(map.get("s")));
        assertThat(Objects.deepEquals(v.get("obj_array"), innerArray.toArray(new Object[0])), is(true));
    }

    @Test
    public void testStreamingOfValueWithoutInnerTypes() throws IOException {
        ObjectType type = ObjectType.untyped();
        BytesStreamOutput out = new BytesStreamOutput();

        ArrayList innerArray = Lists.newArrayList(MapBuilder.newMapBuilder()
            .put("i", 1)
            .map());
        HashMap<String, Object> map = new HashMap<>();
        map.put("s", "foo");
        map.put("obj_array", innerArray);
        type.writeTo(out);
        type.writeValueTo(out,  map);

        StreamInput in = out.bytes().streamInput();
        ObjectType otherType = ObjectType.untyped();
        otherType.readFrom(in);

        Map<String, Object> v = otherType.readValueFrom(in);

        assertThat(v.get("s"), is(map.get("s")));
        assertThat(Objects.deepEquals(v.get("obj_array"), innerArray), is(true));
    }
}
