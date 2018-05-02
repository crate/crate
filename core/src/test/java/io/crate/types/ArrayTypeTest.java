/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import io.crate.test.integration.CrateUnitTest;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import static org.hamcrest.Matchers.array;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class ArrayTypeTest extends CrateUnitTest {

    @Test
    public void testArrayTypeSerialization() throws Exception {
        // nested string array: [ ["x"], ["y"] ]
        ArrayType arrayType = new ArrayType(new ArrayType(StringType.INSTANCE));
        BytesStreamOutput out = new BytesStreamOutput();

        DataTypes.toStream(arrayType, out);

        StreamInput in = out.bytes().streamInput();

        DataType readType = DataTypes.fromStream(in);
        assertThat(readType, instanceOf(ArrayType.class));

        ArrayType readArrayType = (ArrayType) readType;
        assertThat(readArrayType.innerType(), instanceOf(ArrayType.class));

        ArrayType readInnerArrayType = (ArrayType) readArrayType.innerType();
        assertThat(readInnerArrayType.innerType(), instanceOf(StringType.class));
        assertSame(readInnerArrayType.innerType(), StringType.INSTANCE);
    }

    @Test
    public void testValueSerialization() throws Exception {
        ArrayType arrayType = new ArrayType(StringType.INSTANCE);

        Streamer<?> streamer = arrayType.streamer();

        BytesRef[] serArray = new BytesRef[]{
            new BytesRef("foo"),
            new BytesRef("bar"),
            new BytesRef("foobar")
        };

        BytesStreamOutput out = new BytesStreamOutput();
        streamer.writeValueTo(out, serArray);

        StreamInput in = out.bytes().streamInput();

        assertThat(streamer.readValueFrom(in), is(serArray));
    }

    @Test
    public void testNullValues() throws Exception {
        ArrayType arrayType = new ArrayType(StringType.INSTANCE);

        Streamer<?> streamer = arrayType.streamer();

        BytesStreamOutput out = new BytesStreamOutput();

        streamer.writeValueTo(out, null);

        StreamInput in = out.bytes().streamInput();
        assertThat(streamer.readValueFrom(in), is(nullValue()));

        out.reset();
        Object[] nullArray = {null};
        streamer.writeValueTo(out, nullArray);

        in = out.bytes().streamInput();
        Object[] o = (Object[]) streamer.readValueFrom(in);
        assertThat(o, is(array(nullValue())));
    }
}
