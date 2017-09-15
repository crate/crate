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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class ByteType extends DataType<Byte> implements Streamer<Byte>, FixedWidthType {

    public static final ByteType INSTANCE = new ByteType();
    public static final int ID = Precedence.ByteType;

    private ByteType() {
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public String getName() {
        return "byte";
    }

    @Override
    public Streamer<?> streamer() {
        return this;
    }

    @Override
    public Byte value(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return Byte.parseByte((String) value);
        }
        if (value instanceof BytesRef) {
            return Byte.parseByte(((BytesRef) value).utf8ToString());
        }
        Integer val = ((Number) value).intValue();
        if (val < Byte.MIN_VALUE || Byte.MAX_VALUE < val) {
            throw new IllegalArgumentException("byte value out of range: " + val);
        }
        return val.byteValue();
    }

    @Override
    public int compareValueTo(Byte val1, Byte val2) {
        return nullSafeCompareValueTo(val1, val2, Byte::compare);
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public Byte readValueFrom(StreamInput in) throws IOException {
        return in.readBoolean() ? null : in.readByte();
    }

    @Override
    public void writeValueTo(StreamOutput out, Object v) throws IOException {
        out.writeBoolean(v == null);
        if (v != null) {
            out.writeByte(((Number) v).byteValue());
        }
    }

    @Override
    public int fixedSize() {
        return 16; // object overhead + 1 byte + 7 byte padding
    }
}
