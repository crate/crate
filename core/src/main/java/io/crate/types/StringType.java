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

import com.google.common.collect.Ordering;
import io.crate.Streamer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

public class StringType extends DataType<BytesRef> implements Streamer<BytesRef> {

    public static final int ID = Precedence.StringType;
    public static final StringType INSTANCE = new StringType();
    public static final BytesRef T = new BytesRef("t");
    public static final BytesRef F = new BytesRef("f");

    protected StringType() {
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public String getName() {
        return "string";
    }

    @Override
    public Streamer<BytesRef> streamer() {
        return this;
    }

    @Override
    public BytesRef value(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof BytesRef) {
            return (BytesRef) value;
        }
        if (value instanceof String) {
            return new BytesRef((String) value);
        }
        if (value instanceof Boolean) {
            if ((boolean) value) {
                return T;
            } else {
                return F;
            }
        }
        if (value instanceof Map || value.getClass().isArray()) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "Cannot cast %s to type string", value));
        }
        if (value instanceof TimeValue) {
            return new BytesRef(((TimeValue) value).getStringRep());
        }
        return new BytesRef(value.toString());
    }

    @Override
    public int compareValueTo(BytesRef val1, BytesRef val2) {
        return Ordering.natural().nullsFirst().compare(val1, val2);
    }

    @Override
    public BytesRef readValueFrom(StreamInput in) throws IOException {
        int length = in.readVInt() - 1;
        if (length == -1) {
            return null;
        }
        return in.readBytesRef(length);
    }

    @Override
    public void writeValueTo(StreamOutput out, Object v) throws IOException {
        // .writeBytesRef isn't used here because it will convert null values to empty bytesRefs
        // to distinguish between null and an empty bytesRef
        // 1 is always added to the length so that
        // 0 is null
        // 1 is 0
        // ...
        if (v == null) {
            out.writeVInt(0);
        } else {
            BytesRef bytesRef = (BytesRef) v;
            out.writeVInt(bytesRef.length + 1);
            out.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        }
    }
}
