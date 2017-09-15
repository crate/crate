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

public class ShortType extends DataType<Short> implements Streamer<Short>, FixedWidthType {

    public static final ShortType INSTANCE = new ShortType();
    public static final int ID = Precedence.ShortType;

    private ShortType() {
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public String getName() {
        return "short";
    }

    @Override
    public Streamer<?> streamer() {
        return this;
    }

    @Override
    public Short value(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Short) {
            return (Short) value;
        }
        if (value instanceof String) {
            return Short.valueOf((String) value);
        }
        if (value instanceof BytesRef) {
            return Short.valueOf(((BytesRef) value).utf8ToString());
        }
        int intVal = ((Number) value).intValue();
        if (intVal < Short.MIN_VALUE || Short.MAX_VALUE < intVal) {
            throw new IllegalArgumentException("short value out of range: " + intVal);
        }
        return ((Number) value).shortValue();
    }

    @Override
    public int compareValueTo(Short val1, Short val2) {
        return nullSafeCompareValueTo(val1, val2, Short::compare);
    }

    @Override
    public Short readValueFrom(StreamInput in) throws IOException {
        return in.readBoolean() ? null : in.readShort();
    }

    @Override
    public void writeValueTo(StreamOutput out, Object v) throws IOException {
        out.writeBoolean(v == null);
        if (v != null) {
            out.writeShort(((Number) v).shortValue());
        }
    }

    @Override
    public int fixedSize() {
        return 16; // object overhead + 2 byte for short + 6 byte padding
    }
}

