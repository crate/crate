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
import io.crate.TimestampFormat;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class TimestampType extends DataType<Long> implements FixedWidthType, Streamer<Long> {

    public static final TimestampType INSTANCE = new TimestampType();
    public static final int ID = 11;

    private TimestampType() {
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.LongType;
    }

    @Override
    public String getName() {
        return "timestamp with time zone";
    }

    @Override
    public Streamer<Long> streamer() {
        return this;
    }

    @Override
    public Long value(Object value) throws ClassCastException {
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return valueFromString((String) value);
        }
        // we treat float and double values as seconds with milliseconds as fractions
        // see timestamp documentation
        if (value instanceof Double) {
            return ((Number) (((Double) value) * 1000)).longValue();
        }
        if (value instanceof Float) {
            return ((Number) (((Float) value) * 1000)).longValue();
        }
        if (!(value instanceof Long)) {
            return ((Number) value).longValue();
        }
        return (Long) value;
    }

    @Override
    public int compareValueTo(Long val1, Long val2) {
        return nullSafeCompareValueTo(val1, val2, Long::compare);
    }

    private Long valueFromString(String s) {
        try {
            return Long.valueOf(s);
        } catch (NumberFormatException e) {
            return TimestampFormat.parseTimestampString(s);
        }
    }

    @Override
    public Long readValueFrom(StreamInput in) throws IOException {
        return in.readBoolean() ? null : in.readLong();
    }

    @Override
    public void writeValueTo(StreamOutput out, Long v) throws IOException {
        out.writeBoolean(v == null);
        if (v != null) {
            out.writeLong(v);
        }
    }

    @Override
    public int fixedSize() {
        return 16; // 8 object overhead, 8 long
    }
}
