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

public class FloatType extends DataType<Float> implements Streamer<Float>, FixedWidthType {

    public static final FloatType INSTANCE = new FloatType();
    public static final int ID = Precedence.FloatType;

    private FloatType() {
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public String getName() {
        return "float";
    }

    @Override
    public Streamer<?> streamer() {
        return this;
    }

    @Override
    public Float value(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Float) {
            return (Float) value;
        }
        if (value instanceof String) {
            return Float.parseFloat((String) value);
        }
        if (value instanceof BytesRef) {
            return Float.parseFloat(((BytesRef) value).utf8ToString());
        }
        double doubleValue = ((Number) value).doubleValue();
        if (doubleValue < -Float.MAX_VALUE || Float.MAX_VALUE < doubleValue) {
            throw new IllegalArgumentException("float value out of range: " + doubleValue);
        }
        return ((Number) value).floatValue();
    }

    @Override
    public int compareValueTo(Float val1, Float val2) {
        return nullSafeCompareValueTo(val1, val2, Float::compare);
    }

    @Override
    public Float readValueFrom(StreamInput in) throws IOException {
        return in.readBoolean() ? null : in.readFloat();
    }

    @Override
    public void writeValueTo(StreamOutput out, Object v) throws IOException {
        out.writeBoolean(v == null);
        if (v != null) {
            out.writeFloat(((Number) v).floatValue());
        }
    }

    @Override
    public int fixedSize() {
        return 16; // object overhead + float
    }
}
