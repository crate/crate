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
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class FloatType extends DataType<Float> implements Streamer<Float>, FixedWidthType {

    public static final FloatType INSTANCE = new FloatType();
    public static final int ID = 7;
    private static final int FLOAT_SIZE = (int) RamUsageEstimator.shallowSizeOfInstance(Float.class);

    private FloatType() {
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.FLOAT;
    }

    @Override
    public String getName() {
        return "real";
    }

    @Override
    public Streamer<Float> streamer() {
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
        float val = ((Number) value).floatValue();
        if (Float.isInfinite(val)) {
            throw new IllegalArgumentException("float value out of range: " + value);
        }
        return val;
    }

    @Override
    public int compare(Float val1, Float val2) {
        return Float.compare(val1, val2);
    }

    @Override
    public Float readValueFrom(StreamInput in) throws IOException {
        return in.readBoolean() ? null : in.readFloat();
    }

    @Override
    public void writeValueTo(StreamOutput out, Float v) throws IOException {
        out.writeBoolean(v == null);
        if (v != null) {
            out.writeFloat(v);
        }
    }

    @Override
    public int fixedSize() {
        return FLOAT_SIZE;
    }
}
