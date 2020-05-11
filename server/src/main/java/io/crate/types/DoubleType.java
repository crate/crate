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

public class DoubleType extends DataType<Double> implements FixedWidthType, Streamer<Double> {

    public static final DoubleType INSTANCE = new DoubleType();
    public static final int ID = 6;
    private static final int DOUBLE_SIZE = (int) RamUsageEstimator.shallowSizeOfInstance(Double.class);

    private DoubleType() {
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.DOUBLE;
    }

    @Override
    public String getName() {
        return "double precision";
    }

    @Override
    public Streamer<Double> streamer() {
        return this;
    }

    @Override
    public Double value(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Double) {
            return (Double) value;
        }
        if (value instanceof String) {
            return Double.valueOf((String) value);
        }
        return ((Number) value).doubleValue();
    }

    @Override
    public int compare(Double val1, Double val2) {
        return Double.compare(val1, val2);
    }

    @Override
    public Double readValueFrom(StreamInput in) throws IOException {
        return in.readBoolean() ? null : in.readDouble();
    }

    @Override
    public void writeValueTo(StreamOutput out, Double v) throws IOException {
        out.writeBoolean(v == null);
        if (v != null) {
            out.writeDouble(v);
        }
    }

    @Override
    public int fixedSize() {
        return DOUBLE_SIZE;
    }
}
