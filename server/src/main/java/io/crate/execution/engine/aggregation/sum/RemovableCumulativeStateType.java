/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.engine.aggregation.sum;


import java.io.IOException;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jspecify.annotations.Nullable;

import io.crate.Streamer;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public final class RemovableCumulativeStateType<T> extends DataType<RemovableCumulativeState<T>>
        implements Streamer<RemovableCumulativeState<T>> {

    public static final int ID = 1029;

    @SuppressWarnings("rawtypes")
    public static final RemovableCumulativeStateType INSTANCE = new RemovableCumulativeStateType();

    @Override
    public int id() {
        return ID;
    }

    @Override
    protected Precedence precedence() {
        return Precedence.CUSTOM;

    }

    @Override
    public String getName() {
        return "sum_agg_state";
    }

    @Override
    public Streamer<RemovableCumulativeState<T>> streamer() {
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public RemovableCumulativeState<T> readValueFrom(StreamInput in) throws IOException {
        DataType<T> valueType = (DataType<T>) DataTypes.fromStream(in);
        long count = in.readVLong();
        T value = count == 0 ? null : valueType.streamer().readValueFrom(in);
        return new RemovableCumulativeState<>(value, valueType, count);
    }

    @Override
    public void writeValueTo(StreamOutput out, RemovableCumulativeState<T> v) throws IOException {
        DataTypes.toStream(v.valueType(), out);
        out.writeVLong(v.count());
        if (v.count() > 0) {
            v.valueType().streamer().writeValueTo(out, v.value());
        }
    }

    @Override
    public RemovableCumulativeState<T> sanitizeValue(Object value) {
        return (RemovableCumulativeState<T>) value;
    }

    @Override
    public long valueBytes(@Nullable RemovableCumulativeState<T> state) {
        if (state == null) {
            return RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        }
        return state.ramBytesUsed();
    }

    @Override
    public int compare(RemovableCumulativeState<T> state1, RemovableCumulativeState<T> state2) {
        if (state1 == null) {
            return state2 == null ? 0 : -1;
        }
        if (state2 == null) {
            return 1;
        }
        return state1.valueType().compare(state1.value(), state2.value());
    }
}
