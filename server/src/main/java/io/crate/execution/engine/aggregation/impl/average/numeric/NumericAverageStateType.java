/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.engine.aggregation.impl.average.numeric;

import java.io.IOException;
import java.math.BigDecimal;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.Streamer;
import io.crate.execution.engine.aggregation.impl.util.BigDecimalValueWrapper;
import io.crate.types.DataType;
import io.crate.types.NumericType;

@SuppressWarnings("rawtypes")
public class NumericAverageStateType extends DataType<NumericAverageState> implements Streamer<NumericAverageState> {

    public static final int ID = 1026;
    public static final long INIT_SIZE = NumericType.size(BigDecimal.ZERO) + 8; // Nominator and primitive long denominator.
    public static final NumericAverageStateType INSTANCE = new NumericAverageStateType();

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.CUSTOM;
    }

    @Override
    public String getName() {
        return "numeric_average_state";
    }

    @Override
    public Streamer<NumericAverageState> streamer() {
        return this;
    }

    @Override
    public NumericAverageState sanitizeValue(Object value) {
        return (NumericAverageState) value;
    }

    @Override
    public int compare(NumericAverageState val1, NumericAverageState val2) {
        if (val1 == null) return -1;
        return val1.compareTo(val2);
    }

    @Override
    public NumericAverageState readValueFrom(StreamInput in) throws IOException {
        // Cannot use NumericType.INSTANCE as it has default precision and scale values
        // which might not be equal to written BigDecimal's precision and scale.
        return new NumericAverageState<>(
            new BigDecimalValueWrapper(new NumericType(in.readInt(), in.readInt()).readValueFrom(in)),
            in.readVLong()
        );
    }

    @Override
    public void writeValueTo(StreamOutput out, NumericAverageState v) throws IOException {
        // We want to preserve the scale and precision
        // from the numeric argument type for the return type.
        out.writeInt(v.sum.value().precision());
        out.writeInt(v.sum.value().scale());
        NumericType.INSTANCE.writeValueTo(out, v.sum.value()); // NumericType.INSTANCE writes unscaled value
        out.writeVLong(v.count);
    }

    @Override
    public long valueBytes(NumericAverageState value) {
        throw new UnsupportedOperationException("valueSize is not implemented on NumericAverageStateType");
    }
}
