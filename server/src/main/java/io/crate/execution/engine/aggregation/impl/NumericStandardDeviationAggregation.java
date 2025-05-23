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

package io.crate.execution.engine.aggregation.impl;

import java.io.IOException;
import java.math.BigDecimal;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.Streamer;
import io.crate.data.Input;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.aggregation.statistics.NumericVariance;
import io.crate.memory.MemoryManager;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;

public abstract class NumericStandardDeviationAggregation<V extends NumericVariance>
    extends AggregationFunction<V, BigDecimal> {

    public abstract static class StdDevNumericStateType<V extends NumericVariance>
        extends DataType<V> implements Streamer<V> {

        @Override
        public Precedence precedence() {
            return Precedence.CUSTOM;
        }

        @Override
        public Streamer<V> streamer() {
            return this;
        }

        @SuppressWarnings("unchecked")
        @Override
        public V sanitizeValue(Object value) {
            return (V) value;
        }

        @Override
        public int compare(V val1, V val2) {
            return val1.compareTo(val2);
        }

        @Override
        public void writeValueTo(StreamOutput out, V v) throws IOException {
            v.writeTo(out);
        }

        @Override
        public long valueBytes(V value) {
            return value.size();
        }
    }

    private final Signature signature;
    private final BoundSignature boundSignature;

    public NumericStandardDeviationAggregation(Signature signature, BoundSignature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    protected abstract StdDevNumericStateType<V> stdDevStateTypeInstance();

    protected abstract V newVariance();

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public BoundSignature boundSignature() {
        return boundSignature;
    }

    @Override
    public V iterate(RamAccounting ramAccounting,
                     MemoryManager memoryManager,
                     V state,
                     Input<?>... args) throws CircuitBreakingException {
        if (state != null) {
            BigDecimal value = (BigDecimal) args[0].value();
            if (value != null) {
                state.increment(value);
            }
        }
        return state;
    }

    @Override
    public V reduce(RamAccounting ramAccounting, V state1, V state2) {
        if (state1 == null) {
            return state2;
        }
        if (state2 == null) {
            return state1;
        }
        state1.merge(state2);
        return state1;
    }

    @Override
    public boolean isRemovableCumulative() {
        return true;
    }

    @Override
    public V removeFromAggregatedState(RamAccounting ramAccounting, V previousAggState, Input<?>[]stateToRemove) {
        if (previousAggState != null) {
            BigDecimal value = (BigDecimal) stateToRemove[0].value();
            if (value != null) {
                previousAggState.decrement(value);
            }
        }
        return previousAggState;
    }

    @Override
    public BigDecimal terminatePartial(RamAccounting ramAccounting, V state) {
        return state.result();
    }

    @Override
    public DataType<?> partialType() {
        return stdDevStateTypeInstance();
    }
}
