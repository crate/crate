/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import io.crate.Streamer;
import io.crate.breaker.RamAccounting;
import io.crate.data.Input;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.FixedWidthType;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.types.TypeSignature.parseTypeSignature;

public class CountAggregation extends AggregationFunction<CountAggregation.LongState, Long> {

    public static final String NAME = "count";
    public static final Signature SIGNATURE =
        Signature.aggregate(
            NAME,
            parseTypeSignature("V"),
            DataTypes.LONG.getTypeSignature()
        )
            .withTypeVariableConstraints(typeVariable("V")
        );

    public static final Signature COUNT_STAR_SIGNATURE =
        Signature.aggregate(NAME, DataTypes.LONG.getTypeSignature());

    static {
        DataTypes.register(CountAggregation.LongStateType.ID, in -> CountAggregation.LongStateType.INSTANCE);
    }

    public static final FunctionInfo COUNT_STAR_FUNCTION = new FunctionInfo(
        new FunctionIdent(NAME, List.of()), DataTypes.LONG, FunctionInfo.Type.AGGREGATE);

    public static void register(AggregationImplModule mod) {
        mod.register(
            SIGNATURE,
            (signature, args) ->
                new CountAggregation(
                    new FunctionInfo(
                        new FunctionIdent(NAME, args),
                        DataTypes.LONG,
                        FunctionInfo.Type.AGGREGATE),
                    signature,
                    true
                )
        );
        mod.register(
            COUNT_STAR_SIGNATURE,
            (signature, args) ->
                new CountAggregation(COUNT_STAR_FUNCTION, signature, false)
        );
    }

    private final FunctionInfo info;
    private final Signature signature;
    private final boolean hasArgs;

    private CountAggregation(FunctionInfo info, Signature signature, boolean hasArgs) {
        this.info = info;
        this.signature = signature;
        this.hasArgs = hasArgs;
    }

    @Override
    public LongState iterate(RamAccounting ramAccounting,
                             MemoryManager memoryManager,
                             LongState state,
                             Input... args) {
        if (!hasArgs || args[0].value() != null) {
            return state.add(1L);
        }
        return state;
    }

    @Nullable
    @Override
    public LongState newState(RamAccounting ramAccounting,
                              Version indexVersionCreated,
                              Version minNodeInCluster,
                              MemoryManager memoryManager) {
        ramAccounting.addBytes(LongStateType.INSTANCE.fixedSize());
        return new LongState();
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Nullable
    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Symbol normalizeSymbol(Function function, TransactionContext txnCtx) {
        assert function.arguments().size() <= 1 : "function's number of arguments must be 0 or 1";

        if (function.arguments().size() == 1) {
            Symbol arg = function.arguments().get(0);
            if (arg instanceof Input) {
                if (((Input) arg).value() == null) {
                    return Literal.of(0L);
                } else {
                    return new Function(COUNT_STAR_FUNCTION, COUNT_STAR_SIGNATURE, List.of());
                }
            }
        }
        return function;
    }

    @Override
    public DataType partialType() {
        return LongStateType.INSTANCE;
    }

    @Override
    public LongState reduce(RamAccounting ramAccounting, LongState state1, LongState state2) {
        return state1.merge(state2);
    }

    @Override
    public Long terminatePartial(RamAccounting ramAccounting, LongState state) {
        return state.value;
    }

    public static class LongState implements Comparable<CountAggregation.LongState> {

        long value = 0L;

        LongState() {
        }

        public LongState(long value) {
            this.value = value;
        }

        public LongState add(long value) {
            this.value = this.value + value;
            return this;
        }

        public LongState sub(long value) {
            this.value = this.value - value;
            return this;
        }

        public LongState merge(LongState otherState) {
            add(otherState.value);
            return this;
        }

        @Override
        public int compareTo(LongState o) {
            if (o == null) {
                return 1;
            } else {
                return Long.compare(value, o.value);
            }
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }

    public static class LongStateType extends DataType<LongState>
        implements FixedWidthType, Streamer<LongState> {

        public static final int ID = 16384;
        public static final LongStateType INSTANCE = new LongStateType();

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
            return "long_state";
        }

        @Override
        public Streamer<LongState> streamer() {
            return this;
        }

        @Override
        public LongState value(Object value) throws IllegalArgumentException, ClassCastException {
            return (LongState) value;
        }

        @Override
        public int compare(LongState val1, LongState val2) {
            if (val1 == null) {
                return -1;
            } else {
                return val1.compareTo(val2);
            }
        }

        @Override
        public LongState readValueFrom(StreamInput in) throws IOException {
            return new CountAggregation.LongState(in.readVLong());
        }

        @Override
        public void writeValueTo(StreamOutput out, LongState v) throws IOException {
            out.writeVLong(v.value);
        }

        @Override
        public int fixedSize() {
            return DataTypes.LONG.fixedSize();
        }
    }

    @Override
    public boolean isRemovableCumulative() {
        return true;
    }

    @Override
    public LongState removeFromAggregatedState(RamAccounting ramAccounting,
                                               LongState previousAggState,
                                               Input[] stateToRemove) {
        if (!hasArgs || stateToRemove[0].value() != null) {
            return previousAggState.sub(1L);
        }
        return previousAggState;
    }
}
