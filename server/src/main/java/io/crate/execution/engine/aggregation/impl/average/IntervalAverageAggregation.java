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

package io.crate.execution.engine.aggregation.impl.average;

import static io.crate.execution.engine.aggregation.impl.average.AverageAggregation.NAMES;
import static org.joda.time.DateTimeConstants.MILLIS_PER_DAY;
import static org.joda.time.DateTimeConstants.MILLIS_PER_HOUR;
import static org.joda.time.DateTimeConstants.MILLIS_PER_MINUTE;
import static org.joda.time.DateTimeConstants.MILLIS_PER_SECOND;
import static org.joda.time.DateTimeConstants.MILLIS_PER_WEEK;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Objects;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.joda.time.Period;
import org.joda.time.PeriodType;

import io.crate.Streamer;
import io.crate.data.Input;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.aggregation.impl.util.OverflowAwareMutableLong;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.FixedWidthType;

public class IntervalAverageAggregation extends AggregationFunction<IntervalAverageAggregation.IntervalAverageState, Period> {

    static {
        DataTypes.register(IntervalAverageStateType.ID, in -> IntervalAverageStateType.INSTANCE);
    }

    /**
     * register as "avg" and "mean"
     */
    public static void register(Functions.Builder builder) {
        for (var functionName : NAMES) {
            builder.add(
                    Signature.builder(functionName, FunctionType.AGGREGATE)
                            .argumentTypes(DataTypes.INTERVAL.getTypeSignature())
                            .returnType(DataTypes.INTERVAL.getTypeSignature())
                            .features(Scalar.Feature.DETERMINISTIC)
                            .build(),
                    IntervalAverageAggregation::new
            );
        }
    }

    public static class IntervalAverageState implements Comparable<IntervalAverageState> {

        private long[] sum = new long[8];
        private long count = 0;

        public Period value() {
            if (count > 0) {
                OverflowAwareMutableLong sumMillis = new OverflowAwareMutableLong(sum[7]);
                sumMillis.add(sum[6] * MILLIS_PER_SECOND);
                sumMillis.add(sum[5] * MILLIS_PER_MINUTE);
                sumMillis.add(sum[4] * MILLIS_PER_HOUR);
                sumMillis.add(sum[3] * MILLIS_PER_DAY);
                sumMillis.add(sum[2] * MILLIS_PER_WEEK);
                sumMillis.add(sum[1] * MILLIS_PER_DAY * 30);
                sumMillis.add(sum[0] * MILLIS_PER_DAY * 365);
                long avg = sumMillis.value().divideToIntegralValue(BigDecimal.valueOf(count)).longValue();
                return new Period(avg).normalizedStandard(PeriodType.yearMonthDayTime());
            } else {
                return null;
            }
        }

        public void addPeriod(Period period) {
            sum[0] += period.getYears();
            sum[1] += period.getMonths();
            sum[2] += period.getWeeks();
            sum[3] += period.getDays();
            sum[4] += period.getHours();
            sum[5] += period.getMinutes();
            sum[6] += period.getSeconds();
            sum[7] += period.getMillis();

            count++;
        }

        public void removePeriod(Period period) {
            sum[0] -= period.getYears();
            sum[1] -= period.getMonths();
            sum[2] -= period.getWeeks();
            sum[3] -= period.getDays();
            sum[4] -= period.getHours();
            sum[5] -= period.getMinutes();
            sum[6] -= period.getSeconds();
            sum[7] -= period.getMillis();

            count--;
        }

        public void reduce(@NotNull IntervalAverageState other) {
            this.count += other.count;
            for (int i = 0; i < sum.length; i++) {
                this.sum[i] += other.sum[i];
            }
        }

        @Override
        public int compareTo(IntervalAverageState o) {
            if (o == null) {
                return 1;
            } else {
                int compare = Arrays.compare(sum, o.sum);
                if (compare == 0) {
                    return Long.compare(count, o.count);
                }
                return compare;
            }
        }

        @Override
        public String toString() {
            return "sum: " + Arrays.toString(sum) + " count: " + count;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            IntervalAverageState that = (IntervalAverageState) o;
            return Objects.equals(that.value(), value());
        }

        @Override
        public int hashCode() {
            return Objects.hash(value());
        }
    }

    public static class IntervalAverageStateType extends DataType<IntervalAverageState>
        implements FixedWidthType, Streamer<IntervalAverageState> {

        public static final int ID = 1028;
        private static final IntervalAverageStateType INSTANCE = new IntervalAverageStateType();
        private static final int AVERAGE_STATE_SIZE = (int) RamUsageEstimator.shallowSizeOfInstance(IntervalAverageState.class);

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
            return "interval_average_state";
        }

        @Override
        public Streamer<IntervalAverageState> streamer() {
            return this;
        }

        @Override
        public IntervalAverageState sanitizeValue(Object value) {
            return (IntervalAverageState) value;
        }

        @Override
        public int compare(IntervalAverageState val1, IntervalAverageState val2) {
            if (val1 == null) return -1;
            return val1.compareTo(val2);
        }

        @Override
        public IntervalAverageState readValueFrom(StreamInput in) throws IOException {
            IntervalAverageState averageState = new IntervalAverageState();
            averageState.sum = in.readLongArray();
            averageState.count = in.readVLong();
            return averageState;
        }

        @Override
        public void writeValueTo(StreamOutput out, IntervalAverageState v) throws IOException {
            out.writeLongArray(v.sum);
            out.writeVLong(v.count);
        }

        @Override
        public int fixedSize() {
            return AVERAGE_STATE_SIZE;
        }

        @Override
        public long valueBytes(IntervalAverageState value) {
            return fixedSize();
        }
    }

    private final Signature signature;
    private final BoundSignature boundSignature;

    IntervalAverageAggregation(Signature signature, BoundSignature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    @Override
    public IntervalAverageState iterate(RamAccounting ramAccounting,
                                        MemoryManager memoryManager,
                                        IntervalAverageState state,
                                        Input<?>... args) {
        if (state != null) {
            Period value = (Period) args[0].value();
            if (value != null) {
                state.addPeriod(value); // Mutates state.
            }
        }
        return state;
    }

    @Override
    public boolean isRemovableCumulative() {
        return true;
    }

    @Override
    public IntervalAverageState removeFromAggregatedState(RamAccounting ramAccounting,
                                                          IntervalAverageState previousAggState,
                                                          Input<?>[]stateToRemove) {
        if (previousAggState != null) {
            Period value = (Period) stateToRemove[0].value();
            if (value != null) {
                previousAggState.removePeriod(value); // Mutates previousAggState.
            }
        }
        return previousAggState;
    }

    @Override
    public IntervalAverageState reduce(RamAccounting ramAccounting,
                                       IntervalAverageState state1,
                                       IntervalAverageState state2) {
        if (state1 == null) {
            return state2;
        }
        if (state2 == null) {
            return state1;
        }
        state1.reduce(state2); // Mutates state1.
        return state1;
    }

    @Override
    public Period terminatePartial(RamAccounting ramAccounting, IntervalAverageState state) {
        return state.value();
    }

    @Nullable
    @Override
    public IntervalAverageState newState(RamAccounting ramAccounting,
                                         Version indexVersionCreated,
                                         Version minNodeInCluster,
                                         MemoryManager memoryManager) {
        ramAccounting.addBytes(IntervalAverageStateType.INSTANCE.fixedSize());
        return new IntervalAverageState();
    }

    @Override
    public DataType<?> partialType() {
        return IntervalAverageStateType.INSTANCE;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public BoundSignature boundSignature() {
        return boundSignature;
    }
}

