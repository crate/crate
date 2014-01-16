package io.crate.operator.aggregation.impl;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.operator.Input;
import io.crate.operator.aggregation.AggregationFunction;
import io.crate.operator.aggregation.AggregationState;
import org.cratedb.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class CountAggregation extends AggregationFunction<CountAggregation.CountAggState> {

    public static final String NAME = "count";
    private final FunctionInfo info;

    public static void register(AggregationImplModule mod) {
        for (DataType t : DataType.ALL_TYPES) {
            mod.registerAggregateFunction(
                    new CountAggregation(
                            new FunctionInfo(new FunctionIdent(NAME, ImmutableList.of(t)), DataType.LONG, true))
            );
        }
        // Register function for 0 inputs (count(*))
        mod.registerAggregateFunction(
                new CountAggregation(
                        new FunctionInfo(new FunctionIdent(NAME, ImmutableList.<DataType>of()), DataType.LONG, true))
        );

    }

    CountAggregation(FunctionInfo info) {
        this.info = info;
    }

    public static class CountAggState extends AggregationState<CountAggState> {

        public long value = 0;

        @Override
        public Object value() {
            return value;
        }

        @Override
        public void reduce(CountAggState other) {
            value += other.value;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            value = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(value);
        }

        @Override
        public String toString() {
            return "CountAggState {" + value + "}";
        }

        @Override
        public int compareTo(CountAggState o) {
            return Long.compare(value, o.value);
        }
    }


    @Override
    public boolean iterate(CountAggState state, Input... args) {
        state.value++;
        return true;
    }

    @Override
    public CountAggState newState() {
        return new CountAggState();
    }

    @Override
    public FunctionInfo info() {
        return info;
    }
}
