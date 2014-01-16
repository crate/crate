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

public class SumAggregation extends AggregationFunction<SumAggregation.SumAggState> {

    public static final String NAME = "sum";
    private final FunctionInfo info;

    public static void register(AggregationImplModule mod) {
        for (DataType t : DataType.NUMERIC_TYPES) {
            mod.registerAggregateFunction(
                    new SumAggregation(
                            new FunctionInfo(new FunctionIdent(NAME, ImmutableList.of(t)), DataType.DOUBLE, true))
            );
        }

    }

    SumAggregation(FunctionInfo info) {
        this.info = info;
    }

    public static class SumAggState extends AggregationState<SumAggState> {

        private double value = 0.0;

        @Override
        public Object value() {
            return value;
        }

        @Override
        public void reduce(SumAggState other) {
            value += other.value;
        }

        public void add(Object value) {
            if (value == null) {
                return;
            }
            this.value += ((Number)value).doubleValue();
        }

        @Override
        public int compareTo(SumAggState o) {
            if (o == null) return 1;
            return Double.compare(value, o.value);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void readFrom(StreamInput in) throws IOException {
            value = in.readDouble();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(value);
        }
    }


    @Override
    public boolean iterate(SumAggState state, Input... args) {
        state.add(args[0].value());
        return true;
    }

    @Override
    public SumAggState newState() {
        return new SumAggState();
    }

    @Override
    public FunctionInfo info() {
        return info;
    }
}
