package io.crate.operator.aggregation.impl;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.operator.Input;
import io.crate.operator.aggregation.AggregationFunction;
import io.crate.operator.aggregation.AggregationState;
import org.cratedb.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class AverageAggregation extends AggregationFunction<AverageAggregation.AverageAggState> {

    public static final String NAME = "avg";

    private final FunctionInfo info;

    public static void register() {
        for (DataType t : DataType.NUMERIC_TYPES) {
            Functions.registerImplementation(
                    new AverageAggregation(
                            new FunctionInfo(new FunctionIdent(NAME, ImmutableList.of(t)), DataType.DOUBLE, true))
            );
        }

    }

    AverageAggregation(FunctionInfo info) {
        this.info = info;
    }

    public class AverageAggState extends AggregationState<AverageAggState> {

        private double sum = 0;
        private long count = 0;

        @Override
        public Object value() {
            if (count > 0) {
                return sum / count;
            } else {
                return null;
            }
        }

        @Override
        public void reduce(AverageAggState other) {
            if (other != null) {
                sum += other.sum;
                count += other.count;
            }
        }

        void add(Object otherValue) {
            sum += ((Number) otherValue).doubleValue();
            count++;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            sum = in.readDouble();
            count = in.readVLong();
        }


        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(sum);
            out.writeVLong(count);
        }

        @Override
        public int compareTo(AverageAggState o) {
            if (o == null) {
                return 1;
            } else {
                Double thisValue = (Double) value();
                Double other = (Double) o.value();
                return thisValue.compareTo(other);
            }
        }
    }


    @Override
    public boolean iterate(AverageAggState state, Input... args) {
        state.add(args[0].value());
        return true;
    }

    @Override
    public AverageAggState newState() {
        return new AverageAggState();
    }

    @Override
    public FunctionInfo info() {
        return info;
    }
}
