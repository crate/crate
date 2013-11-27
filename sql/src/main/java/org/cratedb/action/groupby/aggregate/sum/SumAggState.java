package org.cratedb.action.groupby.aggregate.sum;

import org.cratedb.action.groupby.aggregate.AggState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class SumAggState extends AggState<SumAggState> {

    private Double value = null;

    @Override
    public Object value() {
        return value;
    }

    @Override
    public void reduce(SumAggState other) {
        if (other.value == null) {
            return;
        } else if (value == null) {
            value = other.value;
            return;
        }
        value += other.value;
    }

    public void add(Object value) {
        if (this.value == null) {
            this.value = ((Number)value).doubleValue();
        } else {
            this.value += ((Number)value).doubleValue();
        }
    }

    @Override
    public int compareTo(SumAggState o) {
        if (o == null) return 1;
        return Double.compare(value, o.value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readFrom(StreamInput in) throws IOException {
        value = null;
        if (!in.readBoolean()) {
            value = in.readDouble();
        }

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(value == null);
        if (value != null) {
            out.writeDouble(value);
        }
    }
}
