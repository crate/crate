package org.cratedb.action.groupby.aggregate.count;

import org.cratedb.action.groupby.aggregate.AggState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class CountAggState<T> extends AggState<CountAggState<T>> {

    public long value;

    @Override
    public void readFrom(StreamInput in) throws IOException {
        value = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(value);
    }

    public void reduce(CountAggState<T> other) {
        value += other.value;
    }

    @Override
    public Object value() {
        return value;
    }

    @Override
    public String toString() {
        return "AggState {" + value + "}";
    }

    @Override
    public int compareTo(CountAggState<T> o) {
        return Long.compare(value, o.value);

    }
}
