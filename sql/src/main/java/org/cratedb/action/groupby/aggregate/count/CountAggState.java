package org.cratedb.action.groupby.aggregate.count;

import org.cratedb.action.groupby.aggregate.AggState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Set;

public class CountAggState extends AggState<CountAggState> {

    public long value;

    // not serialized;
    public Set<Object> seenValues;

    @Override
    public void readFrom(StreamInput in) throws IOException {
        value = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(value);
    }

    @Override
    public void terminatePartial() {
        if (seenValues != null) {
            value = seenValues.size();
        }
    }

    @Override
    public void setSeenValuesRef(Set<Object> seenValues) {
        this.seenValues = seenValues;
    }

    @Override
    public Object value() {
        return value;
    }

    @Override
    public void reduce(CountAggState other) {
        value += other.value;
    }

    @Override
    public String toString() {
        return "AggState {" + value + "}";
    }

    @Override
    public int compareTo(CountAggState o) {
        return Long.compare(value, o.value);
    }
}
