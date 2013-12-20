package org.cratedb.action.groupby.aggregate.count;

import org.cratedb.action.groupby.aggregate.AggState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Set;

public class CountDistinctAggState extends AggState<CountDistinctAggState> {

    // TODO: we have a limit of Integer.MAX_VALUE for counts due to seenValues.size()
    public Set<Object> seenValues;
    Long value;

    @Override
    public Object value() {
        return value;
    }

    @Override
    public void reduce(CountDistinctAggState other) {
    }

    @Override
    public void terminatePartial() {
        value = ((Number)seenValues.size()).longValue();
    }

    @Override
    public int compareTo(CountDistinctAggState o) {
        return Integer.compare(value == null ? seenValues.size() : value.intValue(), (Integer) o.value());
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            value = in.readVLong();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (value != null) {
            out.writeBoolean(true);
            out.writeVLong(value);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public void setSeenValuesRef(Set<Object> seenValues) {
        this.seenValues = seenValues;
    }
}
