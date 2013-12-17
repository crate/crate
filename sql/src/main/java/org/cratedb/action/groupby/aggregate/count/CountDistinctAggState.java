package org.cratedb.action.groupby.aggregate.count;

import org.cratedb.action.groupby.aggregate.AggState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Set;

public class CountDistinctAggState extends AggState<CountDistinctAggState> {

    public Set<Object> seenValues;

    @Override
    public Object value() {
        // TODO: we have a limit of Integer.MAX_VALUE for counts
        if (seenValues != null) {
            return ((Number) seenValues.size()).longValue();
        }
        return 0;
    }

    @Override
    public void reduce(CountDistinctAggState other) {
    }

    @Override
    public int compareTo(CountDistinctAggState o) {
        return Integer.compare(seenValues.size(), (Integer) o.value());
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
    }

    @Override
    public void setSeenValuesRef(Set<Object> seenValues) {
        this.seenValues = seenValues;
    }

}
