package org.cratedb.action.groupby.aggregate.count;

import org.cratedb.action.groupby.aggregate.AggState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class CountAggState extends AggState {

    public long value;

    @Override
    public void readFrom(StreamInput in) throws IOException {
        value = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte((byte)0);
        out.writeVLong(value);
    }

    @Override
    public void merge(AggState other) {
        assert other instanceof CountAggState;
        value += ((CountAggState)other).value;
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
    public int compareTo(AggState o) {
        // let it crash if AggState isn't a CountAggState since it's not comparable
        CountAggState countAggState = (CountAggState)o;
        return Long.compare(value, countAggState.value);
    }
}
