package org.cratedb.action.groupby.aggregate.avg;

import org.cratedb.action.groupby.aggregate.AggState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;


public class AvgAggState extends AggState {

    public long sum;
    public long count;

    @Override
    public void readFrom(StreamInput in) throws IOException {
        sum = in.readVLong();
        count = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(sum);
        out.writeVLong(count);
    }

    @Override
    public void reduce(AggState other) {
        assert other instanceof AvgAggState;
    }

    @Override
    public Object value() {
        return (double)sum / count;
    }

    @Override
    public int compareTo(AggState o) {
        // let it fail if other isn't of type AvgAggState since it is not comparable.
        AvgAggState other = (AvgAggState)o;
        double thisValue = (double)value();
        double otherValue = (double)other.value();

        return Double.compare(thisValue, otherValue);
    }
}
