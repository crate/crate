package org.cratedb.action.groupby.aggregate.avg;

import org.cratedb.action.groupby.aggregate.AggState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class AvgAggState extends AggState<AvgAggState> {

    private double sum = 0;
    private long count = 0;

    @Override
    public Object value() {
        if (count > 0) {
            return sum/count;
        } else {
            return null;
        }
    }

    @Override
    public void reduce(AvgAggState other) {
        if (other != null) {
            sum += other.sum;
            count += other.count;
        }
    }

    public void add(Object otherValue) {
        sum += ((Number)otherValue).doubleValue();
        count++;
    }

    @Override
    public int compareTo(AvgAggState o) {
        if (o==null) { return 1;}
        else {
            Double thisValue = (Double)value();
            Double other = (Double)o.value();
            return thisValue.compareTo(other);
        }
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
}
