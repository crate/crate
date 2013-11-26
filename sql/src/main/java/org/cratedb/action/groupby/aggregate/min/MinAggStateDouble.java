package org.cratedb.action.groupby.aggregate.min;


import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class MinAggStateDouble extends MinAggState<MinAggStateDouble> {

    private Double value = null;

    @Override
    public Object value() {
        return value;
    }

    @Override
    public void setValue(Object value) {
        this.value = (Double)value;
    }

    @Override
    public void reduce(MinAggStateDouble other) {
        if (other.value == null) {
            return;
        } else if (value == null) {
            value = other.value;
            return;
        }

        if (compareTo(other) == 1) {
            value = other.value;
        }
    }

    @Override
    public int compareTo(MinAggStateDouble o) {
        if (o == null) return -1;
        return compareValue(o.value);
    }

    @Override
    public int compareValue(Object otherValue) {
        if (value == null) return (otherValue == null ? 0 : 1);
        if (otherValue == null) return -1;
        return value.compareTo((Double)otherValue);

    }

    @Override
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
