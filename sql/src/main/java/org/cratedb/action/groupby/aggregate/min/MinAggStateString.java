package org.cratedb.action.groupby.aggregate.min;


import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class MinAggStateString extends MinAggState<MinAggStateString> {

    private String value = null;

    @Override
    public Object value() {
        return value;
    }

    @Override
    public void setValue(Object value) {
        this.value = (String)value;
    }

    @Override
    public void reduce(MinAggStateString other) {
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
    public int compareTo(MinAggStateString o) {
        if (o == null) return -1;
        return compareValue(o.value);
    }

    @Override
    public int compareValue(Object otherValue) {
        if (value == null) return otherValue == null ? 0 : 1;
        if (otherValue == null) return -1;
        int res = value.compareTo((String)otherValue);
        return (res < 0) ? -1 : ((res == 0) ? 0 : 1);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        value = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(value);
    }
}
