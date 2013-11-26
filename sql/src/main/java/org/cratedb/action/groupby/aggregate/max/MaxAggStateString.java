package org.cratedb.action.groupby.aggregate.max;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class MaxAggStateString extends MaxAggState<MaxAggStateString> {
    private String value = null;

    @Override
    public void setValue(Object value) {
        this.value = (String)value;
    }

    @Override
    public int compareValue(Object otherValue) {
        if (value == null) return (otherValue == null ? 0 : 1);
        if (otherValue == null) return -1;
        return value.compareTo((String)otherValue);
    }

    @Override
    public Object value() {
        return this.value;
    }

    @Override
    public void reduce(MaxAggStateString other) {
        if (other.value == null) {
            return;
        } else if (value == null) {
            value = other.value;
            return;
        }

        if (compareTo(other) < 0) {
            value = other.value;
        }
    }

    @Override
    public int compareTo(MaxAggStateString o) {
        if (o == null) return -1;
        return compareValue(o.value);
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
