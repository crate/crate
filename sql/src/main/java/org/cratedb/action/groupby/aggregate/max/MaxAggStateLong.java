package org.cratedb.action.groupby.aggregate.max;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class MaxAggStateLong extends MaxAggState<MaxAggStateLong> {
    private Long value = null;

    @Override
    public void setValue(Object value) {
        this.value = (Long)value;
    }

    @Override
    public int compareValue(Object otherValue) {
        if (value == null) return (otherValue == null ? 0 : 1);
        if (otherValue == null) return -1;
        return value.compareTo((Long)otherValue);
    }

    @Override
    public Object value() {
        return this.value;
    }

    @Override
    public void reduce(MaxAggStateLong other) {
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
    public int compareTo(MaxAggStateLong o) {
        if (o == null) return -1;
        return compareValue(o.value);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        value = null;
        if (!in.readBoolean()) {
            value = in.readLong();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(value == null);
        if (value != null) {
            out.writeLong(value);
        }
    }
}
