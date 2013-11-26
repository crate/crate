package org.cratedb.action.groupby.aggregate.max;

import org.cratedb.action.groupby.aggregate.AggState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;


public class MaxAggState<T extends Comparable<T>> extends AggState<MaxAggState<T>> {

    private T value = null;

    @Override
    public Object value() {
        return value;
    }

    @Override
    public void reduce(MaxAggState<T> other) {
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


    public void setValue(T value) {
        this.value = value;
    }

    @Override
    public int compareTo(MaxAggState<T> o) {
        if (o == null) return -1;
        return compareValue(o.value);
    }

    public int compareValue(T otherValue) {
        if (value == null) return (otherValue == null ? 0 : -1);
        if (otherValue == null) return 1;

        return Integer.signum(value.compareTo(otherValue));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readFrom(StreamInput in) throws IOException {
        value = null;
        if (!in.readBoolean()) {
            value = (T)in.readGenericValue();
        }

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(value == null);
        if (value != null) {
            out.writeGenericValue(value);
        }
    }
}
