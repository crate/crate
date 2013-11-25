package org.cratedb.action.groupby.aggregate.min;

import org.cratedb.action.groupby.aggregate.AggState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class MinAggState extends AggState<MinAggState> {

    public Object value;


    @Override
    public Object value() {
        return value;
    }

    @Override
    public void reduce(MinAggState other) {
        if (other.value == null) {
            return;
        } else if (value == null) {
            value = other.value;
            return;
        }

        int res = compareTo(other);
        if (res == 1) {
            value = other.value;
        }
    }

    @Override
    public String toString() {
        return "AggState {" + value + "}";
    }

    public int compareValue(Object other) {
        if (value == null && other == null) {
            return 0;
        } else if (value == null && other != null) {
            return 1;
        } else if (other == null) {
            return -1;
        }

        Class type = value.getClass();
        if (type == Double.class) {
            return Double.compare((Double)value, (Double)other);
        } else if (type == Long.class) {
            return Long.compare((Long)value, (Long)other);
        } else if (type == String.class) {
            int res = ((String)value).compareTo(((String)other));
            return (res < 0) ? -1 : ((res == 0) ? 0 : 1);
        }
        return 0;
    }

    @Override
    public int compareTo(MinAggState o) {
        return compareValue(o.value);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        boolean isNull = in.readBoolean();
        if (!isNull) {
            value = in.readGenericValue();
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
