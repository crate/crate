package org.cratedb.action.groupby.aggregate;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

/**
 * State of a aggregation function
 *
 * Note on serialization:
 *      In order to read the correct concrete AggState class on the receiver
 *      the receiver has to get the ParsedStatement beforehand and then use it
 *      to instantiate the correct concrete AggState instances.
 */
public abstract class AggState<T extends AggState> implements Comparable<T>, Streamable {


    public Object value;

    public abstract void reduce(T other);

    public Object value() {
        return value;
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

    @Override
    public int compareTo(AggState o) {
        if (value == null && o.value == null) {
            return 0;
        } else if (value == null && o.value != null) {
            return 1;
        } else if (o.value == null) {
            return -1;
        }

        Class type = value.getClass();
        if (type == Double.class) {
            return Double.compare((Double)value, (Double)o.value);
        } else if (type == Long.class) {
            return Long.compare((Long)value, (Long)o.value);
        } else if (type == String.class) {
            int res = ((String)value).compareTo(((String)o.value));
            return (res < 0) ? -1 : ((res == 0) ? 0 : 1);
        }

        return 0;
    }
}
