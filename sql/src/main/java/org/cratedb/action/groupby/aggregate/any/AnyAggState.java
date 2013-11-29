package org.cratedb.action.groupby.aggregate.any;

import org.cratedb.action.groupby.aggregate.AggState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class AnyAggState<T> extends AggState<AnyAggState<T>>{

    private T value = null;

    @Override
    public Object value() {
        return value;
    }

    public void add(T otherValue) {
        this.value = otherValue;
    }

    @Override
    public void reduce(AnyAggState<T> other) {
        this.value = other.value;
    }

    @Override
    public int compareTo(AnyAggState<T> o) {
        if (o == null) return 1;
        if (value == null) return (o.value == null ? 0 : -1);
        if (o.value == null) return 1;

        return 0; // any two object that are not null are considered equal
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        value = (T)in.readGenericValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericValue(value);
    }
}
