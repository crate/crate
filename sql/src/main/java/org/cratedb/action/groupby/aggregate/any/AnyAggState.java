package org.cratedb.action.groupby.aggregate.any;

import org.cratedb.action.groupby.aggregate.AggState;

public abstract class AnyAggState<T> extends AggState<AnyAggState<T>>{

    public T value = null;

    @Override
    public Object value() {
        return value;
    }

    public abstract void add(Object otherValue);

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
}
