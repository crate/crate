package org.cratedb.action.groupby.aggregate.min;

import org.cratedb.action.groupby.aggregate.AggState;

/**
 * MIN Aggregation Function State
 * @param <T>
 */
public abstract class MinAggState<T extends Comparable<T>> extends AggState<MinAggState<T>> {

    private T value = null;

    @Override
    public Object value() {
        return value;
    }

    @Override
    public void reduce(MinAggState<T> other) {
        if (other.value == null) {
            return;
        } else if (value == null) {
            value = other.value;
            return;
        }

        if (compareTo(other) > 0) {
            value = other.value;
        }
    }


    public void setValue(T value) {
        this.value = value;
    }

    @Override
    public int compareTo(MinAggState<T> o) {
        if (o == null) return -1;
        return compareValue(o.value);
    }

    public int compareValue(T otherValue) {
        if (value == null) return (otherValue == null ? 0 : 1);
        if (otherValue == null) return -1;
        return value.compareTo(otherValue);
    }

    @Override
    public String toString() {
        return "<MinAggState \"" + value + "\"";
    }
}
