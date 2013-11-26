package org.cratedb.action.groupby.aggregate.min;

import org.cratedb.action.groupby.aggregate.AggState;

/**
 * marker class
 * @param <T>
 */
public abstract class MinAggState<T extends MinAggState> extends AggState<T> {
    public abstract void setValue(Object value);
    public abstract int compareValue(Object otherValue);
}
