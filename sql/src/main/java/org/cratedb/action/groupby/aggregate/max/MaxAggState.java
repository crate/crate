package org.cratedb.action.groupby.aggregate.max;

import org.cratedb.action.groupby.aggregate.AggState;

public abstract class MaxAggState<T extends MaxAggState> extends AggState<T> {
    public abstract void setValue(Object value);
    public abstract int compareValue(Object otherValue);
}
