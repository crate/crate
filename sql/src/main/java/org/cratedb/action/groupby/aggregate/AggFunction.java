package org.cratedb.action.groupby.aggregate;

import org.cratedb.DataType;

import java.util.Set;

public abstract class AggFunction<T extends AggState> {

    public abstract void iterate(T state, Object columnValue);
    public abstract T createAggState();
    public abstract Set<DataType> supportedColumnTypes();

}
