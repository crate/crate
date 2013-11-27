package org.cratedb.action.groupby.aggregate;

import org.cratedb.DataType;

import java.util.Set;

public abstract class AggFunction<T extends AggState> {

    public abstract String name();
    public abstract void iterate(T state, Object columnValue);
    public abstract Set<DataType> supportedColumnTypes();
    public boolean supportsDistinct() {
        return false;
    }
}
