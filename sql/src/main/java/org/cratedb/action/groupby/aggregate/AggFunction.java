package org.cratedb.action.groupby.aggregate;

import org.cratedb.DataType;

import java.util.Set;

public abstract class AggFunction<T extends AggState> {

    public abstract String name();

    /**
     * Apply the columnValue to the argument AggState using the logic in this AggFunction
     *
     * @param state the state to apply the columnValue on
     * @param columnValue the columnValue found in a document
     * @return false if we do not need any further iteration for this state
     */
    public abstract boolean iterate(T state, Object columnValue);
    public abstract Set<DataType> supportedColumnTypes();
    public boolean supportsDistinct() {
        return false;
    }
}
