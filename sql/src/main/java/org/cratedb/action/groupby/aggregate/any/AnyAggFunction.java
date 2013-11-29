package org.cratedb.action.groupby.aggregate.any;

import org.cratedb.DataType;
import org.cratedb.action.groupby.aggregate.AggFunction;

import java.util.Set;

/**
 *
 */
public class AnyAggFunction<T> extends AggFunction<AnyAggState<T>> {

    public static final String NAME = "ANY";


    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void iterate(AnyAggState<T> state, Object columnValue) {
        state.add((T)columnValue);
    }

    @Override
    public Set<DataType> supportedColumnTypes() {
        return DataType.PRIMITIVE_TYPES;
    }
}
