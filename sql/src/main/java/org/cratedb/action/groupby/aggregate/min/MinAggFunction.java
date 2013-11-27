package org.cratedb.action.groupby.aggregate.min;

import com.google.common.collect.ImmutableSet;
import org.cratedb.DataType;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggFunction;

import java.util.Set;

public class MinAggFunction<T extends Comparable<T>> extends AggFunction<MinAggState<T>> {

    public static final String NAME = "MIN";
    public static final Set<DataType> supportedColumnTypes = new ImmutableSet.Builder<DataType>()
            .addAll(DataType.NUMERIC_TYPES)
            .add(DataType.STRING)
            .add(DataType.TIMESTAMP)
            .build();

    @Override
    public String name() {
        return NAME;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void iterate(MinAggState<T> state, Object columnValue) {
        if (state.compareValue((T)columnValue) > 0) {
            state.setValue((T)columnValue);
        }
    }

    @Override
    public Set<DataType> supportedColumnTypes() {
        return supportedColumnTypes;
    }



}
