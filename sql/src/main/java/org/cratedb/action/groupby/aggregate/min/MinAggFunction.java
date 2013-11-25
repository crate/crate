package org.cratedb.action.groupby.aggregate.min;

import com.google.common.collect.ImmutableSet;
import org.cratedb.DataType;
import org.cratedb.action.groupby.aggregate.AggFunction;

import java.util.Set;

public class MinAggFunction extends AggFunction<MinAggState> {

    public static final String NAME = "MIN";
    public static final Set<DataType> supportedColumnTypes = new ImmutableSet.Builder<DataType>()
            .addAll(DataType.NUMERIC_TYPES)
            .add(DataType.STRING)
            .add(DataType.TIMESTAMP)
            .build();

    @Override
    public void iterate(MinAggState state, Object columnValue) {
        if (state.compareValue(columnValue) == 1) {
            state.value = columnValue;
        }
    }

    @Override
    public MinAggState createAggState() {
        return new MinAggState();
    }

    @Override
    public Set<DataType> supportedColumnTypes() {
        return supportedColumnTypes;
    }

}
