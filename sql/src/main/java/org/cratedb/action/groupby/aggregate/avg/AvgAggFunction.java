package org.cratedb.action.groupby.aggregate.avg;

import com.google.common.collect.ImmutableSet;
import org.cratedb.DataType;
import org.cratedb.action.groupby.aggregate.AggFunction;

import java.util.Set;

public class AvgAggFunction extends AggFunction<AvgAggState> {

    public static final String NAME = "AVG";
    public static final ImmutableSet<DataType> supportedColumnTypes = new ImmutableSet.Builder<DataType>()
            .addAll(DataType.NUMERIC_TYPES)
            .add(DataType.TIMESTAMP)
            .build();

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public boolean iterate(AvgAggState state, Object columnValue) {
        if (columnValue != null && columnValue instanceof Number) {
            state.add(columnValue);
        }
        return true;
    }

    @Override
    public Set<DataType> supportedColumnTypes() {
        return supportedColumnTypes;
    }
}
