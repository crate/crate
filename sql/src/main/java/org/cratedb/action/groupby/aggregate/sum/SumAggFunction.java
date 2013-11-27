package org.cratedb.action.groupby.aggregate.sum;

import com.google.common.collect.ImmutableSet;
import org.cratedb.DataType;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggFunction;

import java.util.Set;

public class SumAggFunction extends AggFunction<SumAggState> {
    public static final String NAME = "SUM";
    public static final ImmutableSet<DataType> supportedColumnTypes = new ImmutableSet.Builder<DataType>()
            .addAll(DataType.NUMERIC_TYPES)
            .add(DataType.TIMESTAMP)
            .build();


    @Override
    public void iterate(SumAggState state, Object columnValue) {
        if (columnValue != null && columnValue instanceof Number) {
            state.add(columnValue);
        }
    }

    @Override
    public Set<DataType> supportedColumnTypes() {
        return supportedColumnTypes;
    }
}
