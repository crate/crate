package org.cratedb.action.groupby.aggregate.max;


import com.google.common.collect.ImmutableSet;
import org.cratedb.DataType;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggFunction;

import java.util.Set;

public class MaxAggFunction<T extends Comparable<T>> extends AggFunction<MaxAggState<T>> {

    public static final String NAME = "MAX";
    public static final Set<DataType> supportedColumnTypes = new ImmutableSet.Builder<DataType>()
            .addAll(DataType.NUMERIC_TYPES)
            .add(DataType.STRING)
            .add(DataType.TIMESTAMP)
            .build();

    @Override
    public void iterate(MaxAggState<T> state, Object columnValue) {
        if (state.compareValue((T)columnValue) < 0) {
            state.setValue((T)columnValue);
        }
    }

    @Override
    public MaxAggState createAggState(AggExpr aggExpr) {
        assert aggExpr.parameterInfo != null;
        if (DataType.DECIMAL_TYPES.contains(aggExpr.parameterInfo.dataType)) {
            return new MaxAggState<Double>();
        } else if (DataType.INTEGER_TYPES.contains(aggExpr.parameterInfo.dataType) ||
                aggExpr.parameterInfo.dataType == DataType.TIMESTAMP) {
            return new MaxAggState<Long>();
        } else if (aggExpr.parameterInfo.dataType == DataType.STRING) {
            return new MaxAggState<String>();
        }
        // shouldn't happen
        throw new IllegalArgumentException("Illegal AggExpr for MAX");
    }

    @Override
    public Set<DataType> supportedColumnTypes() {
        return supportedColumnTypes;
    }
}
