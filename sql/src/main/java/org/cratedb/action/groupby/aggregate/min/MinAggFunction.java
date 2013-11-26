package org.cratedb.action.groupby.aggregate.min;

import com.google.common.collect.ImmutableSet;
import org.cratedb.DataType;
import org.cratedb.action.groupby.aggregate.AggExpr;
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
            state.setValue(columnValue);
        }
    }

    @Override
    public MinAggState createAggState(AggExpr aggExpr) {
        assert aggExpr.parameterInfo != null;
        if (DataType.DECIMAL_TYPES.contains(aggExpr.parameterInfo.dataType)) {
            return new MinAggStateDouble();
        } else if (DataType.INTEGER_TYPES.contains(aggExpr.parameterInfo.dataType) ||
                aggExpr.parameterInfo.dataType == DataType.TIMESTAMP) {
            return new MinAggStateLong();
        } else if (aggExpr.parameterInfo.dataType == DataType.STRING) {
            return new MinAggStateString();
        }
        // shouldn't happen
        throw new IllegalArgumentException("Illegal AggExpr for MinAggFunction");
    }

    @Override
    public Set<DataType> supportedColumnTypes() {
        return supportedColumnTypes;
    }

}
