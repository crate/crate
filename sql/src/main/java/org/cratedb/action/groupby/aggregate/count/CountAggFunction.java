package org.cratedb.action.groupby.aggregate.count;

import org.cratedb.DataType;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggFunction;

import java.util.Set;

public class CountAggFunction<T> extends AggFunction<CountAggState<T>> {

    public static String NAME = "COUNT";
    public static String COUNT_ROWS_NAME = "COUNT(*)";

    @Override
    public void iterate(CountAggState state, Object columnValue) {
        // TODO: do not count null values when in Column Mode
        state.value++;
    }

    @Override
    public CountAggState createAggState(AggExpr aggExpr) {
        // TODO: count on columns
        return new CountAggState();
    }

    @Override
    public Set<DataType> supportedColumnTypes() {
        return DataType.ALL_TYPES;
    }
}
