package org.cratedb.action.groupby.aggregate.count;

import org.cratedb.DataType;
import org.cratedb.action.groupby.aggregate.AggFunction;

import java.util.Set;

public class CountAggFunction<T> extends AggFunction<CountAggState> {

    public static String NAME = "COUNT";
    public static String COUNT_ROWS_NAME = "COUNT(*)";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void iterate(CountAggState state, Object columnValue) {
        if (state.isDistinct && columnValue != null) {

            // to improve readability in the groupingCollector the seenValues.add is done here
            // if the seenValues is shared across multiple states this means that the add operation
            // is executed multiple times. TODO: move to collector if performance is too bad.
            state.seenValues.add(columnValue);
        } else {
            state.value++;
        }
    }

    @Override
    public Set<DataType> supportedColumnTypes() {
        return DataType.ALL_TYPES;
    }

    @Override
    public boolean supportsDistinct() {
        return true;
    }
}
