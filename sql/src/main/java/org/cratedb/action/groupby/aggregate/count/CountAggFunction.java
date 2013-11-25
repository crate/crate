package org.cratedb.action.groupby.aggregate.count;

import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.sql.types.SQLTypes;

import java.util.Set;

public class CountAggFunction extends AggFunction<CountAggState> {

    public static String NAME = "COUNT";
    public static String COUNT_ROWS_NAME = "COUNT(*)";

    @Override
    public void iterate(CountAggState state, Object columnValue) {
        // TODO: do not count null values when in Column Mode
        state.value++;
    }

    @Override
    public CountAggState createAggState() {
        return new CountAggState();
    }

    @Override
    public Set<String> supportedColumnTypes() {
        return SQLTypes.ALL_TYPES.keySet();
    }
}
