package org.cratedb.action.groupby.aggregate.count;

import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.groupby.aggregate.AggState;

public class CountAggFunction extends AggFunction {

    public static String NAME = "COUNT";

    @Override
    public void iterate(AggState state, Object columnValue) {
        ((CountAggState)state).value++;
    }

    @Override
    public AggState createAggState() {
        return new CountAggState();
    }
}
