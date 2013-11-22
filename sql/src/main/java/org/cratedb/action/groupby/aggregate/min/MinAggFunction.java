package org.cratedb.action.groupby.aggregate.min;

import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.groupby.aggregate.AggState;

public class MinAggFunction extends AggFunction {

    public static String NAME = "MIN";
    private MinAggState newAggState = new MinAggState();

    @Override
    public void iterate(AggState state, Object columnValue) {
        newAggState.value = columnValue;
        state.reduce(newAggState);
    }

    @Override
    public AggState createAggState() {
        return new MinAggState();
    }

}
