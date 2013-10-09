package org.cratedb.action.groupby.aggregate;

public abstract class AggFunction {

    public abstract void iterate(AggState state, Object columnValue);
    public abstract AggState createAggState();
}
