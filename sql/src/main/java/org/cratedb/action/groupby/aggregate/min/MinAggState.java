package org.cratedb.action.groupby.aggregate.min;

import org.cratedb.action.groupby.aggregate.AggState;

public class MinAggState extends AggState {

    @Override
    public void reduce(AggState other) {
        assert other instanceof MinAggState;

        if (other.value == null) {
            return;
        } else if (value == null) {
            value = other.value;
            return;
        }

        int res = compareTo(other);
        if (res == 1) {
            value = other.value;
        }
    }

    @Override
    public String toString() {
        return "AggState {" + value + "}";
    }

    @Override
    public int compareTo(AggState o) {
        // let it crash if AggState isn't a MinAggState since it's not comparable
        MinAggState minAggState = (MinAggState)o;
        return super.compareTo(minAggState);
    }

}
