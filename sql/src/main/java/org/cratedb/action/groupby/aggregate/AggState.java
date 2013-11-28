package org.cratedb.action.groupby.aggregate;

import org.elasticsearch.common.io.stream.Streamable;

import java.util.Set;

/**
 * State of a aggregation function
 *
 * Note on serialization:
 *      In order to read the correct concrete AggState class on the receiver
 *      the receiver has to get the ParsedStatement beforehand and then use it
 *      to instantiate the correct concrete AggState instances.
 */
public abstract class AggState<T extends AggState> implements Comparable<T>, Streamable {

    public abstract Object value();
    public abstract void reduce(T other);

    /**
     * called after the rows/state have been merged on the reducer,
     * but before the rows are sent to the handler.
     */
    public void terminatePartial() {
        // noop;
    }


    /**
     * can be used to get a reference to a set of unique values
     * the set is managed by the aggStates encapsulating GroupByRow
     */
    public void setSeenValuesRef(Set<Object> seenValues) {
    }
}
