package org.cratedb.action.groupby.aggregate;

import org.elasticsearch.common.io.stream.Streamable;

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
}
