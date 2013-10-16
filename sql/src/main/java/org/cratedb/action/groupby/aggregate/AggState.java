package org.cratedb.action.groupby.aggregate;

import org.elasticsearch.common.io.stream.Streamable;

/**
 * State of a aggregation function
 *
 * Note on serialization:
 *      Use {@link AggStateReader} to read from StreamInput
 *
 *      Concrete implementations of AggState have to write a byte first in order to identify the class.
 *      And AggStateReader has to be extended if a new AggState implementation is added.
 */
public abstract class AggState implements Comparable<AggState>, Streamable {

    public abstract void merge(AggState other);
    public abstract Object value();
}
