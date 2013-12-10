package org.cratedb.action.groupby.aggregate.distinct;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public abstract class TypedSeenSerializer<T> {

    public abstract T readFrom(StreamInput in) throws IOException;
    public abstract void writeTo(StreamOutput out, T value) throws IOException;
}
