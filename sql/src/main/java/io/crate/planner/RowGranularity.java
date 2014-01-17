package io.crate.planner;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public enum RowGranularity {

    DOC, SHARD, NODE, CLUSTER;

    public static RowGranularity fromStream(StreamInput in) throws IOException {
        return RowGranularity.values()[in.readVInt()];
    }

    public static void toStream(RowGranularity granularity, StreamOutput out) throws IOException {
        out.writeVInt(granularity.ordinal());
    }
}
