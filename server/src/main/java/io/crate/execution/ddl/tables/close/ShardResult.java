package io.crate.execution.ddl.tables.close;


import java.io.IOException;

import javax.annotation.Nullable;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.CollectionUtils;

public final class ShardResult implements Writeable {

    private final int id;
    private final Failure[] failures;

    public ShardResult(final int id, final Failure[] failures) {
        this.id = id;
        this.failures = failures;
    }

    ShardResult(final StreamInput in) throws IOException {
        this.id = in.readVInt();
        this.failures = in.readOptionalArray(Failure::readFailure, Failure[]::new);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeVInt(id);
        out.writeOptionalArray(failures);
    }

    public boolean hasFailures() {
        return CollectionUtils.isEmpty(failures) == false;
    }

    public int getId() {
        return id;
    }

    public Failure[] getFailures() {
        return failures;
    }

    public static class Failure extends DefaultShardOperationFailedException {

        private @Nullable String nodeId;

        private Failure(StreamInput in) throws IOException {
            super(in);
            nodeId = in.readOptionalString();
        }

        public Failure(final String index, final int shardId, final Throwable reason) {
            this(index, shardId, reason, null);
        }

        public Failure(final String index, final int shardId, final Throwable reason, final String nodeId) {
            super(index, shardId, reason);
            this.nodeId = nodeId;
        }

        public String getNodeId() {
            return nodeId;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(nodeId);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        static Failure readFailure(final StreamInput in) throws IOException {
            return new Failure(in);
        }
    }
}
