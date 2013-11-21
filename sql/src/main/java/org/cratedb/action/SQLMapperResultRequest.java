package org.cratedb.action;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

public class SQLMapperResultRequest extends TransportRequest {

    public UUID contextId;
    public SQLGroupByResult groupByResult;

    // fields below are only set/available on the receiver side.
    public SQLReduceJobStatus status;
    private Map<UUID, SQLReduceJobStatus> reduceJobs;

    public SQLMapperResultRequest() {

    }

    public SQLMapperResultRequest(Map<UUID, SQLReduceJobStatus> reduceJobs) {
        this.reduceJobs = reduceJobs;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        contextId = new UUID(in.readLong(), in.readLong());
        initStatus(contextId);
        groupByResult = SQLGroupByResult.readSQLGroupByResult(
            status.groupByResult.aggFunctions,
            status.groupByResult.aggExprs,
            in
        );
    }

    private void initStatus(UUID contextId) {
        do {
            status = reduceJobs.get(contextId);

            /**
             * Possible race condition.
             * Mapper sends MapResult before the ReduceJob Context was established.
             *
             * TODO: use some kind of blockingConcurrentMap for activeReduceJobs or
             * use some kind of listener pattern.
             */
        } while (status == null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(contextId.getMostSignificantBits());
        out.writeLong(contextId.getLeastSignificantBits());
        groupByResult.writeTo(out);
    }
}
