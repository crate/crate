package org.cratedb.action;

import org.cratedb.core.concurrent.FutureConcurrentMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class SQLMapperResultRequest extends TransportRequest {

    public UUID contextId;
    public SQLGroupByResult groupByResult;

    // fields below are only set/available on the receiver side.
    public SQLReduceJobStatus status;
    private FutureConcurrentMap<UUID, SQLReduceJobStatus> reduceJobs;

    public SQLMapperResultRequest() {

    }

    public SQLMapperResultRequest(FutureConcurrentMap<UUID, SQLReduceJobStatus> reduceJobs) {
        this.reduceJobs = reduceJobs;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        contextId = new UUID(in.readLong(), in.readLong());
        try {
            status = reduceJobs.get(contextId, 30, TimeUnit.SECONDS);
        } catch (Exception e ) {
            throw new IOException(e);
        }
        groupByResult = SQLGroupByResult.readSQLGroupByResult(
            status.parsedStatement.aggregateExpressions,
            in
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(contextId.getMostSignificantBits());
        out.writeLong(contextId.getLeastSignificantBits());
        groupByResult.writeTo(out);
    }
}
