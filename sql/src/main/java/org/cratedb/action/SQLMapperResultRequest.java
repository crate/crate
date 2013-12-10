package org.cratedb.action;

import org.cratedb.core.concurrent.FutureConcurrentMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class SQLMapperResultRequest extends TransportRequest {

    final ESLogger logger = Loggers.getLogger(getClass());

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
        try {
            contextId = new UUID(in.readLong(), in.readLong());

            status = reduceJobs.get(contextId, 30, TimeUnit.SECONDS);

            groupByResult = SQLGroupByResult.readSQLGroupByResult(
                status.rowSerializationContext,
                in
            );
        } catch (Exception e ) {
            logger.error(e.getMessage(), e);
            throw new IOException(e);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(contextId.getMostSignificantBits());
        out.writeLong(contextId.getLeastSignificantBits());
        groupByResult.writeTo(out);
    }
}
