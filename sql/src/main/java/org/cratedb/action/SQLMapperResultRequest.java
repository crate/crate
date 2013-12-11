package org.cratedb.action;

import org.cratedb.core.concurrent.FutureConcurrentMap;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class SQLMapperResultRequest extends TransportRequest {

    private CacheRecycler cacheRecycler;
    public UUID contextId;
    public SQLGroupByResult groupByResult;
    public boolean failed = false;

    // fields below are only set/available on the receiver side.
    public SQLReduceJobStatus status;
    private FutureConcurrentMap<UUID, SQLReduceJobStatus> reduceJobs;

    public SQLMapperResultRequest() {

    }

    public SQLMapperResultRequest(
            FutureConcurrentMap<UUID, SQLReduceJobStatus> reduceJobs,
            CacheRecycler cacheRecycler) {
        this.reduceJobs = reduceJobs;
        this.cacheRecycler = cacheRecycler;
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
        failed = in.readBoolean();
        if (!failed){
            groupByResult = SQLGroupByResult.readSQLGroupByResult(status.parsedStatement,
                    cacheRecycler, in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(contextId.getMostSignificantBits());
        out.writeLong(contextId.getLeastSignificantBits());
        out.writeBoolean(failed);
        if (!failed){
            groupByResult.writeTo(out);
        }
    }
}
