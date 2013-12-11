package org.cratedb.action;

import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.transport.TransportRequest;

import java.io.*;
import java.util.UUID;

public class SQLMapperResultRequest extends TransportRequest {

    public boolean failed = false;

    // fields below are only set/available on the receiver side.
    final ESLogger logger = Loggers.getLogger(getClass());
    private CacheRecycler cacheRecycler;
    private ReduceJobStatusContext jobStatusContext;

    public UUID contextId;
    public SQLGroupByResult groupByResult;
    public BytesStreamOutput memoryOutputStream = new BytesStreamOutput();
    public SQLReduceJobStatus status;

    public SQLMapperResultRequest() {}
    public SQLMapperResultRequest(ReduceJobStatusContext jobStatusContext,
                                  CacheRecycler cacheRecycler) {
        this.jobStatusContext = jobStatusContext;
        this.cacheRecycler = cacheRecycler;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        try {
            contextId = new UUID(in.readLong(), in.readLong());
            status = jobStatusContext.get(contextId);
            failed = in.readBoolean();
            if (!failed){
                if (status == null) {
                    Streams.copy(in, memoryOutputStream);
                } else {
                    groupByResult = SQLGroupByResult.readSQLGroupByResult(
                        status.parsedStatement, cacheRecycler, in);
                }
            }
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
        out.writeBoolean(failed);
        if (!failed){
            groupByResult.writeTo(out);
        }
    }
}
