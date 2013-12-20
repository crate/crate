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
    private ReduceJobRequestContext jobStatusContext;

    public UUID contextId;
    public SQLGroupByResult groupByResult;
    public BytesStreamOutput memoryOutputStream = new BytesStreamOutput();
    public ReduceJobContext status;

    public SQLMapperResultRequest() {}
    public SQLMapperResultRequest(ReduceJobRequestContext jobStatusContext) {
        this.jobStatusContext = jobStatusContext;
        cacheRecycler = jobStatusContext.cacheRecycler();
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
            throw e;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        try {
            out.writeLong(contextId.getMostSignificantBits());
            out.writeLong(contextId.getLeastSignificantBits());
            out.writeBoolean(failed);
            if (!failed){
                groupByResult.writeTo(out);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }
}
