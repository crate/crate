package org.cratedb.action;

import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.HandlesStreamInput;

import java.io.IOException;
import java.util.*;

public class ReduceJobStatusContext {

    private final Map<UUID, SQLReduceJobStatus> reduceJobs = new HashMap<>();
    private final Map<UUID, List<BytesReference>> unreadStreams = new HashMap<>();
    private final Object lock = new Object();
    private final CacheRecycler cacheRecycler;

    public CacheRecycler cacheRecycler() {
        return cacheRecycler;
    }

    public ReduceJobStatusContext(CacheRecycler cacheRecycler) {
        this.cacheRecycler = cacheRecycler;
    }

    public SQLReduceJobStatus get(UUID contextId) {
        synchronized (lock) {
            return reduceJobs.get(contextId);
        }
    }

    public void remove(UUID contextId) {
        reduceJobs.remove(contextId);
    }

    public void put(UUID contextId, SQLReduceJobStatus status) throws IOException {
        List<BytesReference> bytesReferences;
        synchronized (lock) {
            reduceJobs.put(contextId, status);
            bytesReferences = unreadStreams.get(contextId);
        }

        if (bytesReferences != null) {
            for (BytesReference bytes : bytesReferences) {
                mergeFromBytesReference(bytes, status);
            }
        }
    }

    public void push(final SQLMapperResultRequest request) throws IOException {
        if (request.groupByResult != null) {
            request.status.merge(request.groupByResult);
            return;
        }

        synchronized (lock) {
            SQLReduceJobStatus status = reduceJobs.get(request.contextId);
            if (status == null) {
                List<BytesReference> bytesStreamOutputs = unreadStreams.get(request.contextId);
                if (bytesStreamOutputs == null) {
                    bytesStreamOutputs = new ArrayList<>();
                    unreadStreams.put(request.contextId, bytesStreamOutputs);
                }

                bytesStreamOutputs.add(request.memoryOutputStream.bytes());
            } else {
                mergeFromBytesReference(request.memoryOutputStream.bytes(), status);
            }
        }
    }

    private void mergeFromBytesReference(BytesReference bytesReference, SQLReduceJobStatus status) throws IOException {
        SQLGroupByResult sqlGroupByResult = SQLGroupByResult.readSQLGroupByResult(
            status.parsedStatement,
            cacheRecycler,
            // required to wrap into HandlesStreamInput because it has a different readString()
            // implementation than BytesStreamInput alone.
            // the memoryOutputStream originates from a HandlesStreamOutput
            new HandlesStreamInput(new BytesStreamInput(bytesReference))
        );
        status.merge(sqlGroupByResult);
    }
}

