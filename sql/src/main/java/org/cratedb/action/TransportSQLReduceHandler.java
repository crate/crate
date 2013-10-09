package org.cratedb.action;

import org.cratedb.sql.SQLReduceJobTimeoutException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class TransportSQLReduceHandler {

    private final TransportService transportService;
    private final ConcurrentMap<UUID, SQLReduceJobStatus> activeReduceJobs =
        ConcurrentCollections.newConcurrentMap();

    public static class Actions {
        public static final String START_REDUCE_JOB = "crate/sql/shard/reduce/start_job";
        public static final String RECEIVE_PARTIAL_RESULT = "crate/sql/shard/reduce/partial_result";
    }

    @Inject
    public TransportSQLReduceHandler(TransportService transportService) {
        this.transportService = transportService;
    }

    public void registerHandler() {
        transportService.registerHandler(Actions.START_REDUCE_JOB, new StartReduceJobHandler());
        transportService.registerHandler(
            Actions.RECEIVE_PARTIAL_RESULT, new RecievePartialResultHandler());
    }

    private class StartReduceJobHandler implements TransportRequestHandler<SQLReduceJobRequest> {

        @Override
        public SQLReduceJobRequest newInstance() {
            return new SQLReduceJobRequest();
        }

        @Override
        public void messageReceived(SQLReduceJobRequest request, TransportChannel channel) throws Exception {
            SQLReduceJobStatus reduceJobStatus = new SQLReduceJobStatus(request.expectedShardResults);
            activeReduceJobs.put(request.contextId, reduceJobStatus);

            if (reduceJobStatus.shardsToProcess.await(2, TimeUnit.MINUTES)) {
                channel.sendResponse(new SQLReduceJobResponse(reduceJobStatus.groupByResult));
            } else {
                channel.sendResponse(new SQLReduceJobTimeoutException());
            }

            activeReduceJobs.remove(request.contextId);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SEARCH;
        }
    }

    private class RecievePartialResultHandler implements TransportRequestHandler<SQLMapperResultRequest> {
        @Override
        public SQLMapperResultRequest newInstance() {
            return new SQLMapperResultRequest();
        }

        @Override
        public void messageReceived(SQLMapperResultRequest request, TransportChannel channel) throws Exception {
            SQLReduceJobStatus status = activeReduceJobs.get(request.contextId);
            if (status == null) {
                throw new IllegalStateException("Received ReduceResult but don't have an active ReduceJob");
            }

            System.out.println("received mapper result");

            // TODO lock for merge
            status.groupByResult.merge(request.groupByResult);
            status.shardsToProcess.countDown();

            System.out.println("shards left: " + status.shardsToProcess.getCount());
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SEARCH;
        }
    }
}
