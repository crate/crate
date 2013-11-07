package org.cratedb.action;

import org.cratedb.sql.CrateException;
import org.cratedb.sql.SQLReduceJobTimeoutException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
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

    private final ESLogger logger = Loggers.getLogger(getClass());
    private final TransportService transportService;
    private final ConcurrentMap<UUID, SQLReduceJobStatus> activeReduceJobs =
        ConcurrentCollections.newConcurrentMap();
    private final ClusterService clusterService;

    public static class Actions {
        public static final String START_REDUCE_JOB = "crate/sql/shard/reduce/start_job";
        public static final String RECEIVE_PARTIAL_RESULT = "crate/sql/shard/reduce/partial_result";
    }

    @Inject
    public TransportSQLReduceHandler(TransportService transportService, ClusterService clusterService) {
        this.clusterService = clusterService;
        this.transportService = transportService;
    }

    public void registerHandler() {
        transportService.registerHandler(Actions.START_REDUCE_JOB, new StartReduceJobHandler());
        transportService.registerHandler(
            Actions.RECEIVE_PARTIAL_RESULT, new RecievePartialResultHandler());
    }

    public SQLReduceJobResponse reduceOperationStart(SQLReduceJobRequest request) {
        logger.trace("received reduce job request. Creating context");

        SQLReduceJobStatus reduceJobStatus = new SQLReduceJobStatus(
            request.expectedShardResults, request.limit, request.orderByIndices
        );
        activeReduceJobs.put(request.contextId, reduceJobStatus);
        logger.trace("created context "
            + request.contextId + " on node " + clusterService.localNode().getId());

        try {
            if (!reduceJobStatus.shardsToProcess.await(2, TimeUnit.MINUTES)) {
                throw new SQLReduceJobTimeoutException();
            }

            return new SQLReduceJobResponse(reduceJobStatus);

        } catch (InterruptedException e) {
            throw new SQLReduceJobTimeoutException();
        } finally {
            activeReduceJobs.remove(request.contextId);
        }
    }

    private class StartReduceJobHandler implements TransportRequestHandler<SQLReduceJobRequest> {

        @Override
        public SQLReduceJobRequest newInstance() {
            return new SQLReduceJobRequest();
        }

        @Override
        public void messageReceived(SQLReduceJobRequest request, TransportChannel channel) throws Exception {
            try {
                SQLReduceJobResponse response = reduceOperationStart(request);
                channel.sendResponse(response);
            } catch (CrateException ex) {
                channel.sendResponse(ex);
            }
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
            SQLReduceJobStatus status;

            do {
                status = activeReduceJobs.get(request.contextId);

                /**
                 * Possible race condition.
                 * Mapper sends MapResult before the ReduceJob Context was established.
                 *
                 * TODO: use some kind of blockingConcurrentMap for activeReduceJobs or
                 * use some kind of listener pattern.
                 */
            } while (status == null);

            logger.trace("received mapper result for {} on node {}",
                request.contextId, clusterService.localNode().getId());

            synchronized (status.lock) {
                status.groupByResult.merge(request.groupByResult);
            }
            status.shardsToProcess.countDown();

            logger.trace("shards left: {}", status.shardsToProcess.getCount());
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SEARCH;
        }
    }
}
