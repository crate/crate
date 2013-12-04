package org.cratedb.action;

import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.core.concurrent.FutureConcurrentMap;
import org.cratedb.service.SQLParseService;
import org.cratedb.sql.CrateException;
import org.cratedb.sql.SQLReduceJobTimeoutException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class TransportSQLReduceHandler {

    private final ESLogger logger = Loggers.getLogger(getClass());
    private final TransportService transportService;
    private final FutureConcurrentMap<UUID, SQLReduceJobStatus> activeReduceJobs = FutureConcurrentMap.newMap();
    private final ClusterService clusterService;
    private final SQLParseService sqlParseService;

    public static class Actions {
        public static final String START_REDUCE_JOB = "crate/sql/shard/reduce/start_job";
        public static final String RECEIVE_PARTIAL_RESULT = "crate/sql/shard/reduce/partial_result";
    }

    @Inject
    public TransportSQLReduceHandler(TransportService transportService,
                                     ClusterService clusterService,
                                     SQLParseService sqlParseService) {
        this.sqlParseService = sqlParseService;
        this.clusterService = clusterService;
        this.transportService = transportService;
    }

    public void registerHandler() {
        transportService.registerHandler(Actions.START_REDUCE_JOB, new StartReduceJobHandler());
        transportService.registerHandler(
            Actions.RECEIVE_PARTIAL_RESULT, new RecievePartialResultHandler());
    }

    public SQLReduceJobResponse reduceOperationStart(SQLReduceJobRequest request) {
        ParsedStatement parsedStatement =
            sqlParseService.parse(request.request.stmt(), request.request.args());

        SQLReduceJobStatus reduceJobStatus = new SQLReduceJobStatus(
            parsedStatement, request.expectedShardResults
        );

        activeReduceJobs.put(request.contextId, reduceJobStatus);

        long now = 0;
        if (logger.isTraceEnabled()) {
            logger.trace("[{}]: context {} Received SQLReduce Job and created context",
                clusterService.localNode().getId(), request.contextId
            );
            now = new Date().getTime();
        }

        try {
            if (!reduceJobStatus.shardsToProcess.await(2, TimeUnit.MINUTES)) {
                throw new SQLReduceJobTimeoutException();
            }

            logger.trace("[{}]: context: {} completed SQLReduceJob. Took {} ms",
                clusterService.localNode().id(), request.contextId, (new Date().getTime() - now));

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

        @Override
        public boolean isForceExecution() {
            return true;
        }
    }

    private class RecievePartialResultHandler implements TransportRequestHandler<SQLMapperResultRequest> {
        @Override
        public SQLMapperResultRequest newInstance() {
            return new SQLMapperResultRequest(activeReduceJobs);
        }

        @Override
        public void messageReceived(SQLMapperResultRequest request, TransportChannel channel) throws Exception {
            SQLReduceJobStatus status = request.status;

            long now = 0;
            if (logger.isTraceEnabled()) {
                logger.trace("[{}]: context {} received result from mapper",
                    clusterService.localNode().getId(),
                    request.contextId
                );
                now = new Date().getTime();
            }

            status.merge(request.groupByResult);

            if (logger.isTraceEnabled()) {
                logger.trace("[{}]: context {} merging mapper result took {} ms. Now we got {} results",
                    clusterService.localNode().getId(),
                    request.contextId,
                    (new Date().getTime() - now),
                    status.reducedResult.size()
                );
            }

            status.shardsToProcess.countDown();

            if (logger.isTraceEnabled()) {
                logger.trace("[{}]: context {} shards left: {}",
                    clusterService.localNode().getId(),
                    request.contextId,
                    status.shardsToProcess.getCount()
                );
            }

            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SEARCH;
        }

        @Override
        public boolean isForceExecution() {
            return true;
        }
    }
}
