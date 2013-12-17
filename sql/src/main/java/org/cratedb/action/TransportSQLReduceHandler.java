package org.cratedb.action;

import org.cratedb.Constants;
import org.cratedb.action.groupby.GroupByHelper;
import org.cratedb.action.groupby.GroupByKey;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.service.SQLParseService;
import org.cratedb.sql.CrateException;
import org.cratedb.sql.SQLReduceJobFailedException;
import org.cratedb.sql.SQLReduceJobTimeoutException;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.recycler.SoftThreadLocalRecycler;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TransportSQLReduceHandler {

    private final ESLogger logger = Loggers.getLogger(getClass());
    private final TransportService transportService;
    private final ReduceJobStatusContext reduceJobStatusContext;
    private final ClusterService clusterService;
    private final SQLParseService sqlParseService;
    private final CacheRecycler cacheRecycler;
    private final ThreadPool threadPool;
    private final ScheduledExecutorService scheduledExecutorService;

    public static class Actions {
        public static final String START_REDUCE_JOB = "crate/sql/shard/reduce/start_job";
        public static final String RECEIVE_PARTIAL_RESULT = "crate/sql/shard/reduce/partial_result";
    }

    @Inject
    public TransportSQLReduceHandler(TransportService transportService,
            ClusterService clusterService,
            SQLParseService sqlParseService,
            CacheRecycler cacheRecycler,
            ThreadPool threadPool) {
        this.cacheRecycler = cacheRecycler;
        this.sqlParseService = sqlParseService;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.threadPool = threadPool;
        this.reduceJobStatusContext = new ReduceJobStatusContext(cacheRecycler);
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(2);
    }

    public void registerHandler() {
        transportService.registerHandler(Actions.START_REDUCE_JOB, new StartReduceJobHandler());
        transportService.registerHandler(
            Actions.RECEIVE_PARTIAL_RESULT, new ReceivePartialResultHandler());
    }

    public ListenableActionFuture<SQLReduceJobResponse> reduceOperationStart(SQLReduceJobRequest request) {
        ParsedStatement parsedStatement =
            sqlParseService.parse(request.request.stmt(), request.request.args());

        SQLReduceJobStatus reduceJobStatus = new SQLReduceJobStatus(
            parsedStatement,
            threadPool,
            request.expectedShardResults,
            request.contextId,
            reduceJobStatusContext
        );
        final WeakReference<SQLReduceJobStatus> weakStatus = new WeakReference<>(reduceJobStatus);

        scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                SQLReduceJobStatus status = weakStatus.get();
                if (status != null) {
                    status.timeout();
                }
            }
        }, Constants.GROUP_BY_TIMEOUT, TimeUnit.SECONDS);

        try {
            reduceJobStatusContext.put(request.contextId, reduceJobStatus);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        return reduceJobStatus;
    }

    private class StartReduceJobHandler extends BaseTransportRequestHandler<SQLReduceJobRequest> {

        @Override
        public SQLReduceJobRequest newInstance() {
            return new SQLReduceJobRequest();
        }

        @Override
        public void messageReceived(final SQLReduceJobRequest request,
                                    final TransportChannel channel) throws Exception {
            reduceOperationStart(request).addListener(new ActionListener<SQLReduceJobResponse>() {
                @Override
                public void onResponse(SQLReduceJobResponse response) {
                    try {
                        channel.sendResponse(response);
                    } catch (IOException e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (IOException e1) {
                        logger.error(e1.getMessage(), e1);
                    }
                }
            });
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SEARCH;
        }
    }

    private class ReceivePartialResultHandler extends BaseTransportRequestHandler<SQLMapperResultRequest> {
        @Override
        public SQLMapperResultRequest newInstance() {
            return new SQLMapperResultRequest(reduceJobStatusContext);
        }

        @Override
        public void messageReceived(final SQLMapperResultRequest request, TransportChannel channel) throws Exception {
            try {
                reduceJobStatusContext.push(request);
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        @Override
        public String executor() {
            return ThreadPool.Names.MERGE;
        }
    }
}
