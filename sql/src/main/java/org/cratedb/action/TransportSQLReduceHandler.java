package org.cratedb.action;

import org.cratedb.Constants;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.service.SQLParseService;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TransportSQLReduceHandler {

    private final ESLogger logger = Loggers.getLogger(getClass());
    private final TransportService transportService;
    private final ReduceJobRequestContext reduceJobRequestContext;
    private final SQLParseService sqlParseService;
    private final ThreadPool threadPool;
    private final ScheduledExecutorService scheduledExecutorService;

    public static class Actions {
        public static final String START_REDUCE_JOB = "crate/sql/shard/reduce/start_job";
        public static final String RECEIVE_PARTIAL_RESULT = "crate/sql/shard/reduce/partial_result";
    }

    @Inject
    public TransportSQLReduceHandler(TransportService transportService,
                                     SQLParseService sqlParseService,
                                     CacheRecycler cacheRecycler,
                                     ThreadPool threadPool) {
        this.sqlParseService = sqlParseService;
        this.transportService = transportService;
        this.threadPool = threadPool;
        this.reduceJobRequestContext = new ReduceJobRequestContext(cacheRecycler);
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

        ReduceJobContext reduceJobStatus = new ReduceJobContext(
            parsedStatement,
            threadPool,
            request.expectedShardResults,
            request.contextId,
            reduceJobRequestContext
        );
        final WeakReference<ReduceJobContext> weakStatus = new WeakReference<>(reduceJobStatus);

        scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                ReduceJobContext status = weakStatus.get();
                if (status != null) {
                    status.timeout();
                }
            }
        }, Constants.GROUP_BY_TIMEOUT, TimeUnit.SECONDS);

        try {
            reduceJobRequestContext.put(request.contextId, reduceJobStatus);
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
            return new SQLMapperResultRequest(reduceJobRequestContext);
        }

        @Override
        public void messageReceived(final SQLMapperResultRequest request, TransportChannel channel) throws Exception {
            try {
                reduceJobRequestContext.push(request);
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
