package org.cratedb.action;

import com.google.common.collect.MinMaxPriorityQueue;
import org.cratedb.action.groupby.GroupByKey;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.GroupByRowComparator;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.SQLRequest;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.service.SQLParseService;
import org.cratedb.sql.CrateException;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.Sets.newHashSet;

/**
 * Transport for SQLRequests that require Grouping
 *
 * Every DistributedSQLRequest initiates a MapReduce Job that is sent to Mappers and to Reducers
 *
 *
 * HandlerNode:
 *      Receives DistributedSQLRequest. Sends JobInfo and Query to Mapper and Reducers
 *      Will receive partial results from the reducers which are then further reduced
 *      into one final result.
 *
 * Mappers / QueryShard:
 *      Queries the Lucene index.
 *      The result is partitioned and grouped by key
 *      The partial results are then sent to the reducers.
 *
 * Reducers:
 *      Will receive a partial result from each mapper and merge that result
 *      The merged result is then sent to the original handler node for a final merge.
 *
 *  @startuml
 *  actor "Caller" as c
 *  participant "Handler" as h
 *  participant "MapperShard" as ms
 *  participant "Reducer" as r
 *
 *  c -> h: doExecute(SQLRequest)
 *  h -> h: parseQuery
 *  h -> h: new DistributedSQLRequest(parsedQuery, sqlRequest)
 *  h -> r: new SQLReduceJobRequest(contextId, numMapperRequests)
 *  h -> ms: SQLShardRequest(contextId, reducers)
 *  ms -> ms: Query Lucene
 *  ms -> r: SQLMapperResultRequest(contextId)
 *  r -> r: merge ReduceResult
 *  r -> r: numMapperRequests -= 1
 *  r -> r: wait for reduceResult of all mappers
 *  r -> h: send SQLReduceJobResponse
 *  h -> h: merge reduceResult
 *  h -> h: wait for reduceResult of all reducers
 *  h -> h: reduceResult to SQLResponse
 *  h -> c: SQLResponse
 *  @enduml
 */
public class TransportDistributedSQLAction extends TransportAction<DistributedSQLRequest, SQLResponse>
{
    private final ESLogger logger = Loggers.getLogger(getClass());

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final SQLParseService sqlParseService;
    private final SQLQueryService sqlQueryService;
    private final TransportSQLReduceHandler transportSQLReduceHandler;
    private final Map<String, AggFunction> aggFunctionMap;
    final String executor = ThreadPool.Names.SEARCH;
    final String transportShardAction = "crate/sql/shard/gather";

    @Inject
    protected TransportDistributedSQLAction(Settings settings,
                                            ThreadPool threadPool,
                                            ClusterService clusterService,
                                            TransportService transportService,
                                            SQLParseService sqlParseService,
                                            TransportSQLReduceHandler transportSQLReduceHandler,
                                            SQLQueryService sqlQueryService,
                                            Map<String, AggFunction> aggFunctionMap) {
        super(settings, threadPool);
        this.sqlParseService = sqlParseService;
        this.sqlQueryService = sqlQueryService;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.transportSQLReduceHandler = transportSQLReduceHandler;
        this.aggFunctionMap = aggFunctionMap;
    }

    @Override
    protected void doExecute(DistributedSQLRequest request, ActionListener<SQLResponse> listener) {
        new AsyncBroadcastAction(request, listener).start();
    }


    protected SQLShardRequest newShardRequest(SQLRequest request, String concreteIndex,
                                              int shardId, UUID contextId, String[] reducers) {
        SQLShardRequest shardRequest = new SQLShardRequest();
        shardRequest.concreteIndex = concreteIndex;
        shardRequest.shardId = shardId;
        shardRequest.contextId = contextId;
        shardRequest.sqlRequest = request;

        shardRequest.reducers = reducers;
        return shardRequest;
    }

    protected SQLShardResponse newShardResponse() {
        return new SQLShardResponse();
    }

    protected SQLShardResponse shardOperation(SQLShardRequest request) throws ElasticSearchException {
        ParsedStatement stmt = sqlParseService.parse(request.sqlRequest.stmt(), request.sqlRequest.args());
        long now = 0;

        if (logger.isTraceEnabled()) {
            logger.trace("shard operation on: {} shard: {}", clusterService.localNode().getId(), request.shardId);
            now = new Date().getTime();
        }


        CrateException exception = null;
        Map<String, Map<GroupByKey, GroupByRow>> distributedCollectResult = null;

        try {
            distributedCollectResult =
                sqlQueryService.query(request.reducers, request.concreteIndex, stmt, request.shardId);
        } catch (CrateException e) {
            exception = e;
        } catch (Exception e) {
            exception = new CrateException(e);
        }

        if (exception != null) {
            // create an empty fake result to send to the reducers.
            distributedCollectResult = new HashMap<>();
            for (String reducer : request.reducers) {
                distributedCollectResult.put(reducer, new HashMap<GroupByKey, GroupByRow>());
            }
        }

        long numResults = 0;
        for (String reducer : request.reducers) {

            SQLMapperResultRequest mapperResultRequest = new SQLMapperResultRequest();
            mapperResultRequest.contextId = request.contextId;
            mapperResultRequest.groupByResult =
                new SQLGroupByResult(distributedCollectResult.get(reducer).values());

            numResults += mapperResultRequest.groupByResult.size();

            // TODO: could be optimized if reducerNode == mapperNode to avoid transportService
            DiscoveryNode node = clusterService.state().getNodes().get(reducer);
            transportService.submitRequest(
                node,
                TransportSQLReduceHandler.Actions.RECEIVE_PARTIAL_RESULT,
                mapperResultRequest,
                TransportRequestOptions.options(),
                EmptyTransportResponseHandler.INSTANCE_SAME
            );
        }

        if (logger.isTraceEnabled()) {
            logger.trace("[{}] shard: {} collecting {} results took {} ms",
                clusterService.localNode().getId(), request.shardId, numResults, (new Date().getTime() - now));
        }

        // throw the exception after a result has been sent to the reducers so that they won't wait
        // for a mapper result which they will never receive.
        if (exception != null) {
            throw exception;
        }

        return new SQLShardResponse();
    }

    protected GroupShardsIterator shards(ClusterState clusterState,
                                         String[] indices,
                                         String[] concreteIndices) {
        return clusterService.operationRouting().searchShards(
            clusterState, indices, concreteIndices, null, null
        );
    }

    public void registerHandler() {
        transportService.registerHandler(transportShardAction, new ShardTransportHandler());
    }

    class AsyncBroadcastAction {

        private final ParsedStatement parsedStatement;
        private final SQLRequest sqlRequest;
        private final ActionListener<SQLResponse> listener;
        private final ClusterState clusterState;
        private final GroupShardsIterator shardsIts;
        private final String[] reducers;
        private final int expectedShardResponses;
        private final DiscoveryNodes nodes;
        private final AtomicLong shardResponseCounter;
        private final AtomicLong reduceResponseCounter;
        private final AtomicBoolean reducerErrors;
        private final AtomicBoolean shardErrors;
        private final AtomicReference<Throwable> lastException;
        private final MinMaxPriorityQueue<GroupByRow> groupByResult;

        AsyncBroadcastAction(DistributedSQLRequest request, ActionListener<SQLResponse> listener) {
            this.parsedStatement = request.parsedStatement;
            this.sqlRequest = request.sqlRequest;
            this.listener = listener;

            MinMaxPriorityQueue.Builder<GroupByRow> rowBuilder =
                MinMaxPriorityQueue.orderedBy(
                    new GroupByRowComparator(parsedStatement.idxMap, parsedStatement.orderByIndices()));

            if (parsedStatement.limit != null) {
                rowBuilder.maximumSize(parsedStatement.limit);
            } else {
                rowBuilder.maximumSize(SQLParseService.DEFAULT_SELECT_LIMIT);
            }
            this.groupByResult = rowBuilder.create();
            clusterState = clusterService.state();

            // TODO: TransportBroadcastOperationAction does checkGlobalBlock, required?

            // resolve aliases to the concreteIndices
            String[] concreteIndices = clusterState.metaData().concreteIndices(
                parsedStatement.indices(), IgnoreIndices.NONE, true
            );

            nodes = clusterState.nodes();
            shardsIts = shards(clusterState, parsedStatement.indices(), concreteIndices);
            expectedShardResponses = shardsIts.size();
            reducers = extractNodes(shardsIts);

            lastException = new AtomicReference<>(null);
            shardErrors = new AtomicBoolean(false);
            reducerErrors = new AtomicBoolean(false);

            reduceResponseCounter = new AtomicLong(reducers.length);
            shardResponseCounter = new AtomicLong(expectedShardResponses);
        }

        public void start() {
            if (expectedShardResponses == 0 || reducers.length == 0) {
                try {
                    listener.onResponse(
                        new SQLResponse(parsedStatement.cols(), new Object[0][0], 0L,
                                sqlRequest.creationTime()));
                } catch (Throwable e) {
                    listener.onFailure(e);
                }
            }

            UUID contextId = UUID.randomUUID();
            sendReduceRequests(contextId);
            sendShardRequests(contextId);
        }

        public void sendSqlResponse() {
            long rowCount = groupByResult.size();
            try {
                listener.onResponse(
                    new SQLResponse(parsedStatement.cols(),
                        groupbyResultToRows(parsedStatement, groupByResult),
                        rowCount,
                        sqlRequest.creationTime()
                    )
                );
            } catch (Throwable e) {
                listener.onFailure(e);
            }
        }

        private Object[][] groupbyResultToRows(ParsedStatement parsedStatement,
                                               MinMaxPriorityQueue<GroupByRow> groupByResult) {
            Object[][] rows = new Object[groupByResult.size()][parsedStatement.outputFields().size()];

            GroupByRow row;
            int currentRow = -1;
            while ( (row = groupByResult.pollFirst()) != null) {
                currentRow++;

                for (int c = 0; c < parsedStatement.outputFields().size(); c++) {
                    rows[currentRow][c] = row.get(parsedStatement.idxMap[c]);
                }
            }

            return rows;
        }

        private String[] extractNodes(GroupShardsIterator shardsIts) {
            Set<String> nodes = newHashSet();

            ShardRouting shardRouting;
            for (ShardIterator shardsIt : shardsIts) {
                while ((shardRouting = shardsIt.nextOrNull()) != null) {
                    nodes.add(shardRouting.currentNodeId());
                }
            }

            return nodes.toArray(new String[nodes.size()]);
        }

        private void sendReduceRequests(UUID contextId) {
            for (String reducer : reducers) {
                performReduceOperation(reducer, contextId);
            }

        }

        private void performReduceOperation(String reducer, UUID contextId) {
            DiscoveryNode node = nodes.get(reducer);
            if (node == null) {
                throw new NodeNotConnectedException(node, "Can't perform reduce operation on node");
            }

            final SQLReduceJobRequest request = new SQLReduceJobRequest(
                contextId,
                expectedShardResponses,
                sqlRequest
            );

            if (reducer.equals(nodes.getLocalNodeId())) {
                threadPool.executor(executor).execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            onReduceJobResponse(
                                transportSQLReduceHandler.reduceOperationStart(request));
                        } catch (Exception e) {
                            onReduceJobFailure(e);
                        }
                    }
                });
            } else {
                transportService.sendRequest(node,
                    TransportSQLReduceHandler.Actions.START_REDUCE_JOB,
                    request,
                    TransportRequestOptions.options(),
                    new ReduceTransportResponseHandler()
                );
            }
        }

        private void sendShardRequests(UUID contextId) {
            int shardIndex = -1;
            for (final ShardIterator shardIt : shardsIts) {
                shardIndex++;
                final ShardRouting shard = shardIt.firstOrNull();
                if (shard != null) {
                    performMapperOperation(contextId, shard, shard.index());
                } else {
                    onMapperFailure(new NoShardAvailableActionException(shardIt.shardId()));
                }
            }
        }

        private void performMapperOperation(UUID contextId, ShardRouting shard, String concreteIndex) {
            assert shard != null;

            SQLShardRequest shardRequest = newShardRequest(
                sqlRequest, concreteIndex, shard.id(), contextId, reducers
            );
            if (shardOnLocalNode(shard)) {
                executeMapperLocal(shardRequest);
            } else {
                executeMapperRemote(shard, shardRequest);
            }
        }

        private void executeMapperRemote(final ShardRouting shard,
                                         final SQLShardRequest shardRequest)
        {
            DiscoveryNode node = nodes.get(shard.currentNodeId());
            if (node == null) {
                onMapperFailure(new NoShardAvailableActionException(shard.shardId()));
                return;
            }

            transportService.sendRequest(node, transportShardAction, shardRequest,
                new BaseTransportResponseHandler<SQLShardResponse>() {
                    @Override
                    public SQLShardResponse newInstance() {
                        return newShardResponse();
                    }

                    @Override
                    public void handleResponse(SQLShardResponse response) {
                        onMapperOperation();

                    }

                    @Override
                    public void handleException(TransportException exp) {
                        onMapperFailure(exp);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }
                });
        }


        private void executeMapperLocal(final SQLShardRequest shardRequest) {
            threadPool.executor(executor).execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        shardOperation(shardRequest);
                        onMapperOperation();
                    } catch (Exception e) {
                        onMapperFailure(e);
                    }
                }
            });
        }

        private void onMapperOperation() {
            shardResponseCounter.decrementAndGet();
            tryFinishResponse();
        }

        private void onMapperFailure(Throwable e) {
            shardErrors.set(true);
            lastException.set(e);
            onMapperOperation();
        }

        private boolean shardOnLocalNode(ShardRouting shard) {
            return shard.currentNodeId().equals(nodes.getLocalNodeId());
        }

        private void tryFinishResponse() {
            if (shardResponseCounter.get() == 0 && reduceResponseCounter.get() == 0) {
                if (reducerErrors.get() || shardErrors.get()) {
                    listener.onFailure(lastException.get());
                } else {
                    sendSqlResponse();
                }
            }
        }

        private void onReduceJobResponse(SQLReduceJobResponse response) {
            synchronized (groupByResult) {
                Collections.addAll(groupByResult, response.result);
            }

            reduceResponseCounter.decrementAndGet();
            tryFinishResponse();
        }

        private void onReduceJobFailure(Throwable e) {
            reducerErrors.set(true);
            lastException.set(e);
            reduceResponseCounter.decrementAndGet();
            tryFinishResponse();
        }

        class ReduceTransportResponseHandler extends BaseTransportResponseHandler<SQLReduceJobResponse> {

            public ReduceTransportResponseHandler() {
            }

            @Override
            public SQLReduceJobResponse newInstance() {
                return new SQLReduceJobResponse(aggFunctionMap, parsedStatement);
            }

            @Override
            public void handleResponse(SQLReduceJobResponse response) {
                onReduceJobResponse(response);
            }

            @Override
            public void handleException(TransportException exp) {
                onReduceJobFailure(exp.getRootCause());
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }
        }
    }

    class ShardTransportHandler extends BaseTransportRequestHandler<SQLShardRequest> {

        @Override
        public SQLShardRequest newInstance() {
            return new SQLShardRequest();
        }

        @Override
        public void messageReceived(final SQLShardRequest request, final TransportChannel channel)
            throws Exception
        {
            channel.sendResponse(shardOperation(request));
        }

        @Override
        public String executor() {
            return executor;
        }
    }
}
