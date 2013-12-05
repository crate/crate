package org.cratedb.action;

import org.cratedb.action.groupby.GroupByHelper;
import org.cratedb.action.groupby.GroupByKey;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.GroupByRowComparator;
import org.cratedb.action.sql.*;
import org.cratedb.core.collections.LimitingCollectionIterator;
import org.cratedb.service.SQLParseService;
import org.cratedb.service.StatsService;
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
import org.elasticsearch.common.StopWatch;
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
 *      Will receive a partial result from each mapper and reduce that result
 *      The merged result is then sent to the original handler node for a final reduce.
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
 *  r -> r: reduce ReduceResult
 *  r -> r: numMapperRequests -= 1
 *  r -> r: wait for reduceResult of all mappers
 *  r -> h: send SQLReduceJobResponse
 *  h -> h: reduce reduceResult
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
    private final NodeExecutionContext nodeExecutionContext; // TODO: put this into a service class
    private final StatsService statsService;
    private final TransportSQLReduceHandler transportSQLReduceHandler;
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
                                            NodeExecutionContext nodeExecutionContext,
                                            StatsService statsService) {
        super(settings, threadPool);
        this.sqlParseService = sqlParseService;
        this.sqlQueryService = sqlQueryService;
        this.nodeExecutionContext = nodeExecutionContext;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.statsService = statsService;
        this.transportSQLReduceHandler = transportSQLReduceHandler;
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
        StopWatch stopWatch = null;
        CrateException exception = null;
        Map<String, Map<GroupByKey, GroupByRow>> distributedCollectResult = null;
        List<List<Object>> collectResult = null;

        if (logger.isTraceEnabled()) {
            stopWatch = new StopWatch().start();
            logger.trace("[{}] context {} shard operation shard: {}",
                clusterService.localNode().getId(),
                request.contextId,
                request.shardId
            );
        }

        try {
            // all virtual tableNames of the schema `stats` are saved as `virtualTableName`
            // inside the ParsedStatement. The stmt.tableName optional extracted from the WHERE
            // clause is used for routing to the relevant shards.
            if (!stmt.hasGroupBy() && stmt.isStatsQuery() && !stmt.isGlobalAggregate()) {
                collectResult = statsService.query(stmt.virtualTableName(),
                    request.concreteIndex, stmt, request.shardId, clusterService.localNode().id());
            } else {
                if (stmt.isStatsQuery()) {
                    distributedCollectResult =
                            statsService.queryGroupBy(
                                    stmt.virtualTableName(),
                                    request.reducers, request.concreteIndex,
                                    stmt, request.shardId,
                                    clusterService.localNode().id());
                } else {
                    distributedCollectResult =
                        sqlQueryService.query(request.reducers, request.concreteIndex, stmt, request.shardId);
                }
            }
        } catch (CrateException e) {
            exception = e;
        } catch (Exception e) {
            exception = new CrateException(e);
        }


        if (logger.isTraceEnabled()) {
            assert stopWatch != null;
            stopWatch.stop();
            logger.trace("[{}] context: {} shard {} index {} collecting took {} ms",
                clusterService.localNode().getId(),
                request.contextId,
                request.shardId,
                request.concreteIndex,
                stopWatch.totalTime().getMillis()
            );
        }

        if (exception != null) {
            // create an empty fake result to send to the reducers.
            distributedCollectResult = createEmptyCollectResult(request);
        }

        for (String reducer : request.reducers) {
            if (logger.isTraceEnabled()) {
                stopWatch = new StopWatch().start();
            }

            SQLMapperResultRequest mapperResultRequest = new SQLMapperResultRequest();
            mapperResultRequest.contextId = request.contextId;
            mapperResultRequest.groupByResult =
                new SQLGroupByResult(distributedCollectResult.get(reducer).values());

            // TODO: could be optimized if reducerNode == mapperNode to avoid transportService
            DiscoveryNode node = clusterService.state().getNodes().get(reducer);
            transportService.submitRequest(
                node,
                TransportSQLReduceHandler.Actions.RECEIVE_PARTIAL_RESULT,
                mapperResultRequest,
                TransportRequestOptions.options(),
                EmptyTransportResponseHandler.INSTANCE_SAME
            );

            if (logger.isTraceEnabled()) {
                assert stopWatch != null;
                stopWatch.stop();
                logger.trace("[{}] context: {} shard {} sending to reducer {} (write to buffer) took {} ms",
                    clusterService.localNode().getId(),
                    request.contextId,
                    request.shardId,
                    reducer,
                    stopWatch.totalTime().getMillis()
                );
            }
        }

        // throw the exception after a result has been sent to the reducers so that they won't wait
        // for a mapper result which they will never receive.
        if (exception != null) {
            throw exception;
        }

        if (collectResult != null && collectResult.size() > 0) {
            if (logger.isTraceEnabled()) {
                logger.trace("[{}] context: {} shard {} index {} collected {} results, sending back to main handler",
                        clusterService.localNode().getId(),
                        request.contextId,
                        request.shardId,
                        request.concreteIndex,
                        collectResult.size()
                );
            }

            SQLShardResponse sqlShardResponse = newShardResponse();
            sqlShardResponse.results = collectResult;
            return sqlShardResponse;
        }

        return new SQLShardResponse();
    }

    private Map<String, Map<GroupByKey, GroupByRow>> createEmptyCollectResult(SQLShardRequest request) {
        Map<String, Map<GroupByKey, GroupByRow>> distributedCollectResult = new HashMap<>(request.reducers.length);
        for (String reducer : request.reducers) {
            distributedCollectResult.put(reducer, new HashMap<GroupByKey, GroupByRow>(0));
        }
        return distributedCollectResult;
    }

    protected GroupShardsIterator shards(ClusterState clusterState,
                                         String[] indices,
                                         String[] concreteIndices,
                                         boolean allShards) {
        if (!allShards) {
            return clusterService.operationRouting().searchShards(
                clusterState, indices, concreteIndices, null, null
            );
        } else {
            return clusterState.routingTable().allAssignedShardsGrouped(concreteIndices, false);
        }
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
        private final List<GroupByRow> groupByResult;
        private final List<List> mapperResults;
        private final ITableExecutionContext tableExecutionContext;
        private final UUID contextId;
        private final String[] concreteIndices;

        AsyncBroadcastAction(DistributedSQLRequest request, ActionListener<SQLResponse> listener) {
            this.parsedStatement = request.parsedStatement;
            this.sqlRequest = request.sqlRequest;
            this.listener = listener;

            groupByResult = new ArrayList<>(parsedStatement.totalLimit());
            mapperResults = new ArrayList<>();
            clusterState = clusterService.state();

            // TODO: TransportBroadcastOperationAction does checkGlobalBlock, required?

            String[] indices = parsedStatement.indices();
            String tableName;
            if (indices == null || indices.length == 0) {
                // resolve all available indices
                concreteIndices = clusterState.metaData().concreteAllIndices();
                indices = concreteIndices;
                tableName = parsedStatement.schemaName();
            } else {
                tableName = parsedStatement.tableName();
                // resolve aliases to the concreteIndices
                concreteIndices = clusterState.metaData().concreteIndices(
                    indices, IgnoreIndices.NONE, true
                );
            }

            nodes = clusterState.nodes();
            shardsIts = shards(clusterState, indices, concreteIndices, parsedStatement.isStatsQuery());
            expectedShardResponses = shardsIts.size();

            if (parsedStatement.partialReducerCount < 0) {
                /**
                 * distinct requires that one reducer has a complete set of all seenValues
                 * in order for the {@link org.cratedb.action.groupby.aggregate.AggState#terminatePartial()}
                 * to generate the correct value.
                 */
                reducers = extractNodes(shardsIts, this.parsedStatement.hasDistinctAggregate ? 1 : -1);
            } else {
                reducers = new String[0];
            }

            lastException = new AtomicReference<>(null);
            shardErrors = new AtomicBoolean(false);
            reducerErrors = new AtomicBoolean(false);

            reduceResponseCounter = new AtomicLong(reducers.length);
            shardResponseCounter = new AtomicLong(expectedShardResponses);

            // TODO: put this into a service class
            tableExecutionContext = nodeExecutionContext.tableContext(
                    parsedStatement.schemaName(), tableName);

            contextId = UUID.randomUUID();
        }

        public void start() {
            if (expectedShardResponses == 0) {
                try {
                    listener.onResponse(
                        new SQLResponse(parsedStatement.cols(), new Object[0][0], 0L,
                                sqlRequest.creationTime()));
                } catch (Throwable e) {
                    listener.onFailure(e);
                }
            }

            sendReduceRequests();
            sendShardRequests();
        }

        public void sendSqlResponse() {
            StopWatch stopWatch = null;

            // we got all reducer results, lets merge them with unassigned/empty shard results
            if (parsedStatement.isStatsQuery()) {
                try {
                    collectUnassignedShardStats(clusterState, concreteIndices);
                } catch (Throwable e) {
                    listener.onFailure(e);
                }
            }


            if (logger.isTraceEnabled()) {
                stopWatch = new StopWatch().start();
                logger.trace(
                    "[{}]: context: {} got all reduceJob responses. Sorting and generating SQLResponse",
                    clusterService.localNode().getId(),
                    contextId
                );
            }

            Object[][] rows;

            if (reducers.length > 0) {
                // here extractors without mapping are used because for sorting there is no need
                // to cast the values to the correct type.
                GroupByRowComparator comparator = new GroupByRowComparator(
                        GroupByHelper.buildFieldExtractor(parsedStatement, null),
                        parsedStatement.orderByIndices()
                );
                Collections.sort(groupByResult, comparator);
                rows = GroupByHelper.sortedRowsToObjectArray(
                        new LimitingCollectionIterator<>(groupByResult, parsedStatement.totalLimit()),
                        parsedStatement,
                        GroupByHelper.buildFieldExtractor(parsedStatement, tableExecutionContext.mapper())
                );
            } else {
                SQLRowComparator comparator = new SQLRowComparator(parsedStatement.orderByIndices());
                Collections.sort(mapperResults, comparator);
                rows = SQLShardResultHelper.sortedRowsToObjectArray(
                        new LimitingCollectionIterator<>(mapperResults, parsedStatement.totalLimit()),
                        parsedStatement
                );
            }

            if (logger.isTraceEnabled()) {
                assert stopWatch != null;
                stopWatch.stop();
                logger.trace(
                    "[{}]: context: {} sorted and prepared rows for SQLResponse. Took {} ms",
                    clusterService.localNode().getId(),
                    contextId,
                    stopWatch.totalTime().getMillis()
                );
            }

            listener.onResponse(
                new SQLResponse(parsedStatement.cols(), rows, rows.length, sqlRequest.creationTime())
            );
        }

        /**
         * extract nodes of shards shardIterator
         * @param shardsIts ShardIterator of shards containing a certain index
         * @param numberOfNodes the maximum number of nodes to extract, if -1 all available nodes will be extracted
         * @return Array of nodeIds
         */
        private String[] extractNodes(GroupShardsIterator shardsIts, int numberOfNodes) {
            Set<String> nodes = newHashSet();

            if (numberOfNodes < 0) { numberOfNodes = Integer.MAX_VALUE; }

            ShardRouting shardRouting;
            for (ShardIterator shardsIt : shardsIts) {
                while ((shardRouting = shardsIt.nextOrNull()) != null && nodes.size() < numberOfNodes) {
                    nodes.add(shardRouting.currentNodeId());
                }
            }

            return nodes.toArray(new String[nodes.size()]);
        }

        private void sendReduceRequests() {
            for (String reducer : reducers) {
                performReduceOperation(reducer);
            }

        }

        private void performReduceOperation(String reducer) {
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

        private void sendShardRequests() {
            for (final ShardIterator shardIt : shardsIts) {
                final ShardRouting shard = shardIt.firstOrNull();
                if (shard != null) {
                    performMapperOperation(shard, shard.index());
                } else {
                    onMapperFailure(new NoShardAvailableActionException(shardIt.shardId()));
                }
            }
        }

        private void performMapperOperation(ShardRouting shard, String concreteIndex) {
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
                        onMapperOperation(response);

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
                        SQLShardResponse response = shardOperation(shardRequest);
                        onMapperOperation(response);
                    } catch (Exception e) {
                        onMapperFailure(e);
                    }
                }
            });
        }

        private void onMapperOperation(SQLShardResponse response) {
            shardResponseCounter.decrementAndGet();
            if (response != null && response.results != null) {
                onMapperResults(response.results);
            }
            tryFinishResponse();
        }

        private void onMapperFailure(Throwable e) {
            shardErrors.set(true);
            logger.error("mapper failure", e);
            lastException.set(e);
            onMapperOperation(null);
        }

        private boolean shardOnLocalNode(ShardRouting shard) {
            return shard.currentNodeId().equals(nodes.getLocalNodeId());
        }

        private void tryFinishResponse() {
            if (shardResponseCounter.get() == 0 &&
                    (reduceResponseCounter.get() == 0 || reducers.length == 0)) {
                if (reducerErrors.get() || shardErrors.get()) {
                    listener.onFailure(lastException.get());
                } else {
                    sendSqlResponse();
                }
            }
        }

        private void onReduceJobResponse(SQLReduceJobResponse response) {
            StopWatch stopWatch = null;
            if (logger.isTraceEnabled()) {
                logger.trace("[{}]: context: {} received response from reducer.",
                    clusterService.localNode().getId(),
                    contextId
                );
                stopWatch = new StopWatch().start();
            }

            synchronized (groupByResult) {
                groupByResult.addAll(response.result);
            }

            if (logger.isTraceEnabled()) {
                assert stopWatch != null;
                stopWatch.stop();
                logger.trace("[{}]: context: {} adding reduceJob took {} ms",
                    clusterService.localNode().getId(),
                    contextId,
                    stopWatch.totalTime().getMillis());
            }

            reduceResponseCounter.decrementAndGet();
            tryFinishResponse();
        }

        private void onReduceJobFailure(Throwable e) {
            reducerErrors.set(true);
            logger.error(e.getMessage(), e);
            lastException.set(e);
            reduceResponseCounter.decrementAndGet();
            tryFinishResponse();
        }

        private void onMapperResults(List<List<Object>> results) {
            if (results != null && results.size() > 0) {
                synchronized (mapperResults) {
                    mapperResults.addAll(results);
                }
            }
        }

        /**
         * Collect stats from unassigned/empty shards.
         * On groupBy all existing results are merged together with the unassigned shards results,
         * so it is very important, that this method is called AFTER all partial reducers
         * finished.
         *
         * @param clusterState
         * @param concreteIndices
         * @throws Exception
         */
        private void collectUnassignedShardStats(ClusterState clusterState,
                                                               String[] concreteIndices)
                                                                        throws Exception {
            // until we find an easy ways to get only unassigned shards,
            // fetch all including unassigned(empty)
            final GroupShardsIterator shardsItsAll =
                    clusterState.routingTable().allAssignedShardsGrouped(concreteIndices, true);

            // use a dummy reducer for the groupByRow partitioning logic
            String dummyReducer = "dummyReducer";

            CrateException exception = null;
            SQLReduceJobStatus reduceJobStatus = null;
            List<List<Object>> collectResults = null;
            if (parsedStatement.hasGroupBy()) {
                // create a reduce job without a shard CountDownLatch as we collect all shards
                // at once here
                reduceJobStatus = new SQLReduceJobStatus(parsedStatement);
            } else {
                collectResults = new ArrayList<>(shardsItsAll.size() - shardsIts.size());
            }

            // empty/unassigned shards don't have an index attribute, but they are listed always
            // after assigned ones.
            String lastIndex = null;
            for (final ShardIterator shardIt : shardsItsAll) {
                final ShardRouting shard = shardIt.firstOrNull();
                if (shard == null) {
                    try {
                        if (parsedStatement.hasGroupBy() || parsedStatement.isGlobalAggregate()) {
                            Map<String, Map<GroupByKey, GroupByRow>> shardGroupByCollectResults =
                                    statsService.queryGroupBy(
                                            parsedStatement.virtualTableName(),
                                            new String[]{dummyReducer}, lastIndex,
                                            parsedStatement, shardIt.shardId().id(),
                                            null);
                            // merge results to the reduce job
                            reduceJobStatus.merge(new SQLGroupByResult
                                    (shardGroupByCollectResults.get(dummyReducer).values()));
                        } else {
                            List<List<Object>> shardCollectResults = statsService.query(
                                    parsedStatement.virtualTableName(),
                                    lastIndex, parsedStatement, shardIt.shardId().id(),
                                    null);
                            // add results
                            if (shardCollectResults != null) {
                                collectResults.addAll(shardCollectResults);
                            }
                        }
                    } catch (CrateException e) {
                        exception = e;
                    } catch (Exception e) {
                        exception = new CrateException(e);
                    }

                    if (exception != null) {
                        throw exception;
                    }
                } else {
                    lastIndex = shard.index();
                }
            }

            if (!parsedStatement.hasGroupBy()) {
                // add all results
                onMapperResults(collectResults);
            } else {
                // reduce existing results with results from unassigned/empty shards
                synchronized (groupByResult) {
                    reduceJobStatus.merge(new SQLGroupByResult(groupByResult));
                    groupByResult.clear();
                    groupByResult.addAll(
                            reduceJobStatus.trimRows(reduceJobStatus.reducedResult.values())
                    );
                }

            }
        }

        class ReduceTransportResponseHandler extends BaseTransportResponseHandler<SQLReduceJobResponse> {

            public ReduceTransportResponseHandler() {
            }

            @Override
            public SQLReduceJobResponse newInstance() {
                return new SQLReduceJobResponse(parsedStatement);
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
                return ThreadPool.Names.MERGE;
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
