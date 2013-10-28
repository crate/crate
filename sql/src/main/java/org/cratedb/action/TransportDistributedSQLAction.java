package org.cratedb.action;

import com.google.common.collect.MinMaxPriorityQueue;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.GroupByRowComparator;
import org.cratedb.action.groupby.aggregate.AggState;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.SQLRequest;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.service.SQLParseService;
import org.cratedb.sql.CrateException;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
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
    final String executor = ThreadPool.Names.SEARCH;
    final String transportShardAction = "crate/sql/shard/gather";

    @Inject
    protected TransportDistributedSQLAction(Settings settings,
                                            ThreadPool threadPool,
                                            ClusterService clusterService,
                                            TransportService transportService,
                                            SQLParseService sqlParseService,
                                            TransportSQLReduceHandler transportSQLReduceHandler,
                                            SQLQueryService sqlQueryService) {
        super(settings, threadPool);
        this.sqlParseService = sqlParseService;
        this.sqlQueryService = sqlQueryService;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.transportSQLReduceHandler = transportSQLReduceHandler;
    }

    @Override
    protected void doExecute(DistributedSQLRequest request, ActionListener<SQLResponse> listener) {
        new AsyncBroadcastAction(request, listener).start();
    }


    protected SQLShardRequest newShardRequest(SQLRequest request, int shardId, UUID contextId,
                                              String[] reducers) {
        SQLShardRequest shardRequest = new SQLShardRequest();
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
        logger.trace("shard operation on: {} shard: {}", clusterService.localNode().getId(), request.shardId);

        try {
            Map<String, Map<Integer, GroupByRow>> distributedCollectResult =
                sqlQueryService.query(request.reducers, stmt, request.shardId);

            for (String reducer : request.reducers) {

                SQLMapperResultRequest mapperResultRequest = new SQLMapperResultRequest();
                mapperResultRequest.contextId = request.contextId;
                mapperResultRequest.groupByResult = new SQLGroupByResult(distributedCollectResult.get(reducer));

                DiscoveryNode node = clusterService.state().getNodes().get(reducer);
                transportService.submitRequest(
                    node,
                    TransportSQLReduceHandler.Actions.RECEIVE_PARTIAL_RESULT,
                    mapperResultRequest,
                    TransportRequestOptions.options(),
                    EmptyTransportResponseHandler.INSTANCE_SAME
                );
            }
        } catch (Exception e) {
            throw new CrateException(e);
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
        private final AtomicLong reduceResponseCounter;
        private final AtomicBoolean done;
        private final MinMaxPriorityQueue<GroupByRow> groupByResult;

        AsyncBroadcastAction(DistributedSQLRequest request, ActionListener<SQLResponse> listener) {
            this.parsedStatement = request.parsedStatement;
            this.sqlRequest = request.sqlRequest;
            this.listener = listener;

            MinMaxPriorityQueue.Builder<GroupByRow> rowBuilder =
                MinMaxPriorityQueue.orderedBy(new GroupByRowComparator(parsedStatement.orderByIndices()));

            if (parsedStatement.limit != null) {
                rowBuilder.maximumSize(parsedStatement.limit);
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
            done = new AtomicBoolean(false);

            reduceResponseCounter = new AtomicLong(reducers.length);
        }

        public void start() {
            if (expectedShardResponses == 0 || reducers.length == 0) {
                try {
                    listener.onResponse(
                        new SQLResponse(parsedStatement.cols(), new Object[0][0], 0L));
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
                        rowCount
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

                for (Map.Entry<Integer, AggState> aggStateEntry : row.aggregateStates.entrySet()) {
                    rows[currentRow][aggStateEntry.getKey()] = aggStateEntry.getValue().value();
                }

                for (Map.Entry<Integer, Object> objectEntry : row.regularColumns.entrySet()) {
                    rows[currentRow][objectEntry.getKey()] = objectEntry.getValue();
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
                contextId, expectedShardResponses, parsedStatement.limit,
                parsedStatement.orderByIndices()
            );

            if (reducer.equals(nodes.getLocalNodeId())) {
                threadPool.executor(executor).execute(new Runnable() {
                    @Override
                    public void run() {
                        onReduceJobResponse(
                            transportSQLReduceHandler.reduceOperationStart(request));
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
                    performMapperOperation(contextId, shard, shardIndex);
                } else {
                    onMapperOperation(null, shardIndex,
                        new NoShardAvailableActionException(shardIt.shardId()));
                }
            }
        }

        private void performMapperOperation(UUID contextId, ShardRouting shard, int shardIndex) {
            assert shard != null;

            SQLShardRequest shardRequest = newShardRequest(sqlRequest, shard.id(), contextId, reducers);
            if (shardOnLocalNode(shard)) {
                executeMapperLocal(shard, shardRequest, shardIndex);
            } else {
                executeMapperRemote(shard, shardRequest, shardIndex);
            }
        }

        private void executeMapperRemote(final ShardRouting shard,
                                         final SQLShardRequest shardRequest,
                                         final int shardIndex)
        {
            DiscoveryNode node = nodes.get(shard.currentNodeId());
            if (node == null) {
                onMapperOperation(shard, shardIndex,
                    new NoShardAvailableActionException(shard.shardId()));
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
                        onMapperOperation(shard, shardIndex, response);

                    }

                    @Override
                    public void handleException(TransportException exp) {
                        onMapperOperation(shard, shardIndex, exp);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }
                });
        }


        private void executeMapperLocal(final ShardRouting shard,
                                        final SQLShardRequest shardRequest,
                                        final int shardIndex) {
            threadPool.executor(executor).execute(new Runnable() {
                @Override
                public void run() {
                    onMapperOperation(shard, shardIndex, shardOperation(shardRequest));
                }
            });
        }

        private void onMapperOperation(ShardRouting shard, int shardIndex,
                                       SQLShardResponse sqlShardResponse) {
            // response with result is sent from the reducers. Nothing to do here.
        }

        private void onMapperOperation(ShardRouting shard, int shardIndex, Throwable e) {
            // if one shard fails the reducer will run in a timeout because it waits for all shard results
            // here we can return early so the client doesn't have to wait.
            if (!done.get()) {
                done.set(true);
                listener.onFailure(e);
            }
            logger.error("Error from shard {}", e, shard.id());
        }

        private boolean shardOnLocalNode(ShardRouting shard) {
            return shard.currentNodeId().equals(nodes.getLocalNodeId());
        }

        private void onReduceJobResponse(SQLReduceJobResponse response) {
            synchronized (groupByResult) {
                Collections.addAll(groupByResult, response.result);
            }

            if (reduceResponseCounter.decrementAndGet() == 0) {
                sendSqlResponse();
            }
        }

        class ReduceTransportResponseHandler extends BaseTransportResponseHandler<SQLReduceJobResponse> {

            public ReduceTransportResponseHandler() {
            }

            @Override
            public SQLReduceJobResponse newInstance() {
                return new SQLReduceJobResponse();
            }

            @Override
            public void handleResponse(SQLReduceJobResponse response) {
                onReduceJobResponse(response);
            }

            @Override
            public void handleException(TransportException exp) {
                if (!done.get()) {
                    done.set(true);
                    listener.onFailure(exp.getRootCause());
                }
                logger.error("Failure while reducing group by result", exp.getRootCause());
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
