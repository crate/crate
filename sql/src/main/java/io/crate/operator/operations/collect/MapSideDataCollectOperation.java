/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operator.operations.collect;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.operator.Input;
import io.crate.operator.aggregation.CollectExpression;
import io.crate.operator.collector.CrateCollector;
import io.crate.operator.collector.SimpleOneRowCollector;
import io.crate.operator.operations.ImplementationSymbolVisitor;
import io.crate.operator.projectors.NoopProjector;
import io.crate.operator.projectors.ProjectionToProjectorVisitor;
import io.crate.operator.projectors.Projector;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectNode;
import org.cratedb.Constants;
import org.cratedb.sql.CrateException;
import org.cratedb.sql.TableUnknownException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.IndexShardMissingException;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.*;

/**
 * collect local data from node/shards/docs on nodes where the data resides (aka Mapper nodes)
 */
public class MapSideDataCollectOperation implements CollectOperation<Object[][]> {

    private ESLogger logger = Loggers.getLogger(getClass());

    private static class SimpleShardCollectFuture extends ShardCollectFuture {

        public SimpleShardCollectFuture(int numShards, List<Projector> projectorChain) {
            super(numShards, projectorChain);
        }

        @Override
        protected void onAllShardsFinished() {
            projectorChain.get(0).finishProjection();
            super.set(projectorChain.get(projectorChain.size() - 1).getRows());
        }
    }

    private final Functions functions;
    private final ReferenceResolver referenceResolver;
    private final IndicesService indicesService;
    protected final EvaluatingNormalizer nodeNormalizer;
    private final ThreadPool threadPool;
    protected final ClusterService clusterService;

    @Inject
    public MapSideDataCollectOperation(ClusterService clusterService,
                                       Functions functions,
                                       ReferenceResolver referenceResolver,
                                       IndicesService indicesService,
                                       ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.functions = functions;
        this.referenceResolver = referenceResolver;
        this.indicesService = indicesService;
        this.nodeNormalizer = new EvaluatingNormalizer(functions, RowGranularity.NODE, referenceResolver);
        this.threadPool = threadPool;
    }


    /**
     * dispatch by the following criteria:
     * <p/>
     * * if local node id is contained in routing:
     * * if no shards are given:
     * -> run node level collect
     * * if shards are given:
     * -> run shard or doc level collect
     * * else if we got cluster RowGranularity:
     * -> run node level collect (cluster level)
     */
    @Override
    public ListenableFuture<Object[][]> collect(CollectNode collectNode) {
        assert collectNode.isRouted(); // not routed collect is not handled here
        String localNodeId = clusterService.localNode().id();
        if (collectNode.executionNodes().contains(localNodeId)) {
            if (collectNode.routing().locations().get(localNodeId).size() == 0) {
                // node collect
                return handleNodeCollect(collectNode);
            } else {
                // shard or doc level
                return handleShardCollect(collectNode);
            }
        }
        throw new CrateException("unsupported routing");
    }

    /**
     * collect data on node level only - one row per node expected
     *
     * @param collectNode {@link io.crate.planner.node.dql.CollectNode} instance containing routing information and symbols to collect
     * @return the collect result from this node, one row only so return value is <code>Object[1][]</code>
     */
    protected ListenableFuture<Object[][]> handleNodeCollect(CollectNode collectNode) {
        SettableFuture<Object[][]> result = SettableFuture.create();
        collectNode = collectNode.normalize(nodeNormalizer);
        if (collectNode.whereClause().noMatch()) {
            result.set(Constants.EMPTY_RESULT);
            return result;
        }
        assert collectNode.toCollect().size() > 0;
        // resolve Implementations
        ImplementationSymbolVisitor.Context ctx = new ImplementationSymbolVisitor(
                this.referenceResolver,
                this.functions,
                collectNode.maxRowGranularity() // could be CLUSTER or NODE
        ).process(collectNode);
        List<Input<?>> inputs = ctx.topLevelInputs();
        Set<CollectExpression<?>> collectExpressions = ctx.collectExpressions();

        assert ctx.maxGranularity().ordinal() <= RowGranularity.NODE.ordinal() : "wrong RowGranularity";

        List<Projector> projectors = extractProjectors(collectNode);

        new SimpleOneRowCollector(inputs, collectExpressions, projectors.get(0)).doCollect();
        projectors.get(0).finishProjection();

        Object[][] collected = projectors.get(projectors.size() - 1).getRows();
        if (logger.isTraceEnabled()) {
            logger.trace("collected {} on {}-level from node {}",
                    Objects.toString(Arrays.asList(collected[0])),
                    collectNode.maxRowGranularity().name().toLowerCase(),
                    clusterService.localNode().id());
        }
        result.set(collected);
        return result;
    }

    /**
     * collect data on shard or doc level
     * <p/>
     * collects data from each shard in a separate thread,
     * collecting the data into a single state through an {@link java.util.concurrent.ArrayBlockingQueue}.
     *
     * @param collectNode {@link io.crate.planner.node.dql.CollectNode} containing routing information and symbols to collect
     * @return the collect results from all shards on this node that were given in {@link io.crate.planner.node.dql.CollectNode#routing}
     */
    protected ListenableFuture<Object[][]> handleShardCollect(CollectNode collectNode) {

        String localNodeId = clusterService.localNode().id();
        final int numShards = collectNode.routing().numShards(localNodeId);

        List<Projector> projectors = extractProjectors(collectNode);
        final ShardCollectFuture result = getShardCollectFuture(numShards, projectors, collectNode);

        collectNode = collectNode.normalize(nodeNormalizer);
        if (collectNode.whereClause().noMatch()) {
            result.onAllShardsFinished();
            return result;
        }

        List<CrateCollector> shardCollectors = new ArrayList<>(numShards);

        // get shardCollectors from single shards
        Map<String, Set<Integer>> shardIdMap = collectNode.routing().locations().get(localNodeId);
        for (Map.Entry<String, Set<Integer>> entry : shardIdMap.entrySet()) {
            IndexService indexService;
            try {
                indexService = indicesService.indexServiceSafe(entry.getKey());
            } catch (IndexMissingException e) {
                throw new TableUnknownException(entry.getKey(), e);
            }
            for (Integer shardId : entry.getValue()) {
                Injector shardInjector;
                try {
                    shardInjector = indexService.shardInjectorSafe(shardId);
                    ShardCollectService shardCollectService = shardInjector.getInstance(ShardCollectService.class);
                    CrateCollector crateCollector = shardCollectService.getCollector(collectNode, projectors.get(0));
                    shardCollectors.add(crateCollector);
                } catch (IndexShardMissingException e) {
                    throw new CrateException(
                            String.format("unknown shard id %d on index '%s'",
                                    shardId, entry.getKey()));
                } catch (Exception e) {
                    logger.error("Error while getting collector", e);
                    throw new CrateException(e);
                }
            }
        }

        // start the projection
        projectors.get(0).startProjection(); // finishProjection called in ShardCollectFuture

        // start shardCollectors
        for (final CrateCollector shardCollector : shardCollectors) {
            threadPool.executor(ThreadPool.Names.SEARCH).execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        shardCollector.doCollect();
                        result.shardFinished();
                    } catch (Exception ex) {
                        result.shardFailure(ex);
                    }
                    if (logger.isTraceEnabled()) {
                        logger.trace("shard finished collect, {} to go", result.numShards());
                    }
                }
            });
        }

        if (logger.isTraceEnabled()) {
            logger.trace("started {} shardCollectors", numShards);
        }

        return result;
    }

    protected List<Projector> extractProjectors(CollectNode collectNode) {
        ImplementationSymbolVisitor visitor = new ImplementationSymbolVisitor(
                this.referenceResolver,
                this.functions,
                RowGranularity.NODE
        );
        ProjectionToProjectorVisitor projectorVisitor = new ProjectionToProjectorVisitor(visitor);
        List<Projector> projectors = new ArrayList<>(collectNode.projections().size());
        if (collectNode.projections().size() == 0) {
            projectors.add(new NoopProjector());
        } else {
            projectors = projectorVisitor.process(collectNode.projections());
        }
        assert projectors.size() >= 1 : "no projectors";
        return projectors;
    }

    /**
     * chose the right ShardCollectFuture for this class
     *
     * @param numShards   number of shards until the result is considered complete
     * @param projectors  the projectors to process the collected rows
     * @param collectNode in case any other properties need to be extracted
     * @return a fancy ShardCollectFuture implementation
     */
    protected ShardCollectFuture getShardCollectFuture(int numShards, List<Projector> projectors, CollectNode collectNode) {
        return new SimpleShardCollectFuture(numShards, projectors);
    }
}
