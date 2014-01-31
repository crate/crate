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

import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.sun.org.apache.xalan.internal.xsltc.compiler.sym;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.NormalizationHelper;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.metadata.Routing;
import io.crate.metadata.shard.ShardReferenceResolver;
import io.crate.operator.Input;
import io.crate.operator.aggregation.CollectExpression;
import io.crate.operator.operations.ImplementationSymbolVisitor;
import io.crate.operator.projectors.NoopProjector;
import io.crate.operator.projectors.Projector;
import io.crate.planner.RowGranularity;
import io.crate.planner.plan.CollectNode;
import io.crate.planner.projections.Projection;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolType;
import org.cratedb.sql.CrateException;
import org.cratedb.sql.TableUnknownException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexShardMissingException;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * collect local data from node/shards/docs on shards
 */
public class LocalDataCollectOperation implements CollectOperation<Object[][]> {

    public static final Object[][] EMPTY_RESULT = new Object[0][];
    public static final TimeValue timeout = new TimeValue(2, TimeUnit.MINUTES);


    /**
     * future that is set after configured number of shards signal
     * that they have finished collecting.
     */
    static class ShardCollectFuture extends AbstractFuture<Object[][]> {
        private final AtomicInteger numShards;
        private final List<Projector> projectorChain;

        public ShardCollectFuture(int numShards, List<Projector> projectorChain) {
            this.numShards = new AtomicInteger(numShards);
            this.projectorChain = projectorChain;
        }

        protected void shardFinished() {
            if (numShards.decrementAndGet() <= 0) {
                projectorChain.get(0).finishProjection();
                super.set(projectorChain.get(projectorChain.size()-1).getRows());
            }
        }
    }

    private ESLogger logger = Loggers.getLogger(getClass());

    private final Functions functions;
    private final ReferenceResolver referenceResolver;
    private final IndicesService indicesService;
    private final EvaluatingNormalizer normalizer;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;

    @Inject
    public LocalDataCollectOperation(ClusterService clusterService,
                                     Functions functions,
                                     ReferenceResolver referenceResolver,
                                     IndicesService indicesService,
                                     ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.functions = functions;
        this.referenceResolver = referenceResolver;
        this.indicesService = indicesService;
        this.normalizer = new EvaluatingNormalizer(functions, RowGranularity.NODE, referenceResolver);
        this.threadPool = threadPool;
    }



    @Override
    public ListenableFuture<Object[][]> collect(CollectNode collectNode) {
        assert collectNode.routing().hasLocations();
        String localNodeId = clusterService.localNode().id();
        Routing routing = collectNode.routing();
        // assert we have at least a node routing to this node
        Preconditions.checkState(
                routing.nodes().contains(localNodeId),
                "unsupported routing"
        );

        if (collectNode.routing().locations().get(localNodeId).size() == 0) {
            // node collect
            return handleNodeCollect(collectNode);
        } else {
            // shard or doc level
            return handleShardCollect(collectNode);
        }
    }


    /**
     * collect data on node level only - one row per node expected
     * @param collectNode {@link io.crate.planner.plan.CollectNode} instance containing routing information and symbols to collect
     * @return the collect result from this node, one row only so return value is <code>Object[1][]</code>
     */
    private ListenableFuture<Object[][]> handleNodeCollect(CollectNode collectNode) {
        SettableFuture<Object[][]> result = SettableFuture.create();
        Optional<Function> whereClause = collectNode.whereClause();
        if (whereClause.isPresent() && NormalizationHelper.evaluatesToFalse(whereClause.get(), this.normalizer)
                || collectNode.toCollect() == null || collectNode.toCollect().length == 0) {
            result.set(EMPTY_RESULT);
            return result;
        }

        // resolve Implementations
        ImplementationSymbolVisitor.Context ctx = new ImplementationSymbolVisitor(
                this.referenceResolver,
                this.functions,
                RowGranularity.NODE
        ).process(collectNode);
        Input<?>[] inputs = ctx.topLevelInputs();
        Set<CollectExpression<?>> collectExpressions = ctx.collectExpressions();

        assert ctx.maxGranularity().ordinal() <= RowGranularity.NODE.ordinal() : "wrong RowGranularity";

        List<Projector> projectors = extractProjectors(collectNode);
        new SimpleOneRowCollector(inputs, collectExpressions, projectors.get(0)).collect();
        result.set(projectors.get(projectors.size() - 1).getRows());
        return result;
    }

    /**
     * collect data on shard level only - one row per shard expected
     *
     * collects data from each shard in a seperate thread,
     * collecting the data into a single state through an {@link java.util.concurrent.ArrayBlockingQueue}.
     *
     * @param collectNode {@link io.crate.planner.plan.CollectNode} containing routing information and symbols to collect
     * @return the collect results from all shards on this node that were given in {@link io.crate.planner.plan.CollectNode#routing}
     */
    private ListenableFuture<Object[][]> handleShardCollect(CollectNode collectNode) {

        String localNodeId = clusterService.localNode().id();

        Optional<Function> whereClause = collectNode.whereClause();
        if (whereClause.isPresent() && NormalizationHelper.evaluatesToFalse(whereClause.get(), this.normalizer)) {
            SettableFuture<Object[][]> result = SettableFuture.create();
            result.set(EMPTY_RESULT);
            return result;
        }
        final int numShards = collectNode.routing().numShards(localNodeId);
        List<Runnable> shardCollectors = new ArrayList<>(numShards);


        List<Projector> projectors = extractProjectors(collectNode);
        RowGranularity maxRowgranularity = getMaxGranularity(collectNode);

        final ShardCollectFuture result = new ShardCollectFuture(numShards, projectors);

        // get shardCollectors from single shards
        Map<String, Set<Integer>> shardIdMap = collectNode.routing().locations().get(localNodeId);
        for (Map.Entry<String, Set<Integer>> entry : shardIdMap.entrySet()) {
            IndexService indexService;
            try {
                indexService = indicesService.indexServiceSafe(entry.getKey());
            } catch(IndexMissingException e) {
                throw new TableUnknownException(entry.getKey(), e);
            }

            for (Integer shardId : entry.getValue()) {
                Injector shardInjector;
                try {
                    shardInjector = indexService.shardInjectorSafe(shardId);
                } catch(IndexShardMissingException e) {
                    throw new CrateException(
                            String.format("unknown shard id %d on index '%s'",
                                    shardId, entry.getKey()));
                }
                switch(maxRowgranularity) {
                    case SHARD:
                        shardCollectors.add(
                                new SimpleShardCollector(
                                        shardInjector.getInstance(Functions.class),
                                        shardInjector.getInstance(ReferenceResolver.class),
                                        collectNode,
                                        projectors.get(0),
                                        result)
                        );
                        break;
                    case DOC:
                    default:
                        throw new CrateException("unsupported row granularity");
                }

            }
        }

        // start shardCollectors
        for (Runnable shardCollector : shardCollectors ) {
            threadPool.executor(ThreadPool.Names.SEARCH).execute(shardCollector);
        }
        if (logger.isTraceEnabled()) {
            logger.trace("started {} shardCollectors", numShards);
        }

        return result;
    }

    private List<Projector> extractProjectors(CollectNode collectNode) {
        ImplementationSymbolVisitor visitor = new ImplementationSymbolVisitor(
                this.referenceResolver,
                this.functions,
                RowGranularity.NODE
        );
        List<Projector> projectors = new ArrayList<>(collectNode.projections().size());
        if (projectors.size() == 0) {
            projectors.add(new NoopProjector());
        } else {
            Projector lastProjector = null;
            for (Projection projection : collectNode.projections()) {
                Projector p = projection.getProjector(visitor);
                if (lastProjector != null) {
                    lastProjector.setDownStream(p);
                }
                projectors.add(p);
                lastProjector = p;
            }
        }
        assert projectors.size() >= 1 : "no projectors";
        return projectors;
    }

    private RowGranularity getMaxGranularity(CollectNode collectNode) {
        RowGranularity maxRowGranularity = RowGranularity.CLUSTER;
        for (Symbol symbol : collectNode.toCollect()) {
            if (symbol.symbolType() == SymbolType.REFERENCE) {
                RowGranularity symbolGranularity = ((Reference)symbol).info().granularity();
                if (symbolGranularity.compareTo(maxRowGranularity) > 0) {
                    maxRowGranularity = symbolGranularity;

                }
            }
        }
        return maxRowGranularity;
    }
}
