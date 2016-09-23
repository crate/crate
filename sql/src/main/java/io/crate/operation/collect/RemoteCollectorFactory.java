/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.collect;

import io.crate.analyze.EvaluatingNormalizer;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.TreeMapBuilder;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.jobs.JobContextService;
import io.crate.metadata.Functions;
import io.crate.metadata.NestedReferenceResolver;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.collect.collectors.RemoteCollector;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.ProjectorFactory;
import io.crate.operation.projectors.RowReceiver;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.Projections;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.*;

/**
 * Used to create RemoteCollectors
 * Those RemoteCollectors act as proxy collectors to collect data from a shard located on another node.
 */
@Singleton
public class RemoteCollectorFactory {

    private static final int SENDER_PHASE_ID = 0;
    private final ClusterService clusterService;
    private final Functions functions;
    private final ThreadPool threadPool;
    private final JobContextService jobContextService;
    private final Settings settings;
    private final TransportActionProvider transportActionProvider;
    private final BulkRetryCoordinatorPool bulkRetryCoordinatorPool;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final EvaluatingNormalizer normalizer;
    private final ImplementationSymbolVisitor implementationVisitor;

    @Inject
    public RemoteCollectorFactory(ClusterService clusterService,
                                  Functions functions,
                                  ThreadPool threadPool,
                                  JobContextService jobContextService,
                                  Settings settings,
                                  TransportActionProvider transportActionProvider,
                                  BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                                  IndexNameExpressionResolver indexNameExpressionResolver,
                                  NestedReferenceResolver referenceResolver) {
        this.clusterService = clusterService;
        this.functions = functions;
        this.threadPool = threadPool;
        this.jobContextService = jobContextService;
        this.settings = settings;
        this.transportActionProvider = transportActionProvider;
        this.bulkRetryCoordinatorPool = bulkRetryCoordinatorPool;
        this.indexNameExpressionResolver = indexNameExpressionResolver;

        normalizer = new EvaluatingNormalizer(functions, RowGranularity.NODE, referenceResolver);
        implementationVisitor = new ImplementationSymbolVisitor(functions);
    }

    /**
     * create a RemoteCollector
     * The RemoteCollector will collect data from another node using a wormhole as if it was collecting on this node.
     * <p>
     * This should only be used if a shard is not available on the current node due to a relocation
     */
    public CrateCollector.Builder createCollector(String index,
                                                  Integer shardId,
                                                  RoutedCollectPhase collectPhase,
                                                  final RamAccountingContext ramAccountingContext) {
        final UUID childJobId = UUID.randomUUID(); // new job because subContexts can't be merged into an existing job

        IndexShardRoutingTable shardRoutings = clusterService.state().routingTable().shardRoutingTable(index, shardId);
        // for update operations primaryShards must be used
        // (for others that wouldn't be the case, but at this point it is not easily visible which is the case)
        ShardRouting shardRouting = shardRoutings.primaryShard();

        final String remoteNodeId = shardRouting.currentNodeId();
        assert remoteNodeId != null : "primaryShard not assigned :(";
        final String localNodeId = clusterService.localNode().id();
        final RoutedCollectPhase newCollectPhase = createNewCollectPhase(childJobId, collectPhase, index, shardId, remoteNodeId);

        Collection<? extends Projection> shardProjections = Projections.shardProjections(collectPhase.projections());
        final FlatProjectorChain.Builder chainBuilder;
        if (shardProjections.isEmpty()) {
            chainBuilder = null;
        } else {
            ProjectorFactory projectorFactory = new ProjectionToProjectorVisitor(
                clusterService,
                functions,
                indexNameExpressionResolver,
                threadPool,
                settings,
                transportActionProvider,
                bulkRetryCoordinatorPool,
                implementationVisitor,
                normalizer,
                new ShardId(index, shardId));

            chainBuilder = new FlatProjectorChain.Builder(
                collectPhase.jobId(),
                ramAccountingContext,
                projectorFactory,
                shardProjections
            );
        }

        return new CrateCollector.Builder() {
            @Override
            public CrateCollector build(RowReceiver rowReceiver) {
                if (chainBuilder != null) {
                    FlatProjectorChain chain = chainBuilder.build(rowReceiver);
                    chain.prepare();
                    rowReceiver = chain.firstProjector();
                }
                return new RemoteCollector(
                    childJobId,
                    localNodeId,
                    remoteNodeId,
                    transportActionProvider.transportJobInitAction(),
                    transportActionProvider.transportKillJobsNodeAction(),
                    jobContextService,
                    ramAccountingContext,
                    rowReceiver,
                    newCollectPhase);
            }
        };
    }

    private RoutedCollectPhase createNewCollectPhase(
        UUID childJobId, RoutedCollectPhase collectPhase, String index, Integer shardId, String nodeId) {

        Routing routing = new Routing(TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder().put(nodeId,
            TreeMapBuilder.<String, List<Integer>>newMapBuilder().put(index, Collections.singletonList(shardId)).map()).map());
        return new RoutedCollectPhase(
            childJobId,
            SENDER_PHASE_ID,
            collectPhase.name(),
            routing,
            collectPhase.maxRowGranularity(),
            collectPhase.toCollect(),

            // the projector chain is already here on this node, don't need to run projections on the other node
            Collections.<Projection>emptyList(),
            collectPhase.whereClause(),
            DistributionInfo.DEFAULT_BROADCAST
        );
    }
}
