/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.collect.sources;

import io.crate.action.job.SharedShardContext;
import io.crate.action.job.SharedShardContexts;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.exceptions.TableUnknownException;
import io.crate.exceptions.UnhandledServerException;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.Functions;
import io.crate.metadata.PartitionName;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.Paging;
import io.crate.operation.RowDownstream;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.ShardCollectService;
import io.crate.operation.collect.ShardProjectorChain;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.ProjectorFactory;
import io.crate.operation.reference.sys.node.NodeSysExpression;
import io.crate.operation.reference.sys.node.NodeSysReferenceResolver;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectPhase;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexShardMissingException;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;

@Singleton
public class ShardCollectSource implements CollectSource {

    private static final ESLogger LOGGER = Loggers.getLogger(ShardCollectSource.class);

    private final Settings settings;
    private final IndicesService indicesService;
    private final Functions functions;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final TransportActionProvider transportActionProvider;
    private final BulkRetryCoordinatorPool bulkRetryCoordinatorPool;
    private final UnassignedShardsCollectSource unassignedShardsCollectSource;
    private final NodeSysExpression nodeSysExpression;

    @Inject
    public ShardCollectSource(Settings settings,
                              IndicesService indicesService,
                              Functions functions,
                              ClusterService clusterService,
                              ThreadPool threadPool,
                              TransportActionProvider transportActionProvider,
                              BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                              UnassignedShardsCollectSource unassignedShardsCollectSource,
                              NodeSysExpression nodeSysExpression) {
        this.settings = settings;
        this.indicesService = indicesService;
        this.functions = functions;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.transportActionProvider = transportActionProvider;
        this.bulkRetryCoordinatorPool = bulkRetryCoordinatorPool;
        this.unassignedShardsCollectSource = unassignedShardsCollectSource;
        this.nodeSysExpression = nodeSysExpression;
    }

    @Override
    public Collection<CrateCollector> getCollectors(CollectPhase collectPhase, RowDownstream downstream, JobCollectContext jobCollectContext) {
        NodeSysReferenceResolver referenceResolver = new NodeSysReferenceResolver(nodeSysExpression);
        ImplementationSymbolVisitor implementationSymbolVisitor = new ImplementationSymbolVisitor(
                referenceResolver,
                functions,
                RowGranularity.NODE
        );
        EvaluatingNormalizer nodeNormalizer = new EvaluatingNormalizer(functions,
                RowGranularity.NODE,
                referenceResolver);
        CollectPhase normalizedPhase = collectPhase.normalize(nodeNormalizer);
        ProjectorFactory projectorFactory = new ProjectionToProjectorVisitor(
                clusterService,
                threadPool,
                settings,
                transportActionProvider,
                bulkRetryCoordinatorPool,
                implementationSymbolVisitor
        );

        String localNodeId = clusterService.localNode().id();
        int numShardsEstimate = normalizedPhase.routing().numShards(localNodeId);
        ShardProjectorChain projectorChain = new ShardProjectorChain(
                normalizedPhase.jobId(),
                numShardsEstimate,
                normalizedPhase.projections(),
                downstream,
                projectorFactory,
                jobCollectContext.queryPhaseRamAccountingContext()
        );

        Map<String, Map<String, List<Integer>>> locations = normalizedPhase.routing().locations();
        if (locations == null) {
            throw new IllegalStateException("locations must not be null");
        }

        final List<CrateCollector> shardCollectors = new ArrayList<>(numShardsEstimate);
        if (localNodeId.equals(collectPhase.handlerSideCollect())) {
            RowDownstream projectorChainDownstream = projectorChain.newShardDownstreamProjector(projectorFactory);
            // use collectPhase that is only cluster-normalized as un-assigned shards aren't really on a node
            shardCollectors.addAll(unassignedShardsCollectSource.getCollectors(
                    collectPhase, projectorChainDownstream, jobCollectContext));
        }

        Map<String, List<Integer>> indexShards = locations.get(localNodeId);
        if (indexShards != null) {
            shardCollectors.addAll(retrieveShardCollectors(jobCollectContext, normalizedPhase, projectorChain, indexShards));
        }
        if (shardCollectors.isEmpty()) {
            projectorChain.finish();
        } else {
            projectorChain.startProjections(jobCollectContext);
        }
        return shardCollectors;
    }

    private Collection<CrateCollector> retrieveShardCollectors(JobCollectContext jobCollectContext,
                                                               CollectPhase collectPhase,
                                                               ShardProjectorChain projectorChain,
                                                               Map<String, List<Integer>> indexShards) {

        Integer limit = collectPhase.limit();
        int batchSizeHint = Paging.getShardPageSize(collectPhase.limit(), collectPhase.routing().numShards());
        LOGGER.trace("setting batchSizeHint for ShardCollector to: {}; limit is: {}; numShards: {}",
                batchSizeHint, limit, batchSizeHint);

        List<CrateCollector> crateCollectors = new ArrayList<>();
        SharedShardContexts sharedShardContexts = jobCollectContext.sharedShardContexts();
        for (Map.Entry<String, List<Integer>> entry : indexShards.entrySet()) {
            String indexName = entry.getKey();
            IndexService indexService;
            try {
                indexService = indicesService.indexServiceSafe(indexName);
            } catch (IndexMissingException e) {
                if (PartitionName.isPartition(indexName)) {
                    continue;
                }
                throw new TableUnknownException(entry.getKey(), e);
            }

            for (Integer shardId : entry.getValue()) {
                SharedShardContext context = sharedShardContexts.getOrCreateContext(new ShardId(indexName, shardId));
                Injector shardInjector;
                try {
                    shardInjector = indexService.shardInjectorSafe(shardId);
                    ShardCollectService shardCollectService = shardInjector.getInstance(ShardCollectService.class);
                    CrateCollector collector = shardCollectService.getCollector(
                            collectPhase,
                            projectorChain,
                            jobCollectContext,
                            context.readerId(),
                            batchSizeHint
                    );
                    crateCollectors.add(collector);
                } catch (IndexShardMissingException | CancellationException | IllegalIndexShardStateException e) {
                    throw e;
                } catch (Exception e) {
                    throw new UnhandledServerException(e);
                }
            }
        }
        return crateCollectors;
    }

}
