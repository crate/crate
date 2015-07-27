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

package io.crate.operation.collect;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.exceptions.TableUnknownException;
import io.crate.exceptions.UnhandledServerException;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.Functions;
import io.crate.metadata.PartitionName;
import io.crate.metadata.ReferenceResolver;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.Input;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowUpstream;
import io.crate.operation.ThreadPools;
import io.crate.operation.collect.files.FileCollectInputSymbolVisitor;
import io.crate.operation.collect.files.FileInputFactory;
import io.crate.operation.collect.files.FileReadingCollector;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.reference.file.FileLineReferenceResolver;
import io.crate.operation.reference.sys.node.NodeSysExpression;
import io.crate.operation.reference.sys.node.NodeSysReferenceResolver;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.FileUriCollectPhase;
import io.crate.planner.symbol.ValueSymbolVisitor;
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
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * collect local data from node/shards/docs on nodes where the data resides (aka Mapper nodes)
 */
@Singleton
public class MapSideDataCollectOperation implements CollectOperation, RowUpstream {

    private static final ESLogger LOGGER = Loggers.getLogger(MapSideDataCollectOperation.class);

    private static class VoidFunction<Arg> implements Function<Arg, Void> {
        @Nullable
        @Override
        public Void apply(@Nullable Arg input) {
            return null;
        }
    }

    private final IndicesService indicesService;
    protected final EvaluatingNormalizer nodeNormalizer;
    protected final ClusterService clusterService;
    private final FileCollectInputSymbolVisitor fileInputSymbolVisitor;
    private final CollectServiceResolver collectServiceResolver;
    private final ProjectionToProjectorVisitor projectorVisitor;
    private final ThreadPoolExecutor executor;
    private final ListeningExecutorService listeningExecutorService;
    private final int poolSize;

    private final InformationSchemaCollectService informationSchemaCollectService;
    private final UnassignedShardsCollectService unassignedShardsCollectService;

    private final OneRowCollectService clusterCollectService;
    private final CollectService nodeCollectService;

    private final Functions functions;
    private final NodeSysExpression nodeSysExpression;
    private ThreadPool threadPool;
    private final BulkRetryCoordinatorPool bulkRetryCoordinatorPool;
    private final TransportActionProvider transportActionProvider;
    private final Settings settings;

    @Inject
    public MapSideDataCollectOperation(ClusterService clusterService,
                                       Settings settings,
                                       TransportActionProvider transportActionProvider,
                                       BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                                       Functions functions,
                                       ReferenceResolver referenceResolver,
                                       NodeSysExpression nodeSysExpression,
                                       IndicesService indicesService,
                                       ThreadPool threadPool,
                                       CollectServiceResolver collectServiceResolver,
                                       InformationSchemaCollectService informationSchemaCollectService,
                                       UnassignedShardsCollectService unassignedShardsCollectService) {
        this.informationSchemaCollectService = informationSchemaCollectService;
        this.unassignedShardsCollectService = unassignedShardsCollectService;
        this.executor = (ThreadPoolExecutor)threadPool.executor(ThreadPool.Names.SEARCH);
        this.poolSize = executor.getCorePoolSize();
        this.listeningExecutorService = MoreExecutors.listeningDecorator(executor);

        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.nodeNormalizer = new EvaluatingNormalizer(functions, RowGranularity.NODE, referenceResolver);

        this.collectServiceResolver = collectServiceResolver;

        this.settings = settings;
        this.functions = functions;
        this.nodeSysExpression = nodeSysExpression;

        this.clusterCollectService = new OneRowCollectService(new ImplementationSymbolVisitor(
                referenceResolver,
                functions,
                RowGranularity.CLUSTER
        ));
        this.nodeCollectService = new CollectService() {
            @Override
            public CrateCollector getCollector(CollectPhase node, RowDownstream downstream) {
                return getNodeLevelCollector(node, downstream);
            }
        };
        this.fileInputSymbolVisitor =
                new FileCollectInputSymbolVisitor(functions, FileLineReferenceResolver.INSTANCE);

        this.threadPool = threadPool;
        this.bulkRetryCoordinatorPool = bulkRetryCoordinatorPool;
        this.transportActionProvider = transportActionProvider;

        ImplementationSymbolVisitor nodeImplementationSymbolVisitor = new ImplementationSymbolVisitor(
                referenceResolver,
                functions,
                RowGranularity.NODE
        );
        this.projectorVisitor = new ProjectionToProjectorVisitor(
                clusterService,
                threadPool,
                settings,
                transportActionProvider,
                bulkRetryCoordinatorPool,
                nodeImplementationSymbolVisitor
        );
    }

    /**
     * dispatch by the following criteria:
     * <p>
     * * if local node id is contained in routing:<br>
     * * if no shards are given:<br>
     * &nbsp; -&gt; run row granularity level collect<br>
     * &nbsp; except for doc level:
     * &nbsp; &nbsp; if table if partitioned:
     * &nbsp; &nbsp; -&gt; edge case for empty partitioned table
     * &nbsp; &nbsp; else:
     * &nbsp; &nbsp; -&gt; collect from information schema
     * * if shards are given:<br>
     * &nbsp; -&gt; run shard or doc level collect<br>
     * * else if we got cluster RowGranularity:<br>
     * &nbsp; -&gt; run node level collect (cluster level)<br>
     * </p>
     */
    @Override
    public Collection<CrateCollector> collect(CollectPhase collectNode,
                                                RowDownstream downstream,
                                                final JobCollectContext jobCollectContext) {
        assert collectNode.isRouted(); // not routed collect is not handled here
        assert collectNode.jobId() != null : "no jobId present for collect operation";
        String localNodeId = clusterService.state().nodes().localNodeId();
        Set<String> routingNodes = collectNode.routing().nodes();
        if (routingNodes.contains(localNodeId) || localNodeId.equals(collectNode.handlerSideCollect())) {
            if (collectNode.routing().containsShards(localNodeId)) {
                // shard or doc level (incl. unassigned shards)
                return handleShardCollect(collectNode, downstream, jobCollectContext);
            } else {
                Collection<CrateCollector> results;
                if (collectNode instanceof FileUriCollectPhase) {
                    results = handleWithService(nodeCollectService, collectNode, downstream, jobCollectContext);
                } else if (collectNode.isPartitioned() && collectNode.maxRowGranularity() == RowGranularity.DOC) {
                    // edge case: partitioned table without actual indices
                    // no results
                    downstream.registerUpstream(this).finish();
                    results = ImmutableList.of();
                } else {
                    CollectService collectService = getCollectService(collectNode, localNodeId);
                    results = handleWithService(collectService, collectNode, downstream, jobCollectContext);
                }
                return results;
            }
        }
        throw new UnhandledServerException("unsupported routing");
    }

    private CollectService getCollectService(CollectPhase collectNode, String localNodeId) {
        switch (collectNode.maxRowGranularity()) {
            case CLUSTER:
                // sys.cluster
                return clusterCollectService;
            case NODE:
                // sys.nodes collect
                return nodeCollectService;
            case SHARD:
                // unassigned shards
                return unassignedShardsCollectService;
            case DOC:
                if (localNodeId.equals(collectNode.handlerSideCollect())) {
                    // information schema select
                    return informationSchemaCollectService;
                } else {
                    // sys.operations, sys.jobs, sys.*log
                    return nodeCollectService;
                }
            default:
                throw new UnsupportedOperationException("Unsupported rowGranularity " + collectNode.maxRowGranularity());
        }
    }

    private Collection<CrateCollector> handleWithService(final CollectService collectService,
                                                           final CollectPhase node,
                                                           final RowDownstream rowDownstream,
                                                           final JobCollectContext jobCollectContext) {
        EvaluatingNormalizer nodeNormalizer = MapSideDataCollectOperation.this.nodeNormalizer;
        if (node.maxRowGranularity().finerThan(RowGranularity.CLUSTER)) {
            nodeNormalizer = new EvaluatingNormalizer(functions,
                    RowGranularity.NODE,
                    new NodeSysReferenceResolver(nodeSysExpression));
        }

        final CollectPhase localCollectNode = node.normalize(nodeNormalizer);
        if (localCollectNode.whereClause().noMatch()) {
            rowDownstream.registerUpstream(MapSideDataCollectOperation.this).finish();
            return ImmutableList.of();
        }

        final RowDownstream localRowDownStream;
        final FlatProjectorChain projectorChain;
        if (!localCollectNode.projections().isEmpty()) {
                 projectorChain = FlatProjectorChain.withAttachedDownstream(
                        projectorVisitor,
                        jobCollectContext.queryPhaseRamAccountingContext(),
                        localCollectNode.projections(),
                         rowDownstream,
                        node.jobId()
                );
                localRowDownStream = projectorChain.firstProjector();
        } else {
            localRowDownStream = rowDownstream;
            projectorChain = null;
        }
        final CrateCollector collector;
        try {
            collector = collectService.getCollector(localCollectNode, localRowDownStream); // calls projector.registerUpstream();
        } catch (Throwable t) {
            rowDownstream.registerUpstream(MapSideDataCollectOperation.this).fail(t);
            return ImmutableList.of();
        }
        listeningExecutorService.submit(new Callable<List<Void>>() {
            @Override
            public List<Void> call() throws Exception {
                try {
                    if (projectorChain != null) {
                        projectorChain.startProjections(jobCollectContext);
                    }
                    collector.doCollect();
                    jobCollectContext.close();
                } catch (Throwable t) {
                    LOGGER.error("error during collect", t);
                    rowDownstream.registerUpstream(MapSideDataCollectOperation.this).fail(t);
                    Throwables.propagate(t);
                }
                return ImmutableList.of();
            }
        });
        return ImmutableList.of(collector);
    }

    private CrateCollector getNodeLevelCollector(CollectPhase collectNode,
                                                 RowDownstream downstream) {
        if (collectNode instanceof FileUriCollectPhase) {
            FileCollectInputSymbolVisitor.Context context = fileInputSymbolVisitor.extractImplementations(collectNode);
            FileUriCollectPhase fileUriCollectNode = (FileUriCollectPhase) collectNode;

            String[] readers = fileUriCollectNode.executionNodes().toArray(
                    new String[fileUriCollectNode.executionNodes().size()]);
            Arrays.sort(readers);
            return new FileReadingCollector(
                    ValueSymbolVisitor.STRING.process(fileUriCollectNode.targetUri()),
                    context.topLevelInputs(),
                    context.expressions(),
                    downstream,
                    fileUriCollectNode.fileFormat(),
                    fileUriCollectNode.compression(),
                    ImmutableMap.<String, FileInputFactory>of(),
                    fileUriCollectNode.sharedStorage(),
                    readers.length,
                    Arrays.binarySearch(readers, clusterService.state().nodes().localNodeId())
            );
        } else {
            CollectService service = collectServiceResolver.getService(collectNode.routing());
            if (service != null) {
                return service.getCollector(collectNode, downstream);
            }
            ImplementationSymbolVisitor nodeImplementationSymbolVisitor = new ImplementationSymbolVisitor(
                    new NodeSysReferenceResolver(nodeSysExpression),
                    functions,
                    RowGranularity.NODE
            );
            ImplementationSymbolVisitor.Context ctx = nodeImplementationSymbolVisitor.extractImplementations(collectNode);
            assert ctx.maxGranularity().ordinal() <= RowGranularity.NODE.ordinal() : "wrong RowGranularity";
            return new SimpleOneRowCollector(ctx.topLevelInputs(), downstream);
        }
    }

    private int numShards(CollectPhase collectNode, String localNodeId) {
        int numShards = collectNode.routing().numShards(localNodeId);
        if (localNodeId.equals(collectNode.handlerSideCollect()) && collectNode.routing().nodes().contains(TableInfo.NULL_NODE_ID)) {
            // add 1 for unassigned shards - treated as one shard
            // as it is handled with one collector
            numShards += 1;
        }
        return numShards;
    }

    /**
     * collect data on shard or doc level
     * <p>
     * collects data from each shard in a separate thread,
     * collecting the data into a single state through an {@link java.util.concurrent.ArrayBlockingQueue}.
     * </p>
     *
     * @param collectNode {@link CollectPhase} containing routing information and symbols to collect
     */
    protected Collection<CrateCollector> handleShardCollect(final CollectPhase collectNode,
                                                              RowDownstream downstream,
                                                              final JobCollectContext jobCollectContext) {
        String localNodeId = clusterService.state().nodes().localNodeId();

        final int numShards = numShards(collectNode, localNodeId);

        NodeSysReferenceResolver referenceResolver = new NodeSysReferenceResolver(nodeSysExpression);
        EvaluatingNormalizer nodeNormalizer = new EvaluatingNormalizer(functions,
                RowGranularity.NODE,
                referenceResolver);
        CollectPhase normalizedCollectNode = collectNode.normalize(nodeNormalizer);

        if (normalizedCollectNode.whereClause().noMatch()) {
            downstream.registerUpstream(this).finish();
            return ImmutableList.of();
        }

        assert normalizedCollectNode.jobId() != null : "jobId must be set on CollectNode";

        ImplementationSymbolVisitor implementationSymbolVisitor = new ImplementationSymbolVisitor(
                referenceResolver,
                functions,
                RowGranularity.NODE
        );
        ProjectionToProjectorVisitor projectorVisitor = new ProjectionToProjectorVisitor(
                clusterService,
                threadPool,
                settings,
                transportActionProvider,
                bulkRetryCoordinatorPool,
                implementationSymbolVisitor
        );

        ShardProjectorChain projectorChain = new ShardProjectorChain(
                collectNode.jobId(),
                numShards,
                normalizedCollectNode.projections(),
                downstream,
                projectorVisitor,
                jobCollectContext.queryPhaseRamAccountingContext()
        );
        int jobSearchContextId = normalizedCollectNode.routing().jobSearchContextIdBase();

        // get shardCollectors from single shards
        final List<CrateCollector> shardCollectors = new ArrayList<>(numShards);
        for (Map.Entry<String, Map<String, List<Integer>>> nodeEntry : normalizedCollectNode.routing().locations().entrySet()) {
            if (nodeEntry.getKey().equals(localNodeId)) {
                Map<String, List<Integer>> shardIdMap = nodeEntry.getValue();
                for (Map.Entry<String, List<Integer>> entry : shardIdMap.entrySet()) {
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
                        Injector shardInjector;
                        try {
                            shardInjector = indexService.shardInjectorSafe(shardId);
                            ShardCollectService shardCollectService = shardInjector.getInstance(ShardCollectService.class);
                            CrateCollector collector = shardCollectService.getCollector(
                                    normalizedCollectNode,
                                    projectorChain,
                                    jobCollectContext,
                                    jobSearchContextId
                            );
                            shardCollectors.add(collector);
                        } catch (IndexShardMissingException | CancellationException | IllegalIndexShardStateException e) {
                            throw e;
                        } catch (Exception e) {
                            LOGGER.error("Error while getting collector", e);
                            throw new UnhandledServerException(e);
                        }
                        jobSearchContextId++;
                    }
                }
            } else if (TableInfo.NULL_NODE_ID.equals(nodeEntry.getKey()) && localNodeId.equals(collectNode.handlerSideCollect())) {
                // collect unassigned shards
                LOGGER.trace("collecting unassigned shards on node {}", localNodeId);
                EvaluatingNormalizer clusterNormalizer = new EvaluatingNormalizer(functions,
                        RowGranularity.CLUSTER,
                        referenceResolver);
                CollectPhase clusterNormalizedCollectNode = collectNode.normalize(clusterNormalizer);

                RowDownstream projectorChainDownstream = projectorChain.newShardDownstreamProjector(projectorVisitor);
                CrateCollector collector = unassignedShardsCollectService.getCollector(
                        clusterNormalizedCollectNode,
                        projectorChainDownstream
                );
                shardCollectors.add(collector);
            } else if (jobSearchContextId > -1) {
                // just increase jobSearchContextId by shard size of foreign node(s) indices
                for (List<Integer> shardIdMap : nodeEntry.getValue().values()) {
                    jobSearchContextId += shardIdMap.size();
                }
            }
        }

        if (shardCollectors.isEmpty()) {
            downstream.registerUpstream(this).finish();
            return ImmutableList.of();
        }

        // start the projection
        projectorChain.startProjections(jobCollectContext);
        try {
            LOGGER.trace("starting {} shardCollectors...", numShards);
            ListenableFuture<List<Void>> futures = runCollectThreaded(collectNode, shardCollectors);

            Futures.addCallback(futures, new FutureCallback<List<Void>>() {
                @Override
                public void onSuccess(@Nullable List<Void> result) {
                    if (!(collectNode.maxRowGranularity().equals(RowGranularity.DOC))) {
                        jobCollectContext.close();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    jobCollectContext.closeDueToFailure(t);
                }
            });
            return  shardCollectors;
        } catch (RejectedExecutionException e) {
            // on distributing collects the merge nodes need to be informed about the failure
            // so they can clean up their context
            // in order to fire the failure we need to add the operation directly as an upstream to get a handle
            downstream.registerUpstream(this).fail(e);
        }
        return ImmutableList.of();
    }

    private ListenableFuture<List<Void>> runCollectThreaded(CollectPhase collectNode,
                                                            final List<CrateCollector> shardCollectors)
            throws RejectedExecutionException {
        if (collectNode.maxRowGranularity() == RowGranularity.SHARD) {
            // run sequential to prevent sys.shards queries from using too many threads
            // and overflowing the threadpool queues
            return listeningExecutorService.submit(new Callable<List<Void>>() {
                @Override
                public List<Void> call() throws Exception {
                    for (CrateCollector collector : shardCollectors) {
                        collector.doCollect();
                    }
                    return ImmutableList.of();
                }
            });
        } else {
            return ThreadPools.runWithAvailableThreads(
                    executor,
                    poolSize,
                    collectors2Callables(shardCollectors),
                    new VoidFunction<List<Void>>());
        }
    }


    private Collection<Callable<Void>> collectors2Callables(List<CrateCollector> collectors) {
        return Lists.transform(collectors, new Function<CrateCollector, Callable<Void>>() {

            @Override
            public Callable<Void> apply(final CrateCollector collector) {
                return new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        collector.doCollect();
                        return null;
                    }
                };
            }
        });
    }

    private static class OneRowCollectService implements CollectService {

        private final ImplementationSymbolVisitor clusterImplementationSymbolVisitor;

        private OneRowCollectService(ImplementationSymbolVisitor clusterImplementationSymbolVisitor) {
            this.clusterImplementationSymbolVisitor = clusterImplementationSymbolVisitor;
        }

        @Override
        public CrateCollector getCollector(CollectPhase node, RowDownstream downstream) {
            // resolve Implementations
            ImplementationSymbolVisitor.Context ctx = clusterImplementationSymbolVisitor.extractImplementations(node);
            List<Input<?>> inputs = ctx.topLevelInputs();
            return new SimpleOneRowCollector(inputs, downstream);
        }
    }
}
