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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.breaker.RamAccountingContext;
import io.crate.exceptions.TableUnknownException;
import io.crate.exceptions.UnhandledServerException;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.*;
import io.crate.operation.collect.files.FileCollectInputSymbolVisitor;
import io.crate.operation.collect.files.FileInputFactory;
import io.crate.operation.collect.files.FileReadingCollector;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.ResultProvider;
import io.crate.operation.projectors.ResultProviderFactory;
import io.crate.operation.reference.file.FileLineReferenceResolver;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.FileUriCollectNode;
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
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.Callable;
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
    protected final EvaluatingNormalizer clusterNormalizer;
    protected final ClusterService clusterService;
    private final ImplementationSymbolVisitor nodeImplementationSymbolVisitor;
    private final JobContextService jobContextService;
    private final FileCollectInputSymbolVisitor fileInputSymbolVisitor;
    private final CollectServiceResolver collectServiceResolver;
    private final ProjectionToProjectorVisitor projectorVisitor;
    private final ThreadPoolExecutor executor;
    private final ListeningExecutorService listeningExecutorService;
    private final int poolSize;
    private final ResultProviderFactory resultProviderFactory;

    private final InformationSchemaCollectService informationSchemaCollectService;
    private final UnassignedShardsCollectService unassignedShardsCollectService;

    private final OneRowCollectService clusterCollectService;
    private final CollectService nodeCollectService;

    @Inject
    public MapSideDataCollectOperation(ClusterService clusterService,
                                       Settings settings,
                                       TransportActionProvider transportActionProvider,
                                       BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                                       Functions functions,
                                       ReferenceResolver referenceResolver,
                                       IndicesService indicesService,
                                       ThreadPool threadPool,
                                       CollectServiceResolver collectServiceResolver,
                                       ResultProviderFactory resultProviderFactory,
                                       JobContextService jobContextService,
                                       InformationSchemaCollectService informationSchemaCollectService,
                                       UnassignedShardsCollectService unassignedShardsCollectService) {
        this.resultProviderFactory = resultProviderFactory;
        this.informationSchemaCollectService = informationSchemaCollectService;
        this.unassignedShardsCollectService = unassignedShardsCollectService;
        this.executor = (ThreadPoolExecutor)threadPool.executor(ThreadPool.Names.SEARCH);
        this.poolSize = executor.getCorePoolSize();
        this.listeningExecutorService = MoreExecutors.listeningDecorator(executor);

        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.jobContextService = jobContextService;
        this.nodeNormalizer = new EvaluatingNormalizer(functions, RowGranularity.NODE, referenceResolver);
        this.clusterNormalizer = new EvaluatingNormalizer(functions, RowGranularity.CLUSTER, referenceResolver);
        this.collectServiceResolver = collectServiceResolver;
        this.nodeImplementationSymbolVisitor = new ImplementationSymbolVisitor(
                referenceResolver,
                functions,
                RowGranularity.NODE
        );
        this.clusterCollectService = new OneRowCollectService(new ImplementationSymbolVisitor(
                referenceResolver,
                functions,
                RowGranularity.CLUSTER
        ));
        this.nodeCollectService = new CollectService() {

            @Override
            public CrateCollector getCollector(CollectNode node, RowDownstream downstream) {
                return getNodeLevelCollector(node, downstream);
            }
        };
        this.fileInputSymbolVisitor =
                new FileCollectInputSymbolVisitor(functions, FileLineReferenceResolver.INSTANCE);
        this.projectorVisitor = new ProjectionToProjectorVisitor(
                clusterService,
                threadPool,
                settings,
                transportActionProvider,
                bulkRetryCoordinatorPool,
                nodeImplementationSymbolVisitor
        );
    }


    public ResultProvider createDownstream(CollectNode collectNode) {
        return resultProviderFactory.createDownstream(collectNode, collectNode.jobId());
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
    public ListenableFuture<List<Void>> collect(CollectNode collectNode,
                        RowDownstream downstream,
                        RamAccountingContext ramAccountingContext) {
        assert collectNode.isRouted(); // not routed collect is not handled here
        assert collectNode.jobId() != null : "no jobId present for collect operation";
        String localNodeId = clusterService.state().nodes().localNodeId();
        Set<String> routingNodes = collectNode.routing().nodes();
        if (routingNodes.contains(localNodeId) || localNodeId.equals(collectNode.handlerSideCollect())) {
            if (collectNode.routing().containsShards(localNodeId)) {
                // shard or doc level (incl. unassigned shards)
                return handleShardCollect(collectNode, downstream, ramAccountingContext);
            } else {
                if (collectNode instanceof FileUriCollectNode) {
                    return handleNodeCollect(collectNode, downstream, ramAccountingContext);
                }
                switch (collectNode.maxRowGranularity()) {
                    case CLUSTER:
                        // sys.cluster
                        return handleClusterCollect(collectNode, downstream, ramAccountingContext);
                    case NODE:
                        // sys.nodes collect
                        return handleNodeCollect(collectNode, downstream, ramAccountingContext);
                    case SHARD:
                        // unassigned shards
                        return handleUnassignedShardsCollect(collectNode, downstream, ramAccountingContext);
                    case DOC:
                        if (collectNode.isPartitioned()) {
                            // edge case: partitioned table without actual indices
                            // no results
                            downstream.registerUpstream(this).finish();
                            return IMMEDIATE_LIST;
                        } else {
                            if (localNodeId.equals(collectNode.handlerSideCollect())) {
                                // information schema select
                                return handleInformationSchemaCollect(collectNode, downstream, ramAccountingContext);
                            } else {
                                // sys.operations, sys.jobs, sys.*log
                                return handleNodeCollect(collectNode, downstream, ramAccountingContext);
                            }
                        }
                }
            }
        }
        throw new UnhandledServerException("unsupported routing");
    }

    private ListenableFuture<List<Void>> handleInformationSchemaCollect(CollectNode collectNode, RowDownstream downstream, RamAccountingContext ramAccountingContext) {
        return handleWithService(informationSchemaCollectService, collectNode, downstream, ramAccountingContext);
    }

    private ListenableFuture<List<Void>> handleUnassignedShardsCollect(CollectNode collectNode, RowDownstream downstream, RamAccountingContext ramAccountingContext) {
        return handleWithService(unassignedShardsCollectService, collectNode, downstream, ramAccountingContext);
    }

    private ListenableFuture<List<Void>> handleWithService(final CollectService collectService,
                                                           final CollectNode node,
                                                           final RowDownstream rowDownstream,
                                                           final RamAccountingContext ramAccountingContext) {
        return listeningExecutorService.submit(new Callable<List<Void>>() {
            @Override
            public List<Void> call() throws Exception {
                try {
                    CollectNode localCollectNode = node.normalize(nodeNormalizer);
                    RowDownstream localRowDownStream = rowDownstream;
                    if (localCollectNode.whereClause().noMatch()) {
                        localRowDownStream.registerUpstream(MapSideDataCollectOperation.this).finish();
                    } else {
                        if (!localCollectNode.projections().isEmpty()) {
                            FlatProjectorChain projectorChain = FlatProjectorChain.withAttachedDownstream(
                                    projectorVisitor,
                                    ramAccountingContext,
                                    localCollectNode.projections(),
                                    localRowDownStream
                            );
                            projectorChain.startProjections();
                            localRowDownStream = projectorChain.firstProjector();
                        }
                        CrateCollector collector = collectService.getCollector(localCollectNode, localRowDownStream); // calls projector.registerUpstream()
                        collector.doCollect(ramAccountingContext);
                    }
                } catch (Throwable t) {
                    LOGGER.error("error during collect", t);
                    rowDownstream.registerUpstream(MapSideDataCollectOperation.this).fail(t);
                    Throwables.propagate(t);
                }
                return ONE_LIST;
            }
        });
    }

    private ListenableFuture<List<Void>> handleClusterCollect(final CollectNode collectNode,
                                                              final RowDownstream downstream,
                                                              final RamAccountingContext ramAccountingContext) {
        return handleWithService(clusterCollectService, collectNode, downstream, ramAccountingContext);
    }

    /**
     * collect data on node level only - one row per node expected
     *
     * @param collectNode {@link CollectNode} instance containing routing information and symbols to collect
     * @param downstream  the receiver of the rows generated
     */
    protected ListenableFuture<List<Void>> handleNodeCollect(final CollectNode collectNode,
                                                          final RowDownstream downstream,
                                                          final RamAccountingContext ramAccountingContext) {
        return handleWithService(nodeCollectService, collectNode, downstream, ramAccountingContext);
    }

    private CrateCollector getNodeLevelCollector(CollectNode collectNode,
                                                 RowDownstream downstream) {
        if (collectNode instanceof FileUriCollectNode) {
            FileCollectInputSymbolVisitor.Context context = fileInputSymbolVisitor.extractImplementations(collectNode);
            FileUriCollectNode fileUriCollectNode = (FileUriCollectNode) collectNode;

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
            ImplementationSymbolVisitor.Context ctx = nodeImplementationSymbolVisitor.extractImplementations(collectNode);
            assert ctx.maxGranularity().ordinal() <= RowGranularity.NODE.ordinal() : "wrong RowGranularity";
            return new SimpleOneRowCollector(
                    ctx.topLevelInputs(), ctx.collectExpressions(), downstream);
        }
    }

    private int numShards(CollectNode collectNode, String localNodeId) {
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
     * @param collectNode {@link CollectNode} containing routing information and symbols to collect
     */
    protected ListenableFuture<List<Void>> handleShardCollect(CollectNode collectNode, RowDownstream downstream, RamAccountingContext ramAccountingContext) {
        String localNodeId = clusterService.state().nodes().localNodeId();

        final int numShards = numShards(collectNode, localNodeId);

        CollectNode normalizedCollectNode = collectNode.normalize(nodeNormalizer);

        if (normalizedCollectNode.whereClause().noMatch()) {
            downstream.registerUpstream(this).finish();
            return IMMEDIATE_LIST;
        }

        assert normalizedCollectNode.jobId() != null : "jobId must be set on CollectNode";
        JobExecutionContext context;
        context = jobContextService.getContext(normalizedCollectNode.jobId());
        JobCollectContext jobCollectContext;
        try {
            jobCollectContext = context.getSubContext(normalizedCollectNode.executionNodeId());
        } catch (IllegalArgumentException e) {
            downstream.registerUpstream(this).finish();
            return IMMEDIATE_LIST;
        }
        ShardProjectorChain projectorChain = new ShardProjectorChain(
                numShards,
                normalizedCollectNode.projections(),
                downstream,
                projectorVisitor,
                ramAccountingContext
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
                        throw new TableUnknownException(entry.getKey(), e);
                    }

                    for (Integer shardId : entry.getValue()) {
                        jobCollectContext.registerJobContextId(
                                indexService.shardSafe(shardId).shardId(), jobSearchContextId);
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
                        } catch (IndexShardMissingException e) {
                            throw new UnhandledServerException(
                                    String.format(Locale.ENGLISH, "unknown shard id %d on index '%s'",
                                            shardId, entry.getKey()), e);
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
                CollectNode clusterNormalizedCollectNode = collectNode.normalize(clusterNormalizer);
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
        assert shardCollectors.size() == numShards : "invalid number of shardcollectors";

        // start the projection
        projectorChain.startProjections();
        try {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("starting {} shardCollectors...", numShards);
            }
            return runCollectThreaded(collectNode, shardCollectors, ramAccountingContext);
        } catch (RejectedExecutionException e) {
            // on distributing collects the merge nodes need to be informed about the failure
            // so they can clean up their context
            // in order to fire the failure we need to add the operation directly as an upstream to get a handle
            downstream.registerUpstream(this).fail(e);
            return Futures.immediateFailedFuture(e);
        }


    }

    private ListenableFuture<List<Void>> runCollectThreaded(CollectNode collectNode,
                                    final List<CrateCollector> shardCollectors,
                                    final RamAccountingContext ramAccountingContext) throws RejectedExecutionException {
        if (collectNode.maxRowGranularity() == RowGranularity.SHARD) {
            // run sequential to prevent sys.shards queries from using too many threads
            // and overflowing the threadpool queues
            return listeningExecutorService.submit(new Callable<List<Void>>() {
                @Override
                public List<Void> call() throws Exception {
                    for (CrateCollector collector : shardCollectors) {
                        doCollect(collector, ramAccountingContext);
                    }
                    return ONE_LIST;
                }
            });
        } else {
            return ThreadPools.runWithAvailableThreads(
                    executor,
                    poolSize,
                    collectors2Callables(shardCollectors, ramAccountingContext),
                    new VoidFunction<List<Void>>());
        }
    }

    private Collection<Callable<Void>> collectors2Callables(List<CrateCollector> collectors,
                                                            final RamAccountingContext ramAccountingContext) {
        return Lists.transform(collectors, new Function<CrateCollector, Callable<Void>>() {

            @Override
            public Callable<Void> apply(@Nullable final CrateCollector collector) {
                return new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        doCollect(collector, ramAccountingContext);
                        return null;
                    }
                };
            }
        });
    }

    private void doCollect(CrateCollector shardCollector,
                           RamAccountingContext ramAccountingContext) {
        shardCollector.doCollect(ramAccountingContext);
    }

    private static class OneRowCollectService implements CollectService {

        private final ImplementationSymbolVisitor clusterImplementationSymbolVisitor;

        private OneRowCollectService(ImplementationSymbolVisitor clusterImplementationSymbolVisitor) {
            this.clusterImplementationSymbolVisitor = clusterImplementationSymbolVisitor;
        }

        @Override
        public CrateCollector getCollector(CollectNode node, RowDownstream downstream) {
            // resolve Implementations
            ImplementationSymbolVisitor.Context ctx = clusterImplementationSymbolVisitor.extractImplementations(node);
            List<Input<?>> inputs = ctx.topLevelInputs();
            Set<CollectExpression<?>> collectExpressions = ctx.collectExpressions();
            return new SimpleOneRowCollector(inputs, collectExpressions, downstream);
        }
    }
}
