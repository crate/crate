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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.breaker.RamAccountingContext;
import io.crate.exceptions.TableUnknownException;
import io.crate.exceptions.UnhandledServerException;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.jobs.JobContextService;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowUpstream;
import io.crate.operation.ThreadPools;
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
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexShardMissingException;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * collect local data from node/shards/docs on nodes where the data resides (aka Mapper nodes)
 */
@Singleton
public class MapSideDataCollectOperation implements CollectOperation, RowUpstream {

    private final IndicesService indicesService;
    protected final EvaluatingNormalizer nodeNormalizer;
    protected final ClusterService clusterService;
    private final ImplementationSymbolVisitor nodeImplementationSymbolVisitor;
    private final JobContextService jobContextService;
    private final FileCollectInputSymbolVisitor fileInputSymbolVisitor;
    private final CollectServiceResolver collectServiceResolver;
    private final ProjectionToProjectorVisitor projectorVisitor;
    private final ThreadPoolExecutor executor;
    private final int poolSize;
    private static final ESLogger LOGGER = Loggers.getLogger(MapSideDataCollectOperation.class);
    private final ResultProviderFactory resultProviderFactory;

    @Inject
    public MapSideDataCollectOperation(ClusterService clusterService,
                                       Settings settings,
                                       TransportActionProvider transportActionProvider,
                                       Functions functions,
                                       ReferenceResolver referenceResolver,
                                       IndicesService indicesService,
                                       ThreadPool threadPool,
                                       CollectServiceResolver collectServiceResolver,
                                       ResultProviderFactory resultProviderFactory,
                                       JobContextService jobContextService) {
        this.resultProviderFactory = resultProviderFactory;
        executor = (ThreadPoolExecutor) threadPool.executor(ThreadPool.Names.SEARCH);
        poolSize = executor.getPoolSize();
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.jobContextService = jobContextService;
        this.nodeNormalizer = new EvaluatingNormalizer(functions, RowGranularity.NODE, referenceResolver);
        this.collectServiceResolver = collectServiceResolver;
        this.nodeImplementationSymbolVisitor = new ImplementationSymbolVisitor(
                referenceResolver,
                functions,
                RowGranularity.NODE
        );
        this.fileInputSymbolVisitor =
                new FileCollectInputSymbolVisitor(functions, FileLineReferenceResolver.INSTANCE);
        this.projectorVisitor = new ProjectionToProjectorVisitor(
                clusterService,
                settings,
                transportActionProvider,
                nodeImplementationSymbolVisitor
        );
    }


    public ResultProvider createDownstream(CollectNode collectNode) {
        return resultProviderFactory.createDownstream(collectNode, collectNode.jobId().get());
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
    public void collect(CollectNode collectNode,
                        RowDownstream downstream,
                        RamAccountingContext ramAccountingContext) {
        assert collectNode.isRouted(); // not routed collect is not handled here
        assert collectNode.jobId().isPresent() : "no jobId present for collect operation";
        String localNodeId = clusterService.state().nodes().localNodeId();
        if (collectNode.executionNodes().contains(localNodeId)) {
            if (!collectNode.routing().containsShards(localNodeId)) {
                // node collect
                handleNodeCollect(collectNode, downstream, ramAccountingContext);
                return;
            } else {
                // shard or doc level
                handleShardCollect(collectNode, downstream, ramAccountingContext);
                return;
            }
        }
        throw new UnhandledServerException("unsupported routing");
    }

    /**
     * collect data on node level only - one row per node expected
     *
     * @param collectNode {@link CollectNode} instance containing routing information and symbols to collect
     * @param downstream  the receiver of the rows generated
     */
    protected void handleNodeCollect(CollectNode collectNode, RowDownstream downstream, RamAccountingContext ramAccountingContext) {
        collectNode = collectNode.normalize(nodeNormalizer);
        if (collectNode.whereClause().noMatch()) {
            downstream.registerUpstream(this).finish();
            return;
        }
        if (!collectNode.projections().isEmpty()) {
            FlatProjectorChain projectorChain = FlatProjectorChain.withAttachedDownstream(
                    projectorVisitor,
                    ramAccountingContext,
                    collectNode.projections(),
                    downstream
            );
            projectorChain.startProjections();
            downstream = projectorChain.firstProjector();
        }
        CrateCollector collector = getCollector(collectNode, downstream);
        collector.doCollect(ramAccountingContext);
    }

    private CrateCollector getCollector(CollectNode collectNode,
                                        RowDownstream downstream) {
        if (collectNode instanceof FileUriCollectNode) {
            FileCollectInputSymbolVisitor.Context context = fileInputSymbolVisitor.process(collectNode);
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
            ImplementationSymbolVisitor.Context ctx = nodeImplementationSymbolVisitor.process(collectNode);
            assert ctx.maxGranularity().ordinal() <= RowGranularity.NODE.ordinal() : "wrong RowGranularity";
            return new SimpleOneRowCollector(
                    ctx.topLevelInputs(), ctx.collectExpressions(), downstream);
        }
    }

    /**
     * collect data on shard or doc level
     * <p/>
     * collects data from each shard in a separate thread,
     * collecting the data into a single state through an {@link java.util.concurrent.ArrayBlockingQueue}.
     *
     * @param collectNode {@link CollectNode} containing routing information and symbols to collect
     */
    protected void handleShardCollect(CollectNode collectNode, RowDownstream downstream, RamAccountingContext ramAccountingContext) {
        String localNodeId = clusterService.state().nodes().localNodeId();
        final int numShards = collectNode.routing().numShards(localNodeId);

        collectNode = collectNode.normalize(nodeNormalizer);

        if (collectNode.whereClause().noMatch()) {
            downstream.registerUpstream(this).finish();
            return;
        }

        assert collectNode.jobId().isPresent() : "jobId must be set on CollectNode";
        JobCollectContext jobCollectContext = jobContextService.getOrCreateContext(collectNode.jobId().get())
                .collectContext(collectNode.executionNodeId());
        ShardProjectorChain projectorChain = new ShardProjectorChain(
                numShards,
                collectNode.projections(),
                downstream,
                projectorVisitor,
                ramAccountingContext
        );
        int jobSearchContextId = collectNode.routing().jobSearchContextIdBase();
        // get shardCollectors from single shards
        final List<CrateCollector> shardCollectors = new ArrayList<>(numShards);
        for (Map.Entry<String, Map<String, List<Integer>>> nodeEntry : collectNode.routing().locations().entrySet()) {
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
                                    collectNode,
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
            } else if (jobSearchContextId > -1) {
                // just increase jobSearchContextId by shard size of foreign node(s) indices
                for (List<Integer> shardIdMap : nodeEntry.getValue().values()) {
                    jobSearchContextId += shardIdMap.size();
                }
            }
        }

        // start the projection
        projectorChain.startProjections();
        try {
            runCollectThreaded(collectNode, shardCollectors, ramAccountingContext);
        } catch (RejectedExecutionException e) {
            // on distributing collects the merge nodes need to be informed about the failure
            // so they can clean up their context
            // in order to fire the failure we need to add the operation directly as an upstream to get a handle
            downstream.registerUpstream(this).fail(e);
            return;
        }

        // release the job collect context
        jobContextService.releaseContext(collectNode.jobId().get());

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("started {} shardCollectors", numShards);
        }
    }

    private void runCollectThreaded(CollectNode collectNode,
                                    final List<CrateCollector> shardCollectors,
                                    final RamAccountingContext ramAccountingContext) throws RejectedExecutionException {
        if (collectNode.maxRowGranularity() == RowGranularity.SHARD) {
            // run sequential to prevent sys.shards queries from using too many threads
            // and overflowing the threadpool queues
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    for (CrateCollector shardCollector : shardCollectors) {
                        doCollect(shardCollector, ramAccountingContext);
                    }
                }
            });
        } else {
            ThreadPools.runWithAvailableThreads(
                    executor,
                    poolSize,
                    Lists.transform(shardCollectors, new Function<CrateCollector, Runnable>() {

                        @Nullable
                        @Override
                        public Runnable apply(final CrateCollector input) {
                            return new Runnable() {
                                @Override
                                public void run() {
                                    doCollect(input, ramAccountingContext);
                                }
                            };
                        }
                    })
            );
        }
    }

    private void doCollect(CrateCollector shardCollector,
                           RamAccountingContext ramAccountingContext) {
        shardCollector.doCollect(ramAccountingContext);
    }
}
