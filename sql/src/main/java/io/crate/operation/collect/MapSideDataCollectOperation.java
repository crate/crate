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
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.Streamer;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.exceptions.TableUnknownException;
import io.crate.exceptions.UnhandledServerException;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.ThreadPools;
import io.crate.operation.collect.files.FileCollectInputSymbolVisitor;
import io.crate.operation.collect.files.FileInputFactory;
import io.crate.operation.collect.files.FileReadingCollector;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.ResultProvider;
import io.crate.operation.reference.file.FileLineReferenceResolver;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.PlanNodeStreamerVisitor;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.FileUriCollectNode;
import io.crate.planner.symbol.ValueSymbolVisitor;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexShardMissingException;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * collect local data from node/shards/docs on nodes where the data resides (aka Mapper nodes)
 */
public abstract class MapSideDataCollectOperation<T extends ResultProvider> implements CollectOperation {

    public static class SimpleShardCollectFuture extends ShardCollectFuture {

        private final CollectContextService collectContextService;
        private final UUID jobId;
        private ListenableFuture<Bucket> upstreamResult;

        public SimpleShardCollectFuture(int numShards,
                                        ListenableFuture<Bucket> upstreamResult,
                                        CollectContextService collectContextService,
                                        UUID jobId) {
            super(numShards);
            this.upstreamResult = upstreamResult;
            this.collectContextService = collectContextService;
            this.jobId = jobId;

        }

        @Override
        public void onAllShardsFinished() {
            Futures.addCallback(upstreamResult, new FutureCallback<Bucket>() {
                @Override
                public void onSuccess(@Nullable Bucket result) {
                    collectContextService.releaseContext(jobId);
                    set(result);
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    collectContextService.releaseContext(jobId);
                    setException(t);
                }
            });
        }
    }

    protected final PlanNodeStreamerVisitor streamerVisitor;
    private final IndicesService indicesService;
    protected final EvaluatingNormalizer nodeNormalizer;
    protected final ClusterService clusterService;
    private final ImplementationSymbolVisitor nodeImplementationSymbolVisitor;
    private final CollectContextService collectContextService;
    private final FileCollectInputSymbolVisitor fileInputSymbolVisitor;
    private final CollectServiceResolver collectServiceResolver;
    private final ProjectionToProjectorVisitor projectorVisitor;
    private final ThreadPoolExecutor executor;
    private final int poolSize;
    private static final ESLogger LOGGER = Loggers.getLogger(MapSideDataCollectOperation.class);

    public MapSideDataCollectOperation(ClusterService clusterService,
                                       Settings settings,
                                       TransportActionProvider transportActionProvider,
                                       Functions functions,
                                       ReferenceResolver referenceResolver,
                                       IndicesService indicesService,
                                       ThreadPool threadPool,
                                       CollectServiceResolver collectServiceResolver,
                                       PlanNodeStreamerVisitor streamerVisitor,
                                       CollectContextService collectContextService) {
        executor = (ThreadPoolExecutor) threadPool.executor(ThreadPool.Names.SEARCH);
        poolSize = executor.getPoolSize();
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.collectContextService = collectContextService;
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
        this.streamerVisitor = streamerVisitor;
    }

    protected abstract Optional<T> createResultResultProvider(CollectNode node);

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
    public ListenableFuture<Bucket> collect(CollectNode collectNode,
                                            RamAccountingContext ramAccountingContext) {
        assert collectNode.isRouted(); // not routed collect is not handled here
        assert collectNode.jobId().isPresent() : "no jobId present for collect operation";
        String localNodeId = clusterService.localNode().id();
        if (collectNode.executionNodes().contains(localNodeId)) {
            if (!collectNode.routing().containsShards(localNodeId)) {
                // node collect
                T result;
                try {
                    result = handleNodeCollect(collectNode, ramAccountingContext);
                } catch (Exception e) {
                    return Futures.immediateFailedFuture(e);
                }
                if (result==null){
                    return Futures.immediateFuture(Bucket.EMPTY);
                }
                return result.result();
            } else {
                // shard or doc level
                return handleShardCollect(collectNode, ramAccountingContext);
            }
        }
        throw new UnhandledServerException("unsupported routing");
    }

    /**
     * collect data on node level only - one row per node expected
     *
     * @param collectNode {@link io.crate.planner.node.dql.CollectNode} instance containing routing information and symbols to collect
     * @return the collect result from this node, one row only so return value is <code>Object[1][]</code>
     */
    @Nullable
    protected T handleNodeCollect(CollectNode collectNode, RamAccountingContext ramAccountingContext) throws Exception {
        collectNode = collectNode.normalize(nodeNormalizer);
        if (collectNode.whereClause().noMatch()) {
            return null;
        }
        Optional<T> resultProjector = createResultResultProvider(collectNode);

        @SuppressWarnings("unchecked")
        FlatProjectorChain projectorChain = new FlatProjectorChain(
                collectNode.projections(),
                projectorVisitor,
                ramAccountingContext,
                (Optional<ResultProvider>) resultProjector);

        CrateCollector collector = getCollector(collectNode, projectorChain);
        projectorChain.startProjections();
        try {
            collector.doCollect(ramAccountingContext);
        } catch (CollectionAbortedException ex) {
            // ignore
        }
        if (resultProjector.isPresent()){
            return resultProjector.get();
        } else {
            //noinspection unchecked
            return (T) projectorChain.resultProvider();
        }
    }

    private CrateCollector getCollector(CollectNode collectNode,
                                        FlatProjectorChain projectorChain) throws Exception {
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
                    projectorChain.firstProjector(),
                    fileUriCollectNode.fileFormat(),
                    fileUriCollectNode.compression(),
                    ImmutableMap.<String, FileInputFactory>of(),
                    fileUriCollectNode.sharedStorage(),
                    readers.length,
                    Arrays.binarySearch(readers, clusterService.localNode().id())
            );
        } else {
            CollectService service = collectServiceResolver.getService(collectNode.routing());
            if (service != null) {
                return service.getCollector(collectNode, projectorChain.firstProjector());
            }
            ImplementationSymbolVisitor.Context ctx = nodeImplementationSymbolVisitor.process(collectNode);
            assert ctx.maxGranularity().ordinal() <= RowGranularity.NODE.ordinal() : "wrong RowGranularity";
            return new SimpleOneRowCollector(
                    ctx.topLevelInputs(), ctx.collectExpressions(), projectorChain.firstProjector());
        }
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
    protected ListenableFuture<Bucket> handleShardCollect(CollectNode collectNode, RamAccountingContext ramAccountingContext) {

        String localNodeId = clusterService.localNode().id();
        final int numShards = collectNode.routing().numShards(localNodeId);

        collectNode = collectNode.normalize(nodeNormalizer);

        //noinspection unchecked
        ShardProjectorChain projectorChain = new ShardProjectorChain(
                numShards,
                collectNode.projections(),
                (Optional<ResultProvider>) createResultResultProvider(collectNode),
                projectorVisitor, ramAccountingContext);

        final ShardCollectFuture result = getShardCollectFuture(numShards, projectorChain, collectNode);

        if (collectNode.whereClause().noMatch()) {
            projectorChain.startProjections();
            result.onAllShardsFinished();
            return result;
        }

        assert collectNode.jobId().isPresent() : "jobId must be set on CollectNode";
        JobCollectContext jobCollectContext = collectContextService.acquireContext(collectNode.jobId().get());

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
            runCollectThreaded(collectNode, result, shardCollectors, ramAccountingContext);
        } catch (RejectedExecutionException e) {
            // on distributing collects the merge nodes need to be informed about the failure
            // so they can clean up their context
            result.shardFailure(e);
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("started {} shardCollectors", numShards);
        }

        return result;
    }

    private void runCollectThreaded(CollectNode collectNode,
                                    final ShardCollectFuture result,
                                    final List<CrateCollector> shardCollectors,
                                    final RamAccountingContext ramAccountingContext) throws RejectedExecutionException {
        if (collectNode.maxRowGranularity() == RowGranularity.SHARD) {
            // run sequential to prevent sys.shards queries from using too many threads
            // and overflowing the threadpool queues
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    for (CrateCollector shardCollector : shardCollectors) {
                        doCollect(result, shardCollector, ramAccountingContext);
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
                                    doCollect(result, input, ramAccountingContext);
                                }
                            };
                        }
                    })
            );
        }
    }


    private void doCollect(ShardCollectFuture result, CrateCollector shardCollector,
                           RamAccountingContext ramAccountingContext) {
        try {
            shardCollector.doCollect(ramAccountingContext);
            result.shardFinished();
        } catch (CollectionAbortedException ex) {
            // ignore
        } catch (Exception ex) {
            result.shardFailure(ex);
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("shard finished collect, {} to go", result.numShards());
        }
    }

    /**
     * chose the right ShardCollectFuture for this class
     *
     * @param numShards      number of shards until the result is considered complete
     * @param projectorChain the projector chain to process the collected rows
     * @param collectNode    in case any other properties need to be extracted
     * @return a fancy ShardCollectFuture implementation
     */
    protected ShardCollectFuture getShardCollectFuture(int numShards,
                                                       ShardProjectorChain projectorChain,
                                                       CollectNode collectNode) {
        return new SimpleShardCollectFuture(numShards, projectorChain.resultProvider().result(),
                collectContextService, collectNode.jobId().get());
    }

    protected Streamer<?>[] getStreamers(CollectNode node) {
        PlanNodeStreamerVisitor.Context ctx = new PlanNodeStreamerVisitor.Context(null);
        streamerVisitor.process(node, ctx);
        return ctx.outputStreamers();
    }

}
