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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.Constants;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.exceptions.TableUnknownException;
import io.crate.exceptions.UnhandledServerException;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.collect.files.FileCollectInputSymbolVisitor;
import io.crate.operation.collect.files.FileInputFactory;
import io.crate.operation.collect.files.FileReadingCollector;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.reference.file.FileLineReferenceResolver;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.FileUriCollectNode;
import io.crate.planner.symbol.StringValueSymbolVisitor;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.IndexShardMissingException;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.util.*;

/**
 * collect local data from node/shards/docs on nodes where the data resides (aka Mapper nodes)
 */
public class MapSideDataCollectOperation implements CollectOperation<Object[][]> {

    private final FileCollectInputSymbolVisitor fileInputSymbolVisitor;
    private ESLogger logger = Loggers.getLogger(getClass());
    private final ProjectionToProjectorVisitor projectorVisitor;


    private static class SimpleShardCollectFuture extends ShardCollectFuture {

        public SimpleShardCollectFuture(int numShards, ShardProjectorChain projectorChain) {
            super(numShards, projectorChain);
        }

        @Override
        protected void onAllShardsFinished() {
            Futures.addCallback(projectorChain.result(), new FutureCallback<Object[][]>() {
                @Override
                public void onSuccess(@Nullable Object[][] result) {
                    set(result);
                }

                @Override
                public void onFailure(Throwable t) {
                    setException(t);
                }
            });
        }
    }

    private final IndicesService indicesService;
    protected final EvaluatingNormalizer nodeNormalizer;
    private final ThreadPool threadPool;
    protected final ClusterService clusterService;
    private final ImplementationSymbolVisitor nodeImplementationSymbolVisitor;

    @Inject
    public MapSideDataCollectOperation(Provider<Client> clientProvider,
                                       ClusterService clusterService,
                                       Functions functions,
                                       ReferenceResolver referenceResolver,
                                       IndicesService indicesService,
                                       ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.nodeNormalizer = new EvaluatingNormalizer(functions, RowGranularity.NODE, referenceResolver);
        this.threadPool = threadPool;
        this.nodeImplementationSymbolVisitor = new ImplementationSymbolVisitor(
                referenceResolver,
                functions,
                RowGranularity.NODE
        );
        this.fileInputSymbolVisitor =
                new FileCollectInputSymbolVisitor(functions, FileLineReferenceResolver.INSTANCE);
        this.projectorVisitor = new ProjectionToProjectorVisitor(clientProvider, nodeImplementationSymbolVisitor);
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
        throw new UnhandledServerException("unsupported routing");
    }

    /**
     * collect data on node level only - one row per node expected
     *
     * @param collectNode {@link io.crate.planner.node.dql.CollectNode} instance containing routing information and symbols to collect
     * @return the collect result from this node, one row only so return value is <code>Object[1][]</code>
     */
    protected ListenableFuture<Object[][]> handleNodeCollect(CollectNode collectNode) {
        collectNode = collectNode.normalize(nodeNormalizer);
        if (collectNode.whereClause().noMatch()) {
            return Futures.immediateFuture(Constants.EMPTY_RESULT);
        }
        assert collectNode.toCollect().size() > 0;

        FlatProjectorChain projectorChain = new FlatProjectorChain(
                collectNode.projections(), projectorVisitor);
        CrateCollector collector;
        try {
            collector = getCollector(collectNode, projectorChain);
        } catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }
        projectorChain.startProjections();
        try {
            collector.doCollect();
        } catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }
        return projectorChain.result();
    }

    private CrateCollector getCollector(CollectNode collectNode,
                                        FlatProjectorChain projectorChain) throws Exception {
        if (collectNode instanceof FileUriCollectNode) {
            FileCollectInputSymbolVisitor.Context context = fileInputSymbolVisitor.process(collectNode);
            FileUriCollectNode fileUriCollectNode = (FileUriCollectNode)collectNode;

            String[] readers = fileUriCollectNode.executionNodes().toArray(
                    new String[fileUriCollectNode.executionNodes().size()]);
            Arrays.sort(readers);
            return new FileReadingCollector(
                    StringValueSymbolVisitor.INSTANCE.process(fileUriCollectNode.targetUri()),
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
    protected ListenableFuture<Object[][]> handleShardCollect(CollectNode collectNode) {

        String localNodeId = clusterService.localNode().id();
        final int numShards = collectNode.routing().numShards(localNodeId);

        collectNode = collectNode.normalize(nodeNormalizer);
        ShardProjectorChain projectorChain = new ShardProjectorChain(numShards, collectNode.projections(), projectorVisitor);

        final ShardCollectFuture result = getShardCollectFuture(numShards, projectorChain, collectNode);

        if (collectNode.whereClause().noMatch()) {
            projectorChain.startProjections();
            result.onAllShardsFinished();
            return result;
        }

        List<CrateCollector> shardCollectors = new ArrayList<>(numShards);

        // get shardCollectors from single shards
        Map<String, Set<Integer>> shardIdMap = collectNode.routing().locations().get(localNodeId);
        for (Map.Entry<String, Set<Integer>> entry : shardIdMap.entrySet()) {
            String indexName = entry.getKey();
            IndexService indexService;
            try {
                indexService = indicesService.indexServiceSafe(indexName);
            } catch (IndexMissingException e) {
                throw new TableUnknownException(entry.getKey(), e);
            }

            for (Integer shardId : entry.getValue()) {
                Injector shardInjector;
                try {
                    shardInjector = indexService.shardInjectorSafe(shardId);
                    ShardCollectService shardCollectService = shardInjector.getInstance(ShardCollectService.class);
                    CrateCollector collector = shardCollectService.getCollector(
                            collectNode,
                            projectorChain
                    );
                    shardCollectors.add(collector);
                } catch (IndexShardMissingException e) {
                    throw new UnhandledServerException(
                            String.format("unknown shard id %d on index '%s'",
                                    shardId, entry.getKey()));
                } catch (Exception e) {
                    logger.error("Error while getting collector", e);
                    throw new UnhandledServerException(e);
                }
            }
        }

        // start the projection
        projectorChain.startProjections();

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

    /**
     * chose the right ShardCollectFuture for this class
     *
     * @param numShards   number of shards until the result is considered complete
     * @param projectorChain  the projector chain to process the collected rows
     * @param collectNode in case any other properties need to be extracted
     * @return a fancy ShardCollectFuture implementation
     */
    protected ShardCollectFuture getShardCollectFuture(int numShards, ShardProjectorChain projectorChain, CollectNode collectNode) {
        return new SimpleShardCollectFuture(numShards, projectorChain);
    }
}
