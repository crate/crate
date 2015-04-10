/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.fetch;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.metadata.Functions;
import io.crate.operation.Input;
import io.crate.operation.RowDownstream;
import io.crate.operation.ThreadPools;
import io.crate.operation.collect.*;
import io.crate.operation.projectors.Projector;
import io.crate.operation.reference.DocLevelReferenceResolver;
import io.crate.operation.reference.doc.lucene.LuceneDocLevelReferenceResolver;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Reference;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

public class NodeFetchOperation {

    private final UUID jobId;
    private final List<Reference> toFetchReferences;
    private final boolean closeContext;
    private final IntObjectOpenHashMap<ShardDocIdsBucket> shardBuckets = new IntObjectOpenHashMap();

    private final CollectContextService collectContextService;
    private final RamAccountingContext ramAccountingContext;
    private final CollectInputSymbolVisitor<?> docInputSymbolVisitor;
    private final ThreadPoolExecutor executor;
    private final int poolSize;

    private int inputCursor = 0;

    private static final ESLogger LOGGER = Loggers.getLogger(NodeFetchOperation.class);

    public NodeFetchOperation(UUID jobId,
                              LongArrayList jobSearchContextDocIds,
                              List<Reference> toFetchReferences,
                              boolean closeContext,
                              CollectContextService collectContextService,
                              ThreadPool threadPool,
                              Functions functions,
                              RamAccountingContext ramAccountingContext) {
        this.jobId = jobId;
        this.toFetchReferences = toFetchReferences;
        this.closeContext = closeContext;
        this.collectContextService = collectContextService;
        this.ramAccountingContext = ramAccountingContext;
        executor = (ThreadPoolExecutor) threadPool.executor(ThreadPool.Names.SEARCH);
        poolSize = executor.getPoolSize();

        DocLevelReferenceResolver<? extends Input<?>> resolver = new LuceneDocLevelReferenceResolver(null);
        this.docInputSymbolVisitor = new CollectInputSymbolVisitor<>(
                functions,
                resolver
        );

        createShardBuckets(jobSearchContextDocIds);
    }

    private void createShardBuckets(LongArrayList jobSearchContextDocIds) {
        for (LongCursor jobSearchContextDocIdCursor : jobSearchContextDocIds) {
            // unpack jobSearchContextId and docId integers from jobSearchContextDocId long
            long jobSearchContextDocId = jobSearchContextDocIdCursor.value;
            int jobSearchContextId = (int)(jobSearchContextDocId >> 32);
            int docId = (int)jobSearchContextDocId;

            ShardDocIdsBucket shardDocIdsBucket = shardBuckets.get(jobSearchContextId);
            if (shardDocIdsBucket == null) {
                shardDocIdsBucket = new ShardDocIdsBucket();
                shardBuckets.put(jobSearchContextId, shardDocIdsBucket);
            }
            shardDocIdsBucket.add(inputCursor++, docId);
        }
    }

    public ListenableFuture<Bucket> fetch() throws Exception {
        int numShards = shardBuckets.size();
        ShardProjectorChain projectorChain = new ShardProjectorChain(numShards,
                ImmutableList.<Projection>of(), null, ramAccountingContext);

        final ShardCollectFuture result = new MapSideDataCollectOperation.SimpleShardCollectFuture(
                numShards, projectorChain.resultProvider().result(), collectContextService, jobId);

        JobCollectContext jobCollectContext = collectContextService.acquireContext(jobId, false);
        if (jobCollectContext == null) {
            String errorMsg = String.format(Locale.ENGLISH, "No jobCollectContext found for job '%s'", jobId);
            LOGGER.error(errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }

        Projector downstream = projectorChain.newShardDownstreamProjector(null);
        RowDownstream upstreamsRowMerger = new PositionalRowMerger(downstream, toFetchReferences.size());

        List<LuceneDocFetcher> shardFetchers = new ArrayList<>(numShards);
        for (IntObjectCursor<ShardDocIdsBucket> entry : shardBuckets) {
            LuceneDocCollector docCollector = jobCollectContext.findCollector(entry.key);
            if (docCollector == null) {
                String errorMsg = String.format(Locale.ENGLISH, "No lucene collector found for job search context id '%s'", entry.key);
                LOGGER.error(errorMsg);
                throw new IllegalArgumentException(errorMsg);
            }
            // create new collect expression for every shard (collect expressions are not thread-safe)
            CollectInputSymbolVisitor.Context docCtx = docInputSymbolVisitor.process(toFetchReferences);
            shardFetchers.add(
                    new LuceneDocFetcher(
                            docCtx.topLevelInputs(),
                            docCtx.docLevelExpressions(),
                            upstreamsRowMerger,
                            entry.value,
                            jobCollectContext,
                            docCollector.searchContext(),
                            entry.key,
                            closeContext));
        }


        // start the projection
        projectorChain.startProjections();
        try {
            runFetchThreaded(result, shardFetchers, ramAccountingContext);
        } catch (RejectedExecutionException e) {
            result.shardFailure(e);
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("started {} shardFetchers", numShards);
        }

        return result;
    }

    private void runFetchThreaded(final ShardCollectFuture result,
                                  final List<LuceneDocFetcher> shardFetchers,
                                  final RamAccountingContext ramAccountingContext) throws RejectedExecutionException {

        ThreadPools.runWithAvailableThreads(
                executor,
                poolSize,
                Lists.transform(shardFetchers, new Function<LuceneDocFetcher, Runnable>() {

                    @Nullable
                    @Override
                    public Runnable apply(final LuceneDocFetcher input) {
                        return new Runnable() {
                            @Override
                            public void run() {
                                doFetch(result, input, ramAccountingContext);
                            }
                        };
                    }
                })
        );
    }


    private void doFetch(ShardCollectFuture result, LuceneDocFetcher fetcher,
                         RamAccountingContext ramAccountingContext) {
        try {
            fetcher.doFetch(ramAccountingContext);
            result.shardFinished();
        } catch (FetchAbortedException ex) {
            // ignore
        } catch (Exception ex) {
            result.shardFailure(ex);
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("shard finished fetch, {} to go", result.numShards());
        }
    }


    static class ShardDocIdsBucket {

        private final IntArrayList positions = new IntArrayList();
        private final IntArrayList docIds = new IntArrayList();

        public void add(int position, int docId) {
            positions.add(position);
            docIds.add(docId);
        }

        public int docId(int index) {
            return docIds.get(index);
        }

        public int size() {
            return docIds.size();
        }

        public int position(int idx) {
            return positions.get(idx);
        }
    }

}
