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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.metadata.Functions;
import io.crate.operation.Input;
import io.crate.operation.collect.*;
import io.crate.operation.projectors.Projector;
import io.crate.operation.reference.DocLevelReferenceResolver;
import io.crate.operation.reference.doc.lucene.LuceneDocLevelReferenceResolver;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.*;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

public class NodeFetchOperation {

    private final UUID jobId;
    private final Map<Integer, IntArrayList> jobSearchContextDocIds;
    private final List<Symbol> toFetchSymbols;
    private final boolean closeContext;

    private final CollectContextService collectContextService;
    private final RamAccountingContext ramAccountingContext;
    private final CollectInputSymbolVisitor<?> docInputSymbolVisitor;
    private final String executorName = ThreadPool.Names.SEARCH;
    private final ThreadPoolExecutor executor;
    private final int poolSize;

    private final ESLogger logger = Loggers.getLogger(getClass());

    public NodeFetchOperation(UUID jobId,
                              Map<Integer, IntArrayList> jobSearchContextDocIds,
                              List<Symbol> toFetchSymbols,
                              boolean closeContext,
                              CollectContextService collectContextService,
                              ThreadPool threadPool,
                              Functions functions,
                              RamAccountingContext ramAccountingContext) {
        this.jobId = jobId;
        this.jobSearchContextDocIds = jobSearchContextDocIds;
        this.toFetchSymbols = toFetchSymbols;
        this.closeContext = closeContext;
        this.collectContextService = collectContextService;
        this.ramAccountingContext = ramAccountingContext;
        executor = (ThreadPoolExecutor) threadPool.executor(executorName);
        poolSize = executor.getPoolSize();

        DocLevelReferenceResolver<? extends Input<?>> resolver = LuceneDocLevelReferenceResolver.INSTANCE;
        this.docInputSymbolVisitor = new CollectInputSymbolVisitor<>(
                functions,
                resolver
        );

    }

    public ListenableFuture<Bucket> fetch() throws Exception {
        int numShards = jobSearchContextDocIds.size();
        ShardProjectorChain projectorChain = new ShardProjectorChain(numShards,
                ImmutableList.<Projection>of(), null, ramAccountingContext);

        final ShardCollectFuture result = new MapSideDataCollectOperation.SimpleShardCollectFuture(
                numShards, projectorChain.resultProvider().result(), collectContextService, jobId);

        JobCollectContext jobCollectContext = collectContextService.acquireContext(jobId, false);
        if (jobCollectContext == null) {
            String errorMsg = String.format(Locale.ENGLISH, "No jobCollectContext found for job '%s'", jobId);
            logger.error(errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }

        CollectInputSymbolVisitor.Context docCtx = docInputSymbolVisitor.process(toFetchSymbols);
        Projector downstream = projectorChain.newShardDownstreamProjector(null);

        List<LuceneDocFetcher> shardFetchers = new ArrayList<>(numShards);
        for (Map.Entry<Integer, IntArrayList> entry : jobSearchContextDocIds.entrySet()) {
            LuceneDocCollector docCollector = jobCollectContext.findCollector(entry.getKey());
            if (docCollector == null) {
                String errorMsg = String.format(Locale.ENGLISH, "No lucene collector found for job search context id '%s'", entry.getKey());
                logger.error(errorMsg);
                throw new IllegalArgumentException(errorMsg);
            }
            docCollector.searchContext().docIdsToLoad(entry.getValue().toArray(), 0, entry.getValue().size());
            shardFetchers.add(
                    new LuceneDocFetcher(
                            docCtx.topLevelInputs(),
                            docCtx.docLevelExpressions(),
                            downstream,
                            jobCollectContext,
                            docCollector.searchContext(),
                            entry.getKey(),
                            closeContext));
        }


        // start the projection
        projectorChain.startProjections();
        try {
            runFetchThreaded(result, shardFetchers, ramAccountingContext);
        } catch (RejectedExecutionException e) {
            result.shardFailure(e);
        }

        if (logger.isTraceEnabled()) {
            logger.trace("started {} shardFetchers", numShards);
        }

        return result;
    }

    private void runFetchThreaded(final ShardCollectFuture result,
                                  final List<LuceneDocFetcher> shardFetchers,
                                  final RamAccountingContext ramAccountingContext) throws RejectedExecutionException {
        int availableThreads = Math.max(poolSize - executor.getActiveCount(), 2);
        if (availableThreads < shardFetchers.size()) {
            Iterable<List<LuceneDocFetcher>> partition = Iterables.partition(
                    shardFetchers, shardFetchers.size() / availableThreads);
            for (final List<LuceneDocFetcher> fetchers : partition) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        for (LuceneDocFetcher fetcher : fetchers) {
                            doFetch(result, fetcher, ramAccountingContext);
                        }
                    }
                });
            }
        } else {
            for (final LuceneDocFetcher fetcher : shardFetchers) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        doFetch(result, fetcher, ramAccountingContext);
                    }
                });
            }
        }
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
        if (logger.isTraceEnabled()) {
            logger.trace("shard finished fetch, {} to go", result.numShards());
        }
    }


}
