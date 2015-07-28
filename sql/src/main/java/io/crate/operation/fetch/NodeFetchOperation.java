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
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import io.crate.action.sql.query.CrateSearchContext;
import io.crate.breaker.RamAccountingContext;
import io.crate.exceptions.ContextMissingException;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.metadata.Functions;
import io.crate.operation.Input;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowUpstream;
import io.crate.operation.ThreadPools;
import io.crate.operation.collect.CollectInputSymbolVisitor;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.projectors.ResultProvider;
import io.crate.operation.reference.DocLevelReferenceResolver;
import io.crate.operation.reference.doc.lucene.LuceneDocLevelReferenceResolver;
import io.crate.planner.symbol.Reference;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

public class NodeFetchOperation implements RowUpstream {

    private final UUID jobId;
    private final int executionPhaseId;
    private final List<Reference> toFetchReferences;
    private final boolean closeContext;
    private final IntObjectOpenHashMap<ShardDocIdsBucket> shardBuckets = new IntObjectOpenHashMap<>();

    private final JobContextService jobContextService;
    private final RamAccountingContext ramAccountingContext;
    private final CollectInputSymbolVisitor<?> docInputSymbolVisitor;
    private final ThreadPoolExecutor executor;
    private final int poolSize;

    private int inputCursor = 0;

    private static final ESLogger LOGGER = Loggers.getLogger(NodeFetchOperation.class);

    public NodeFetchOperation(UUID jobId,
                              int executionPhaseId,
                              LongArrayList jobSearchContextDocIds,
                              List<Reference> toFetchReferences,
                              boolean closeContext,
                              JobContextService jobContextService,
                              ThreadPool threadPool,
                              Functions functions,
                              RamAccountingContext ramAccountingContext) {
        this.jobId = jobId;
        this.executionPhaseId = executionPhaseId;
        this.toFetchReferences = toFetchReferences;
        this.closeContext = closeContext;
        this.jobContextService = jobContextService;
        this.ramAccountingContext = ramAccountingContext;
        executor = (ThreadPoolExecutor) threadPool.executor(ThreadPool.Names.SEARCH);
        poolSize = executor.getCorePoolSize();

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

    public void fetch(ResultProvider resultProvider) throws Exception {
        int numShards = shardBuckets.size();

        JobExecutionContext jobExecutionContext = jobContextService.getContext(jobId);
        final JobCollectContext jobCollectContext = jobExecutionContext.getSubContext(executionPhaseId);

        RowDownstream upstreamsRowMerger = new PositionalRowMerger(resultProvider, toFetchReferences.size());
        if (closeContext) {
            resultProvider.result().addListener(new Runnable() {
                @Override
                public void run() {
                    jobCollectContext.close();
                }
            }, MoreExecutors.directExecutor());
        }

        List<LuceneDocFetcher> shardFetchers = new ArrayList<>(numShards);
        for (IntObjectCursor<ShardDocIdsBucket> entry : shardBuckets) {
            CrateSearchContext searchContext = jobCollectContext.getContext(entry.key);
            if (searchContext == null) {
                throw new ContextMissingException(ContextMissingException.ContextType.SEARCH_CONTEXT, jobId, entry.key);
            }
            // create new collect expression for every shard (collect expressions are not thread-safe)
            CollectInputSymbolVisitor.Context docCtx = docInputSymbolVisitor.extractImplementations(toFetchReferences);
            shardFetchers.add(
                    new LuceneDocFetcher(
                            docCtx.topLevelInputs(),
                            docCtx.docLevelExpressions(),
                            upstreamsRowMerger,
                            entry.value,
                            closeContext,
                            searchContext,
                            jobCollectContext));
        }
        try {
            runFetchThreaded(shardFetchers, ramAccountingContext);
        } catch (RejectedExecutionException e) {
            resultProvider.registerUpstream(this).fail(e);
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("started {} shardFetchers", numShards);
        }
    }

    private void runFetchThreaded(final List<LuceneDocFetcher> shardFetchers,
                                  final RamAccountingContext ramAccountingContext) throws RejectedExecutionException {

        ThreadPools.runWithAvailableThreads(
                executor,
                poolSize,
                Lists.transform(shardFetchers, new Function<LuceneDocFetcher, Runnable>() {

                    @Nullable
                    public Runnable apply(final LuceneDocFetcher input) {
                        return new Runnable() {
                            @Override
                            public void run() {
                                input.doFetch(ramAccountingContext);
                            }
                        };
                    }
                })
        );
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
