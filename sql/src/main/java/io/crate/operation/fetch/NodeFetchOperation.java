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

import com.carrotsearch.hppc.IntContainer;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Streamer;
import io.crate.analyze.symbol.Symbols;
import io.crate.executor.transport.StreamBucket;
import io.crate.metadata.Reference;
import io.crate.metadata.TableIdent;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.operation.reference.doc.lucene.LuceneReferenceResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Singleton
public class NodeFetchOperation {

    private final Executor executor;

    private static class TableFetchInfo {

        private final Streamer<?>[] streamers;
        private final Collection<Reference> refs;
        private final FetchContext fetchContext;

        TableFetchInfo(Collection<Reference> refs, FetchContext fetchContext) {
            this.refs = refs;
            this.fetchContext = fetchContext;
            this.streamers = Symbols.streamerArray(refs);
        }

        FetchCollector createCollector(int readerId) {
            IndexService indexService = fetchContext.indexService(readerId);
            LuceneReferenceResolver resolver = new LuceneReferenceResolver(indexService.mapperService());
            ArrayList<LuceneCollectorExpression<?>> exprs = new ArrayList<>(refs.size());
            for (Reference reference : refs) {
                exprs.add(resolver.getImplementation(reference));
            }
            return new FetchCollector(
                exprs,
                streamers,
                indexService.mapperService(),
                fetchContext.searcher(readerId),
                indexService.fieldData()
            );
        }
    }

    @Inject
    public NodeFetchOperation(ThreadPool threadPool) {
        executor = threadPool.executor(ThreadPool.Names.SEARCH);
    }

    private HashMap<TableIdent, TableFetchInfo> getTableFetchInfos(FetchContext fetchContext) {
        HashMap<TableIdent, TableFetchInfo> result = new HashMap<>(fetchContext.toFetch().size());
        for (Map.Entry<TableIdent, Collection<Reference>> entry : fetchContext.toFetch().entrySet()) {
            TableFetchInfo tableFetchInfo = new TableFetchInfo(entry.getValue(), fetchContext);
            result.put(entry.getKey(), tableFetchInfo);
        }
        return result;
    }

    public ListenableFuture<IntObjectMap<StreamBucket>> doFetch(
        final FetchContext fetchContext, @Nullable IntObjectMap<? extends IntContainer> toFetch) throws Exception {
        if (toFetch == null) {
            return Futures.<IntObjectMap<StreamBucket>>immediateFuture(new IntObjectHashMap<StreamBucket>(0));
        }

        final IntObjectHashMap<StreamBucket> fetched = new IntObjectHashMap<>(toFetch.size());
        HashMap<TableIdent, TableFetchInfo> tableFetchInfos = getTableFetchInfos(fetchContext);
        final AtomicReference<Throwable> lastThrowable = new AtomicReference<>(null);
        final AtomicInteger threadLatch = new AtomicInteger(toFetch.size());

        final SettableFuture<IntObjectMap<StreamBucket>> resultFuture = SettableFuture.create();
        for (IntObjectCursor<? extends IntContainer> toFetchCursor : toFetch) {
            final int readerId = toFetchCursor.key;
            final IntContainer docIds = toFetchCursor.value;

            TableIdent ident = fetchContext.tableIdent(readerId);
            final TableFetchInfo tfi = tableFetchInfos.get(ident);
            assert tfi != null;

            CollectRunnable runnable = new CollectRunnable(
                tfi.createCollector(readerId),
                docIds,
                fetched,
                readerId,
                lastThrowable,
                threadLatch,
                resultFuture
            );
            try {
                executor.execute(runnable);
            } catch (EsRejectedExecutionException | RejectedExecutionException e) {
                runnable.run();
            }
        }
        return resultFuture;
    }

    private static class CollectRunnable implements Runnable {
        private final FetchCollector collector;
        private final IntContainer docIds;
        private final IntObjectHashMap<StreamBucket> fetched;
        private final int readerId;
        private final AtomicReference<Throwable> lastThrowable;
        private final AtomicInteger threadLatch;
        private final SettableFuture<IntObjectMap<StreamBucket>> resultFuture;

        CollectRunnable(FetchCollector collector,
                        IntContainer docIds,
                        IntObjectHashMap<StreamBucket> fetched,
                        int readerId,
                        AtomicReference<Throwable> lastThrowable,
                        AtomicInteger threadLatch,
                        SettableFuture<IntObjectMap<StreamBucket>> resultFuture) {
            this.collector = collector;
            this.docIds = docIds;
            this.fetched = fetched;
            this.readerId = readerId;
            this.lastThrowable = lastThrowable;
            this.threadLatch = threadLatch;
            this.resultFuture = resultFuture;
        }

        @Override
        public void run() {
            try {
                StreamBucket bucket = collector.collect(docIds);
                synchronized (fetched) {
                    fetched.put(readerId, bucket);
                }
            } catch (Exception e) {
                lastThrowable.set(e);
            } finally {
                if (threadLatch.decrementAndGet() == 0) {
                    Throwable throwable = lastThrowable.get();
                    if (throwable == null) {
                        resultFuture.set(fetched);
                    } else {
                        resultFuture.setException(throwable);
                    }
                }
            }
        }
    }
}
