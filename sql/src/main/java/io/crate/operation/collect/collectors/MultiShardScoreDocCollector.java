/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.collect.collectors;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.*;
import io.crate.core.collections.Row;
import io.crate.jobs.KeepAliveListener;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.merge.NumberedIterable;
import io.crate.operation.merge.PagingIterator;
import io.crate.operation.merge.PassThroughPagingIterator;
import io.crate.operation.merge.SortedPagingIterator;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.RowReceiver;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;

public class MultiShardScoreDocCollector implements CrateCollector {

    private static final ESLogger LOGGER = Loggers.getLogger(MultiShardScoreDocCollector.class);

    private static final int KEEP_ALIVE_AFTER_ROWS = 1_000_000;
    private final List<OrderedDocCollector> orderedDocCollectors;
    private final IntObjectOpenHashMap<OrderedDocCollector> orderedCollectorsMap;
    private final RowReceiver rowReceiver;
    private final PagingIterator<Row> pagingIterator;
    private final TopRowUpstream topRowUpstream;
    private final ListeningExecutorService executor;
    private final ListeningExecutorService directExecutor;
    private final FutureCallback<List<NumberedIterable<Row>>> futureCallback;
    private final KeepAliveListener keepAliveListener;
    private final FlatProjectorChain flatProjectorChain;
    private final boolean singleShard;

    private long rowCount = 0;

    public MultiShardScoreDocCollector(final List<OrderedDocCollector> orderedDocCollectors,
                                       KeepAliveListener keepAliveListener,
                                       Ordering<Row> rowOrdering,
                                       FlatProjectorChain flatProjectorChain,
                                       ListeningExecutorService executor) {
        this.flatProjectorChain = flatProjectorChain;
        assert orderedDocCollectors.size() > 0 : "must have at least one shardContext";
        this.keepAliveListener = keepAliveListener;
        this.directExecutor = MoreExecutors.newDirectExecutorService();
        this.executor = executor;
        this.topRowUpstream = new TopRowUpstream(executor, new Runnable() {
            @Override
            public void run() {
                processIterator();
            }
        }, new Runnable() {
            @Override
            public void run() {
                if (singleShard) {
                    runWithoutThreads(orderedDocCollectors.get(0));
                } else {
                    runThreaded();
                }
            }
        });
        rowReceiver = flatProjectorChain.firstProjector();
        rowReceiver.setUpstream(topRowUpstream);
        this.orderedDocCollectors = orderedDocCollectors;
        singleShard = orderedDocCollectors.size() == 1;

        if (singleShard) {
            pagingIterator = new PassThroughPagingIterator<>();
            orderedCollectorsMap = null;
            futureCallback = null;
        } else {
            pagingIterator = new SortedPagingIterator<>(rowOrdering);
            futureCallback = new FutureCallback<List<NumberedIterable<Row>>>() {
                @Override
                public void onSuccess(@Nullable List<NumberedIterable<Row>> result) {
                    assert result != null : "result must not be null";

                    pagingIterator.merge(result);
                    processIterator();
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    fail(t);
                }
            };

            this.orderedCollectorsMap = new IntObjectOpenHashMap<>(orderedDocCollectors.size());
            for (OrderedDocCollector orderedDocCollector : orderedDocCollectors) {
                this.orderedCollectorsMap.put(orderedDocCollector.shardId(), orderedDocCollector);
            }
        }
    }

    @Override
    public void doCollect() {
        flatProjectorChain.startProjections(topRowUpstream);
        if (singleShard) {
            runWithoutThreads(orderedDocCollectors.get(0));
        } else {
            runThreaded();
        }
    }

    private void runThreaded() {
        List<ListenableFuture<NumberedIterable<Row>>> futures = new ArrayList<>(orderedDocCollectors.size());
        for (OrderedDocCollector orderedDocCollector : orderedDocCollectors.subList(1, this.orderedDocCollectors.size())) {
            try {
                futures.add(executor.submit(orderedDocCollector));
            } catch (EsRejectedExecutionException | RejectedExecutionException e) {
                futures.add(directExecutor.submit(orderedDocCollector));
            }
        }
        // since the whole MultiShardScoreDocCollector already runs in the search threadPool we can
        // run one orderedCollector in the same thread to avoid some context switching
        futures.add(directExecutor.submit(orderedDocCollectors.get(0)));
        Futures.addCallback(Futures.allAsList(futures), futureCallback);
    }

    private void runWithoutThreads(OrderedDocCollector orderedDocCollector) {
        NumberedIterable<Row> rows;
        try {
            rows = orderedDocCollector.call();
        } catch (Exception e) {
            fail(e);
            return;
        }
        pagingIterator.merge(ImmutableList.of(rows));
        processIterator();
    }

    private void processIterator() {
        try {
            switch (emitRows()) {
                case PAUSED:
                    return;
                case FINISHED:
                    finish();
                    return;
                case EXHAUSTED:
                    if (allExhausted()) {
                        pagingIterator.finish();
                        if (emitRows() == Result.PAUSED) {
                            return;
                        }
                        finish();
                    } else {
                        if (singleShard) {
                            runWithoutThreads(orderedDocCollectors.get(0));
                        } else {
                            int shardId = pagingIterator.exhaustedIterable();
                            LOGGER.trace("Iterator {} exhausted. Retrieving more data", shardId);
                            runWithoutThreads(orderedCollectorsMap.get(shardId));
                        }
                    }
                    break;
            }
        } catch (Throwable t) {
            fail(t);
        }
    }

    private Result emitRows() {
        while (pagingIterator.hasNext()) {
            topRowUpstream.throwIfKilled();

            Row row = pagingIterator.next();
            rowCount++;

            if (rowCount % KEEP_ALIVE_AFTER_ROWS == 0) {
                keepAliveListener.keepAlive();
            }

            boolean wantMore = rowReceiver.setNextRow(row);
            if (!wantMore) {
                return Result.FINISHED;
            }
            if (topRowUpstream.shouldPause()) {
                topRowUpstream.pauseProcessed();
                return Result.PAUSED;
            }
        }
        return Result.EXHAUSTED;
    }

    private boolean allExhausted() {
        for (OrderedDocCollector orderedDocCollector : orderedDocCollectors) {
            if (!orderedDocCollector.exhausted) {
                return false;
            }
        }
        return true;
    }

    private void finish() {
        closeShardContexts();
        rowReceiver.finish();
    }

    private void fail(@Nonnull Throwable t) {
        closeShardContexts();
        rowReceiver.fail(t);
    }

    private void closeShardContexts() {
        for (OrderedDocCollector orderedDocCollector : orderedDocCollectors) {
            orderedDocCollector.close();
        }
    }

    @Override
    public void kill(@Nullable Throwable throwable) {
        topRowUpstream.kill(throwable);
    }
}
