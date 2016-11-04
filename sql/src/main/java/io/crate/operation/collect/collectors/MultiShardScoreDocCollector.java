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

import com.carrotsearch.hppc.ObjectObjectHashMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.*;
import io.crate.data.Row;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.merge.KeyIterable;
import io.crate.operation.merge.PagingIterator;
import io.crate.operation.merge.PassThroughPagingIterator;
import io.crate.operation.merge.SortedPagingIterator;
import io.crate.operation.projectors.*;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;

public class MultiShardScoreDocCollector implements CrateCollector, ResumeHandle, RepeatHandle {

    private static final Logger LOGGER = Loggers.getLogger(MultiShardScoreDocCollector.class);

    private final List<OrderedDocCollector> orderedDocCollectors;
    private final ObjectObjectHashMap<ShardId, OrderedDocCollector> orderedCollectorsMap;
    private final RowReceiver rowReceiver;
    private final PagingIterator<ShardId, Row> pagingIterator;
    private final ListeningExecutorService executor;
    private final ListeningExecutorService directExecutor;
    private final FutureCallback<List<KeyIterable<ShardId, Row>>> futureCallback;
    private final boolean singleShard;
    private IterableRowEmitter rowEmitter = null;


    public MultiShardScoreDocCollector(final List<OrderedDocCollector> orderedDocCollectors,
                                       Ordering<Row> rowOrdering,
                                       RowReceiver rowReceiver,
                                       ListeningExecutorService executor) {
        assert orderedDocCollectors.size() > 0 : "must have at least one shardContext";
        this.directExecutor = MoreExecutors.newDirectExecutorService();
        this.executor = executor;
        this.rowReceiver = rowReceiver;
        this.orderedDocCollectors = orderedDocCollectors;
        singleShard = orderedDocCollectors.size() == 1;

        boolean needsRepeat = rowReceiver.requirements().contains(Requirement.REPEAT);
        if (singleShard) {
            pagingIterator = needsRepeat ?
                PassThroughPagingIterator.repeatable() :
                PassThroughPagingIterator.oneShot();
            orderedCollectorsMap = null;
            futureCallback = null;
        } else {
            pagingIterator = new SortedPagingIterator<>(rowOrdering, needsRepeat);
            futureCallback = new FutureCallback<List<KeyIterable<ShardId, Row>>>() {
                @Override
                public void onSuccess(@Nullable List<KeyIterable<ShardId, Row>> result) {
                    assert result != null : "result must not be null";

                    pagingIterator.merge(result);
                    processIterator();
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    fail(t);
                }
            };

            this.orderedCollectorsMap = new ObjectObjectHashMap<>(orderedDocCollectors.size());
            for (OrderedDocCollector orderedDocCollector : orderedDocCollectors) {
                this.orderedCollectorsMap.put(orderedDocCollector.shardId(), orderedDocCollector);
            }
        }
    }

    @Override
    public void doCollect() {
        if (singleShard) {
            runWithoutThreads(orderedDocCollectors.get(0));
        } else {
            runThreaded();
        }
    }

    private void runThreaded() {
        List<ListenableFuture<KeyIterable<ShardId, Row>>> futures = new ArrayList<>(orderedDocCollectors.size());
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
        KeyIterable<ShardId, Row> rows;
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
                case PAUSE:
                    return;
                case STOP:
                    finish();
                    return;
                case CONTINUE:
                    if (allExhausted()) {
                        pagingIterator.finish();
                        if (emitRows() == RowReceiver.Result.PAUSE) {
                            return;
                        }
                        finish();
                    } else {
                        if (singleShard) {
                            runWithoutThreads(orderedDocCollectors.get(0));
                        } else {
                            ShardId shardId = pagingIterator.exhaustedIterable();
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

    private RowReceiver.Result emitRows() {
        while (pagingIterator.hasNext()) {
            Row row = pagingIterator.next();
            RowReceiver.Result result = rowReceiver.setNextRow(row);
            switch (result) {
                case CONTINUE:
                    continue;
                case PAUSE:
                    rowReceiver.pauseProcessed(this);
                    return result;
                case STOP:
                    return result;
            }
            throw new AssertionError("Unrecognized setNextRow result: " + result);
        }
        return RowReceiver.Result.CONTINUE;
    }

    private boolean allExhausted() {
        for (OrderedDocCollector orderedDocCollector : orderedDocCollectors) {
            if (!orderedDocCollector.exhausted()) {
                return false;
            }
        }
        return true;
    }

    private void finish() {
        closeShardContexts();
        rowReceiver.finish(this);
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
        if (rowEmitter == null) {
            rowReceiver.kill(throwable);
        } else {
            rowEmitter.kill(throwable);
        }
    }

    @Override
    public void repeat() {
        rowEmitter = new IterableRowEmitter(rowReceiver, pagingIterator.repeat());
        rowEmitter.run();
    }

    @Override
    public void resume(boolean async) {
        processIterator();
    }
}
