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

package io.crate.operation.merge;

import com.google.common.base.Optional;
import io.crate.data.Row;
import io.crate.operation.projectors.IterableRowEmitter;
import io.crate.operation.projectors.RowReceiver;

import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Class which can be used to consume a {@link PagingIterator}, feeding the rows into a {@link RowReceiver}
 */
public class PagingIteratorEmitter<TKey> {

    private final PagingIterator<TKey, Row> pagingIterator;
    private final RowReceiver rowReceiver;
    private final Function<TKey, Boolean> tryFetchMore;
    private final Consumer<Throwable> closeCallback;
    private final Executor executor;

    private boolean upstreamExhausted = false;

    /**
     * @param tryFetchMore trigger that is called once the end of the current "page" has been reached.
     *                     The argument is the "exhaustedBucket" (or null if unknown/all are exhausted).
     *
     *                     The function must return true if the fetchMore request works, false if it's not possible
     *                     to fetch more data.
     *
     * @param closeCallback Callback that is called if all data has been consumed or the rowReceiver returned STOP.
     *                      If an error occurred during setNextRow this callback is called with a throwable.
     */
    public PagingIteratorEmitter(PagingIterator<TKey, Row> pagingIterator,
                                 RowReceiver rowReceiver,
                                 Function<TKey, Boolean> tryFetchMore,
                                 Consumer<Throwable> closeCallback,
                                 Executor executor) {
        this.pagingIterator = pagingIterator;
        this.rowReceiver = rowReceiver;
        this.tryFetchMore = tryFetchMore;
        this.closeCallback = closeCallback;
        this.executor = executor;
    }

    /**
     * Consumes the {@link PagingIterator} and pushes the rows into {@link RowReceiver}.
     *
     * Whoever uses this emitter must make sure to provide the PagingIterator with data using {@link PagingIterator#merge(Iterable)}
     * before calling this function.
     */
    public void consumeItAndFetchMore() {
        if (consumeIt() == RowReceiver.Result.CONTINUE) {
            Boolean fetchMoreSucceeded = tryFetchMore.apply(pagingIterator.exhaustedIterable());
            upstreamExhausted = !fetchMoreSucceeded;
            if (upstreamExhausted) {
                consumeRemaining();
            }
        }
    }

    private void consumeRemaining() {
        pagingIterator.finish();
        if (consumeIt() == RowReceiver.Result.CONTINUE) {
            // can't continue, no more rows
            rowReceiver.finish(this::repeat);
            closeCallback.accept(null);
        }
    }

    private void repeat() {
        IterableRowEmitter emitter = new IterableRowEmitter(rowReceiver, pagingIterator.repeat(), Optional.of(executor));
        emitter.run();
    }

    private RowReceiver.Result consumeIt() {
        while (pagingIterator.hasNext()) {
            Row row = pagingIterator.next();
            RowReceiver.Result result;
            try {
                result = rowReceiver.setNextRow(row);
            } catch (Throwable t) {
                return propagateFailAndStop(t);
            }
            switch (result) {
                case CONTINUE:
                    continue;

                case PAUSE:
                    rowReceiver.pauseProcessed(this::resume);
                    return result;

                case STOP:
                    return propagateFinishAndStop();
            }
        }
        return RowReceiver.Result.CONTINUE;
    }

    private RowReceiver.Result propagateFinishAndStop() {
        rowReceiver.finish(this::repeat);
        closeCallback.accept(null);
        return RowReceiver.Result.STOP;
    }

    private RowReceiver.Result propagateFailAndStop(Throwable t) {
        rowReceiver.fail(t);
        closeCallback.accept(t);
        return RowReceiver.Result.STOP;
    }

    private void resume(boolean async) {
        Runnable runnable;
        if (upstreamExhausted) {
            runnable = this::consumeIt;
        } else {
            runnable = this::consumeItAndFetchMore;
        }
        if (async) {
            executor.execute(runnable);
        } else {
            runnable.run();
        }
    }
}
