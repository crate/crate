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

package io.crate.operation.projectors.ng;

import io.crate.concurrent.CompletableFutures;
import io.crate.data.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BooleanSupplier;
import java.util.stream.Collector;

public class CollectingProjectors {

    private static final Iterable<?> ONE_ROW_ITERABLE = Collections.singletonList(null);

    public static BatchIterator collecting(BatchIterator source,
                                           BatchCollectorListener listener,
                                           Columns rowData) {
        return new CloseAssertingBatchIterator(new LazyCollectingBatchIterator(source, listener, rowData));
    }

    /**
     * A projector which will consume the source, summing up the first column (must be of type long).
     * and emitting one row with the sum.
     * <p>
     * <pre>
     *     input:
     *     [ 1, 2, 2, 1 ]
     *
     *     output:
     *     [ 6 ]
     * </pre>
     */
    public static final BatchIteratorProjector SUMMING_LONG = (BatchIterator source) -> {
        assert source.rowData().size() >= 1 : "summing long requires at least one column";
        final Input<?> input = source.rowData().get(0);
        final long[] state = {0L};
        BatchCollectorListener listener = new BatchCollectorListener() {
            @Override
            public boolean onRow() {
                Object v = input.value();
                if (v != null) {
                    assert v instanceof Long : "a long is required";
                    state[0] += (long) v;
                }
                return true;
            }

            @Override
            public Iterable<?> post() {
                return ONE_ROW_ITERABLE;
            }
        };
        return collecting(source, listener, Columns.singleCol(() -> state[0]));
    };

    /**
     * The collecting interferface used to collect and aggegate the rows of a batch iterator.
     *
     * This does not expose any data handling since this should be done via seperate {@link Columns} objects.
     */
    interface BatchCollectorListener {

        /**
         * Called when on a new valid row position.
         *
         * @return true if the iteration should continue
         */
        boolean onRow();

        /**
         * Preperation hook which is called before starting the iteration.
         */
        default void pre() {

        }

        /**
         * Finalization hook which is called once all rows have been visited.
         *
         * The iterable returned here is the controller of the projected rows. The row values of this iterator
         * is not used, however the implementation needs to upate its output {@link Columns} when the iterator
         * is advanced.
         *
         * @return an iterable for controlling the output rows
         */
        default Iterable<?> post() {
            return Collections.emptyList();
        }

        /**
         * example to show how a {@link Collector} can be mapped this interface
         */
        static <A> BatchCollectorListener fromCollector(Collector<Void, A, ? extends Iterable<?>> collector) {
            return new BatchCollectorListener() {

                A state;

                @Override
                public void pre() {
                    state = collector.supplier().get();
                }

                @Override
                public boolean onRow() {
                    collector.accumulator().accept(state, null);
                    return true;
                }

                @Override
                public Iterable<?> post() {
                    return collector.finisher().apply(state);
                }
            };
        }
    }

    /**
     * A batch iterator which starts collecting a whole source batch iterator lazily when {@link #loadNextBatch()}
     * is called. This class can be used by projector implementations which require all rows from the upstream.
     */
    private static class LazyCollectingBatchIterator implements BatchIterator {

        private final Columns rowData;
        private CompletableFuture<Void> resultFuture;


        private final BatchIterator source;
        private final BatchCollectorListener listener;
        private Iterable<?> iterable;
        private Iterator<?> it;

        public LazyCollectingBatchIterator(BatchIterator source, BatchCollectorListener listener, Columns rowData) {
            this.source = source;
            this.listener = listener;
            this.rowData = rowData;
        }

        @Override
        public Columns rowData() {
            return rowData;
        }

        @Override
        public void moveToStart() {
            raiseIfLoading();
            if (iterable == null) {
                throw new IllegalStateException("CollectingBatchIterator can only move to start when all data is loaded");
            }
            it = iterable.iterator();
        }

        @Override
        public boolean moveNext() {
            if (it != null) {
                if (it.hasNext()) {
                    it.next();
                    return true;
                }
                return false;
            }
            raiseIfLoading();
            return false;
        }

        @Override
        public void close() {
            source.close();
            it = null;
        }

        @Override
        public CompletionStage<?> loadNextBatch() {
            if (resultFuture == null) {
                resultFuture = new CompletableFuture<>();
                listener.pre();
                consumeBatchIterator(source, listener::onRow, resultFuture);
                return resultFuture.whenComplete((r, t) -> {
                    if (t == null) {
                        iterable = listener.post();
                        moveToStart();
                    }
                    source.close();
                });
            }
            return CompletableFutures.failedFuture(new IllegalStateException("BatchIterator already loaded"));
        }

        @Override
        public boolean allLoaded() {
            return resultFuture != null;
        }

        private void raiseIfLoading() {
            if (resultFuture != null && !resultFuture.isDone()) {
                throw new IllegalStateException("BatchIterator is loading");
            }
        }
    }

    private static void consumeBatchIterator(BatchIterator it,
                                             BooleanSupplier moveListener,
                                             CompletableFuture<?> resultFuture) {
        try {
            while (it.moveNext()) {
                if (!moveListener.getAsBoolean()) {
                    resultFuture.complete(null);
                    return;
                }
            }
        } catch (Throwable t) {
            resultFuture.completeExceptionally(t);
            return;
        }

        if (it.allLoaded()) {
            resultFuture.complete(null);
        } else {
            it.loadNextBatch().whenComplete((r, t) -> {
                if (t == null) {
                    consumeBatchIterator(it, moveListener, resultFuture);
                } else {
                    resultFuture.completeExceptionally(t);
                }
            });
        }
    }
}
