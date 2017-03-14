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

package io.crate.operation.collect;

import io.crate.data.BatchConsumer;
import io.crate.data.BatchIterator;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Collector adapter to a BatchIterator.
 * {@link CrateCollector#doCollect()} results in a {@link BatchConsumer#accept(BatchIterator, Throwable)} call on the consumer.
 */
public final class BatchIteratorCollectorBridge {

    public static CrateCollector newInstance(BatchIterator batchIterator, BatchConsumer consumer) {
        return new SyncBatchItCollector(batchIterator, consumer);
    }

    public static CrateCollector newInstance(Supplier<CompletableFuture<BatchIterator>> batchIteratorFuture,
                                             BatchConsumer consumer) {
        return new AsyncBatchItCollector(batchIteratorFuture, consumer);
    }

    private static class SyncBatchItCollector implements CrateCollector {

        private final BatchIterator batchIterator;
        private final BatchConsumer consumer;

        private boolean started;

        SyncBatchItCollector(BatchIterator batchIterator, BatchConsumer consumer) {
            this.batchIterator = batchIterator;
            this.consumer = consumer;
        }

        @Override
        public synchronized void doCollect() {
            if (!started) {
                started = true;
                consumer.accept(batchIterator, null);
            }
            // else: got killed
        }

        @Override
        public synchronized void kill(Throwable throwable) {
            if (started) {
                batchIterator.kill(throwable);
            } else {
                started = true;
                consumer.accept(null, throwable);
            }
        }
    }

    private static class AsyncBatchItCollector implements CrateCollector {

        private final Supplier<CompletableFuture<BatchIterator>> batchIteratorSupplier;
        private final BatchConsumer consumer;

        private BatchIterator batchIterator;
        private Throwable killed = null;


        AsyncBatchItCollector(Supplier<CompletableFuture<BatchIterator>> batchIteratorSupplier,
                              BatchConsumer consumer) {
            this.batchIteratorSupplier = batchIteratorSupplier;
            this.consumer = consumer;
        }

        @Override
        public synchronized void doCollect() {
            if (killed == null) {
                batchIteratorSupplier.get().whenComplete((bi, failure) -> {
                    batchIterator = bi;
                    consumer.accept(bi, failure);
                });
            } else {
                consumer.accept(null, killed);
            }
        }

        @Override
        public synchronized void kill(Throwable throwable) {
            if (batchIterator == null) {
                killed = throwable;
            } else {
                batchIterator.kill(throwable);
            }
        }
    }
}
