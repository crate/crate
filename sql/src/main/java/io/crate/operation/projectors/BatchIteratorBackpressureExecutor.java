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

package io.crate.operation.projectors;

import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.RowBridging;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Consumes a BatchIterator in bulks based on the configured {@link #bulkSize}. When a bulk is complete, it checks
 * whether executing the bulk is possible (based on the provided {@link #backPressureTrigger}) in which case it runs
 * the {@link #executeFunction}. Otherwise, it stops consuming the iterator and schedules retrying the execution of the
 * bulk for later.
 * Iterator consumption is resumed once the parked/scheduled bulk is executed successfully.
 *
 * @param <R> the type of the result the {@link #executeFunction} future returns.
 */
public class BatchIteratorBackpressureExecutor<R> {

    private final BatchIterator batchIterator;
    private final Function<Boolean, CompletableFuture<R>> executeFunction;
    private final Consumer<Row> onRowConsumer;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean collectingEnabled = new AtomicBoolean(false);
    private final AtomicBoolean isScheduledCollectionRunning = new AtomicBoolean(false);
    private final AtomicBoolean computeFinalResult = new AtomicBoolean(false);
    private final Iterator<TimeValue> consumeIteratorDelays;
    private final BooleanSupplier backPressureTrigger;
    private final BiConsumer<R, Throwable> bulkExecutionCompleteListener;
    private final BiConsumer<Object, Throwable> loadNextBatchCompleteListener;
    private final Runnable scheduleConsumeIteratorJob;
    private final int bulkSize;
    private final AtomicInteger inFlightExecutions = new AtomicInteger(0);
    private final CompletableFuture<Void> resultFuture = new CompletableFuture<>();

    private int indexInBulk = 0;
    private volatile boolean lastBulkScheduledToExecute = false;

    public BatchIteratorBackpressureExecutor(BatchIterator batchIterator,
                                             ScheduledExecutorService scheduler,
                                             Consumer<Row> onRowConsumer,
                                             Function<Boolean, CompletableFuture<R>> executeFunction,
                                             BooleanSupplier backPressureTrigger,
                                             int bulkSize,
                                             BackoffPolicy backoffPolicy) {
        this.batchIterator = batchIterator;
        this.scheduler = scheduler;
        this.onRowConsumer = onRowConsumer;
        this.executeFunction = executeFunction;
        this.backPressureTrigger = backPressureTrigger;
        this.bulkSize = bulkSize;
        this.consumeIteratorDelays = backoffPolicy.iterator();
        this.bulkExecutionCompleteListener = createBulkExecutionCompleteListener(batchIterator);
        this.loadNextBatchCompleteListener = createLoadNextBatchListener(batchIterator);
        this.scheduleConsumeIteratorJob = createScheduleConsumeIteratorJob(batchIterator);
    }

    public CompletableFuture<Void> consumeIteratorAndExecute() {
        safeConsumeIterator(batchIterator);
        return resultFuture;
    }

    private Runnable createScheduleConsumeIteratorJob(BatchIterator batchIterator) {
        return () -> {
            isScheduledCollectionRunning.set(false);
            safeConsumeIterator(batchIterator);
        };
    }

    private BiConsumer<Object, Throwable> createLoadNextBatchListener(BatchIterator batchIterator) {
        return (r, t) -> {
            if (t == null) {
                unsafeConsumeIterator(batchIterator);
            } else {
                resultFuture.completeExceptionally(t);
            }
        };
    }

    private BiConsumer<R, Throwable> createBulkExecutionCompleteListener(BatchIterator batchIterator) {
        return (r, t) -> {
            int inFlight = inFlightExecutions.decrementAndGet();
            if (inFlight == 0 && lastBulkScheduledToExecute && computeFinalResult.compareAndSet(false, true)) {
                // all bulks are complete, close iterator and complete executionFuture
                batchIterator.close();
                if (t == null) {
                    resultFuture.complete(null);
                } else {
                    resultFuture.completeExceptionally(t);
                }
            } else {
                safeConsumeIterator(batchIterator);
            }
        };
    }

    /**
     * Consumes the iterator only if there isn't another thread already consuming it.
     */
    private void safeConsumeIterator(BatchIterator batchIterator) {
        if (collectingEnabled.compareAndSet(false, true)) {
            unsafeConsumeIterator(batchIterator);
        }
    }

    /**
     * Consumes the iterator without guarding against concurrent access to the iterator.
     *
     * !! THIS SHOULD ONLY BE CALLED BY THE loadNextBatch COMPLETE LISTENER !!
     */
    private void unsafeConsumeIterator(BatchIterator batchIterator) {
        Row row = RowBridging.toRow(batchIterator.rowData());
        try {
            while (true) {
                if (indexInBulk == bulkSize) {
                    if (tryExecuteBulk() == false) {
                        collectingEnabled.set(false);
                        if (isScheduledCollectionRunning.compareAndSet(false, true)) {
                            scheduleConsumeIterator();
                        }
                        return;
                    }
                }

                if (batchIterator.moveNext()) {
                    indexInBulk++;
                    onRowConsumer.accept(row);
                } else {
                    break;
                }
            }

            if (batchIterator.allLoaded()) {
                lastBulkScheduledToExecute = true;
                inFlightExecutions.incrementAndGet();
                executeFunction.apply(true).whenComplete(bulkExecutionCompleteListener);
            } else {
                batchIterator.loadNextBatch().whenComplete(loadNextBatchCompleteListener);
            }
        } catch (Throwable t) {
            batchIterator.close();
            resultFuture.completeExceptionally(t);
        }
    }

    private void scheduleConsumeIterator() throws EsRejectedExecutionException {
        scheduler.schedule(scheduleConsumeIteratorJob,
            consumeIteratorDelays.next().getMillis(), TimeUnit.MILLISECONDS);
    }

    private boolean tryExecuteBulk() {
        if (backPressureTrigger.getAsBoolean() == false) {
            indexInBulk = 0;
            inFlightExecutions.incrementAndGet();
            CompletableFuture<R> bulkExecutionFuture = executeFunction.apply(false);
            bulkExecutionFuture.whenComplete(bulkExecutionCompleteListener);
            return true;
        }
        return false;
    }
}
