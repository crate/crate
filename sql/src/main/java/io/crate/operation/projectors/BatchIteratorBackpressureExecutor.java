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
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Consumes a BatchIterator in bulks based on the configured {@link #bulkSize}. When a bulk is complete, it checks
 * whether executing the bulk is possible (based on the provided {@link #pauseConsumption}) in which case it runs
 * the {@link #executeFunction}. Otherwise, it stops consuming the iterator and schedules retrying the execution of the
 * bulk for later.
 * Iterator consumption is resumed once the parked/scheduled bulk is executed successfully.
 *
 * @param <R> the type of the result the {@link #executeFunction} future returns.
 */
public class BatchIteratorBackpressureExecutor<R> {

    private final BatchIterator<Row> batchIterator;
    private final Supplier<CompletableFuture<R>> executeFunction;
    private final Consumer<Row> onRowConsumer;
    private final ScheduledExecutorService scheduler;
    private final Iterator<TimeValue> throttleDelay;
    private final BooleanSupplier pauseConsumption;
    private final BiConsumer<Object, Throwable> continueConsumptionOrFinish;
    private final int bulkSize;
    private final AtomicInteger inFlightExecutions = new AtomicInteger(0);
    private final CompletableFuture<Void> resultFuture = new CompletableFuture<>();
    private final Semaphore semaphore = new Semaphore(1);

    private int indexInBulk = 0;
    private volatile boolean consumptionFinished = false;

    public BatchIteratorBackpressureExecutor(BatchIterator<Row> batchIterator,
                                             ScheduledExecutorService scheduler,
                                             Consumer<Row> onRowConsumer,
                                             Supplier<CompletableFuture<R>> executeFunction,
                                             BooleanSupplier pauseConsumption,
                                             int bulkSize,
                                             BackoffPolicy backoffPolicy) {
        this.batchIterator = batchIterator;
        this.scheduler = scheduler;
        this.onRowConsumer = onRowConsumer;
        this.executeFunction = executeFunction;
        this.pauseConsumption = pauseConsumption;
        this.bulkSize = bulkSize;
        this.throttleDelay = backoffPolicy.iterator();
        this.continueConsumptionOrFinish = this::continueConsumptionOrFinish;
    }

    public CompletableFuture<Void> consumeIteratorAndExecute() {
        consumeIterator();
        return resultFuture;
    }

    private void continueConsumptionOrFinish(Object ignored, Throwable failure) {
        int inFlight = inFlightExecutions.decrementAndGet();
        assert inFlight >= 0 : "Number of in-flight executions must not be negative";

        if (consumptionFinished) {
            if (inFlight == 0) {
                setResult(failure);
            }
            // else: waiting for other async-operations to finish
        } else {
            consumeIterator();
        }
    }

    private void setResult(Throwable failure) {
        batchIterator.close();
        if (failure == null) {
            resultFuture.complete(null);
        } else {
            resultFuture.completeExceptionally(failure);
        }
    }

    /**
     * Consumes the rows from the BatchIterator and invokes {@link #executeFunction} every {@link #bulkSize} rows.
     * This loop continues until either:
     *
     *  - The BatchIterator has been fully consumed.
     *  - {@link #pauseConsumption} returns true; In this case a scheduler is used to re-schedule the consumption
     *    after a throttle-delay
     *
     * Each (async) operation which is executed *could* either set the result on {@link #resultFuture} or re-resume
     * consumption.
     * To make sure only a single consumer thread is active at a time a {@link #semaphore} is used.
     */
    private void consumeIterator() {
        if (semaphore.tryAcquire() == false) {
            return;
        }
        try {
            while (batchIterator.moveNext()) {
                indexInBulk++;
                onRowConsumer.accept(batchIterator.currentElement());

                if (indexInBulk == bulkSize) {
                    if (pauseConsumption.getAsBoolean()) {
                        // release semaphore inside resumeConsumption: after throttle delay has passed
                        // to make sure callbacks of previously triggered async operations don't resume consumption
                        scheduler.schedule(this::resumeConsumption, throttleDelay.next().getMillis(), TimeUnit.MILLISECONDS);
                        return;
                    }
                    executeBatch();
                }
            }

            inFlightExecutions.incrementAndGet();
            if (batchIterator.allLoaded()) {
                semaphore.release();
                consumptionFinished = true;
                executeFunction.get().whenComplete(continueConsumptionOrFinish);
            } else {
                batchIterator.loadNextBatch()
                     // consumption can only be continued after loadNextBatch completes; so keep permit until then.
                    .whenComplete((r, f) -> {
                        semaphore.release();
                        continueConsumptionOrFinish.accept(r, f);
                    });
            }
        } catch (Throwable t) {
            // semaphore may be unreleased, but we're finished anyway.
            batchIterator.close();
            resultFuture.completeExceptionally(t);
        }
    }

    private void executeBatch() {
        indexInBulk = 0;
        inFlightExecutions.incrementAndGet();
        executeFunction.get().whenComplete(continueConsumptionOrFinish);
    }

    private void resumeConsumption() {
        if (pauseConsumption.getAsBoolean()) {
            scheduler.schedule(this::resumeConsumption, throttleDelay.next().getMillis(), TimeUnit.MILLISECONDS);
            return;
        }
        // Suspend happened once a batch was ready, so execute it now.
        // consumeIterator would otherwise move past the indexInBulk == bulkSize check and end up building a huge batch
        executeBatch();
        semaphore.release();

        // In case executeBatch takes some time to finish - also immediately resume consumption.
        // The semaphore makes sure there's only 1 active consumer thread
        consumeIterator();
    }
}
