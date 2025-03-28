/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.engine.indexing;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;

import org.jetbrains.annotations.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.crate.data.BatchIterator;
import io.crate.exceptions.JobKilledException;

/**
 * Consumes a BatchIterator, concurrently invoking {@link #execute} on
 * each item until {@link #pauseConsumption} returns true.
 *
 * The future returned on {@link #consumeIteratorAndExecute()} completes
 * once all items in the BatchIterator have been processed.
 *
 * If {@link #pauseConsumption} returns true it will pause for a while ({@link #throttleDelay})
 * and afterwards resume consumption.
 */
public class BatchIteratorBackpressureExecutor<T, R> {

    private static final Logger LOGGER = LogManager.getLogger(BatchIteratorBackpressureExecutor.class);

    private final UUID jobId;
    private final Executor executor;
    private final BatchIterator<T> batchIterator;
    private final Function<T, CompletableFuture<R>> execute;
    private final ScheduledExecutorService scheduler;
    private final BinaryOperator<R> combiner;
    private final Predicate<T> pauseConsumption;
    private final BiConsumer<R, Throwable> continueConsumptionOrFinish;
    private final Predicate<R> earlyTerminationCondition;
    private final Function<R, Throwable> resultsToFailure;
    private final AtomicInteger inFlightExecutions = new AtomicInteger(0);
    private final CompletableFuture<R> resultFuture = new CompletableFuture<>();
    private final Semaphore semaphore = new Semaphore(1);

    private final Function<T, Long> getDelayInMs;
    private final AtomicReference<R> resultRef;
    private final AtomicReference<Throwable> failureRef = new AtomicReference<>(null);
    private volatile boolean consumptionFinished = false;


    /**
     * @param batchIterator provides the items for {@code execute}
     * @param execute async function which is called for each item from the batchIterator
     * @param combiner used to combine partial results returned by the {@code execute} function
     * @param identity default value (this is the result, if the batchIterator contains no items)
     * @param pauseConsumption predicate used to check if the consumption should be paused
     */
    public BatchIteratorBackpressureExecutor(UUID jobId,
                                             ScheduledExecutorService scheduler,
                                             Executor executor,
                                             BatchIterator<T> batchIterator,
                                             Function<T, CompletableFuture<R>> execute,
                                             BinaryOperator<R> combiner,
                                             R identity,
                                             Predicate<T> pauseConsumption,
                                             @Nullable Predicate<R> earlyTerminationCondition,
                                             @Nullable Function<R, Throwable> resultsToFailure,
                                             Function<T, Long> getDelayInMs) {

        this.jobId = jobId;
        this.executor = executor;
        this.batchIterator = batchIterator;
        this.scheduler = scheduler;
        this.execute = execute;
        this.combiner = combiner;
        this.pauseConsumption = pauseConsumption;
        this.getDelayInMs = getDelayInMs;
        this.resultRef = new AtomicReference<>(identity);
        this.continueConsumptionOrFinish = this::continueConsumptionOrFinish;
        this.earlyTerminationCondition = earlyTerminationCondition == null ? (results) -> false : earlyTerminationCondition;
        this.resultsToFailure = resultsToFailure == null ? (result) -> JobKilledException.of(null) : resultsToFailure;
    }

    public CompletableFuture<R> consumeIteratorAndExecute() {
        consumeIterator();
        return resultFuture;
    }

    private void continueConsumptionOrFinish(@Nullable R result, Throwable failure) {
        if (result != null) {
            if (earlyTerminationCondition.test(result)) {
                setResult(null, resultsToFailure.apply(result));
                return;
            }
            resultRef.accumulateAndGet(result, combiner);
        }

        if (failure != null) {
            setResult(null, failure);
            return;
        }

        int inFlight = inFlightExecutions.decrementAndGet();
        assert inFlight >= 0 : "Number of in-flight executions must not be negative";

        if (consumptionFinished) {
            if (inFlight == 0) {
                setResult(resultRef.get(), failure == null ? failureRef.get() : failure);
            }
            // else: waiting for other async-operations to finish
        } else {
            consumeIterator();
        }
    }

    private void setResult(R finalResult, Throwable failure) {
        batchIterator.close();
        if (failure == null) {
            resultFuture.complete(finalResult);
        } else {
            resultFuture.completeExceptionally(failure);
        }
    }

    /**
     * Consumes the rows from the BatchIterator and invokes {@link #execute} on each row.
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
                T item = batchIterator.currentElement();
                if (pauseConsumption.test(item)) {
                    long delayInMs = getDelayInMs.apply(item);
                    if (delayInMs > 0) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Pausing consumption jobId={} delayInMs={}", jobId, delayInMs);
                        }
                        // release semaphore inside resumeConsumption: after throttle delay has passed
                        // to make sure callbacks of previously triggered async operations don't resume consumption
                        scheduler.schedule(this::resumeConsumption, delayInMs, TimeUnit.MILLISECONDS);
                        return;
                    }
                    // fall through to execute
                }
                execute(item);
            }

            inFlightExecutions.incrementAndGet();
            if (batchIterator.allLoaded()) {
                semaphore.release();
                consumptionFinished = true;
                continueConsumptionOrFinish(null, null);
            } else {
                batchIterator.loadNextBatch()
                    // consumption can only be continued after loadNextBatch completes; so keep permit until then.
                    .whenComplete((r, f) -> {
                        semaphore.release();
                        continueConsumptionOrFinish(null, f);
                    });
            }
        } catch (Throwable t) {
            // semaphore may be unreleased, but we're finished anyway.
            batchIterator.close();
            resultFuture.completeExceptionally(t);
        }
    }

    private void execute(T item) {
        inFlightExecutions.incrementAndGet();
        execute.apply(item).whenComplete(continueConsumptionOrFinish);
    }

    private void resumeConsumption() {
        T item = batchIterator.currentElement();
        try {
            executor.execute(() -> doResumeConsumption(item));
        } catch (RejectedExecutionException e) {
            doResumeConsumption(item);
        }
    }

    private void doResumeConsumption(T item) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Resuming consumption jobId={}", jobId);
        }
        // Suspend happened once a batch was ready, so execute it now.
        // consumeIterator would otherwise move past the indexInBulk == bulkSize check and end up building a huge batch
        execute(item);
        semaphore.release();

        // In case executeBatch takes some time to finish - also immediately resume consumption.
        // The semaphore makes sure there's only 1 active consumer thread
        consumeIterator();
    }
}
