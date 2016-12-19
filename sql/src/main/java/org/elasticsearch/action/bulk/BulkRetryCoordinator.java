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

package org.elasticsearch.action.bulk;

import io.crate.action.LimitedExponentialBackoff;
import io.crate.exceptions.Exceptions;
import io.crate.executor.transport.ShardRequest;
import io.crate.executor.transport.ShardResponse;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Locale;
import java.util.Queue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * coordinates bulk operation retries for one node
 */
public class BulkRetryCoordinator {

    private static final Logger LOGGER = Loggers.getLogger(BulkRetryCoordinator.class);

    private final ReadWriteLock retryLock;
    private static final BackoffPolicy backoff = LimitedExponentialBackoff.limitedExponential(1000);

    private final ThreadPool threadPool;

    private final Object pendingLock = new Object();
    private final Queue<PendingOperation<ShardRequest, ShardResponse>> pendingOperations = new ArrayDeque<>();
    private volatile int activeOperations = 0;

    public BulkRetryCoordinator(ThreadPool threadPool) {
        this.threadPool = threadPool;
        this.retryLock = new ReadWriteLock();
    }

    private void trace(String message, Object... args) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("BulkRetryCoordinator{activeOperations='" + activeOperations + "', " +
                         "pendingOperations='" + pendingOperations.size() + "'} {}",
                String.format(Locale.ENGLISH, message, args));
        }
    }

    public void acquireReadLock() throws InterruptedException {
        /**
         * In order to keep the {@link retryLock} field private we expose only this proxy method.
         * This method is only used by the BulkShardProcessor.
         */
        retryLock.acquireReadLock();
    }

    public void releaseWriteLock() {
        /**
         * In order to keep the {@link retryLock} field private we expose only this proxy method.
         * This method is only used by the BulkShardProcessor.
         */
        retryLock.releaseWriteLock();
    }

    public int numPendingOperations() {
        return pendingOperations.size();
    }

    public void retry(ShardRequest request, BulkRequestExecutor executor, ActionListener<ShardResponse> responseListener) {
        retryOperation(new PendingOperation(request, responseListener, executor));
    }

    private void retryOperation(final PendingOperation<ShardRequest, ShardResponse> operation) {
        trace("retryOperation - activeOperations=%d", activeOperations);
        synchronized (pendingLock) {
            if (activeOperations > 0) {
                pendingOperations.add(operation);
                return;
            }
            activeOperations++;
        }

        try {
            retryLock.acquireWriteLock();
        } catch (InterruptedException e) {
            operation.responseListener.onFailure(e);
            return;
        }

        final ActionListener<ShardResponse> triggeringListener = new PendingTriggeringActionListener(operation);
        operation.executor.execute(operation.request, triggeringListener);
    }

    private void triggerNext() {
        releaseWriteLock();
        PendingOperation<ShardRequest, ShardResponse> pendingOperation = null;
        synchronized (pendingLock) {
            activeOperations--;
            if (activeOperations == 0) {
                pendingOperation = pendingOperations.poll();
            }
        }
        trace("triggerNext - pendingOperation=%s pendingOperations=%d", pendingOperation, pendingOperations.size());

        if (pendingOperation == null) {
            return;
        }
        retryOperation(pendingOperation);
    }

    static class PendingOperation<Request, Response> {

        private final Request request;
        private final ActionListener<Response> responseListener;
        private final BulkRequestExecutor executor;
        private final Iterator<TimeValue> delay = backoff.iterator();


        public PendingOperation(Request request, ActionListener<Response> responseListener, BulkRequestExecutor executor) {
            this.request = request;
            this.responseListener = responseListener;
            this.executor = executor;
        }
    }

    private class PendingTriggeringActionListener implements ActionListener<ShardResponse> {
        private final PendingOperation<ShardRequest, ShardResponse> operation;

        public PendingTriggeringActionListener(PendingOperation<ShardRequest, ShardResponse> operation) {
            this.operation = operation;
        }

        @Override
        public void onResponse(ShardResponse response) {
            triggerNext();
            operation.responseListener.onResponse(response);
        }

        @Override
        public void onFailure(Exception e) {
            e = (Exception) Exceptions.unwrap(e);
            if (e instanceof EsRejectedExecutionException && operation.delay.hasNext()) {
                threadPool.schedule(operation.delay.next(), ThreadPool.Names.SAME, new Runnable() {
                    @Override
                    public void run() {
                        operation.executor.execute(operation.request, PendingTriggeringActionListener.this);
                    }
                });
            } else {
                triggerNext();
                operation.responseListener.onFailure(e);
            }
        }
    }

    /**
     * A {@link Semaphore} based read/write lock allowing multiple readers,
     * no reader will block others, and only 1 active writer. Writers take
     * precedence over readers, a writer will block all readers.
     * Compared to a {@link ReadWriteLock}, no lock is owned by a thread.
     */
    static private class ReadWriteLock {
        private final Semaphore readLock = new Semaphore(1, true);
        private final Semaphore writeLock = new Semaphore(1, true);
        private final AtomicInteger activeWriters = new AtomicInteger(0);
        private final AtomicInteger waitingReaders = new AtomicInteger(0);

        public ReadWriteLock() {
        }

        public void acquireWriteLock() throws InterruptedException {
            // check readLock permits to prevent deadlocks
            if (activeWriters.getAndIncrement() == 0 && readLock.availablePermits() == 1) {
                // draining read permits, so all reads will block
                readLock.drainPermits();
            }
            writeLock.acquire();
        }

        public void releaseWriteLock() {
            if (activeWriters.decrementAndGet() == 0) {
                // unlock all readers
                readLock.release(waitingReaders.getAndSet(0) + 1);
            }
            writeLock.release();
        }

        public void acquireReadLock() throws InterruptedException {
            // only acquire permit if writers are active
            if (activeWriters.get() > 0) {
                waitingReaders.getAndIncrement();
                readLock.acquire();
            }
        }

    }
}
