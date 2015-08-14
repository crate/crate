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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

/**
 * coordinates bulk operation retries for one node
 */
public class BulkRetryCoordinator {

    private static final ESLogger LOGGER = Loggers.getLogger(BulkRetryCoordinator.class);
    private static final int DELAY_INCREMENT = 1;

    private final ReadWriteLock retryLock;
    private final AtomicInteger currentDelay;

    private final ScheduledExecutorService retryExecutorService;

    public BulkRetryCoordinator(Settings settings) {
        this.retryExecutorService = Executors.newSingleThreadScheduledExecutor(
                daemonThreadFactory(settings, getClass().getSimpleName()));
        this.retryLock = new ReadWriteLock();
        this.currentDelay = new AtomicInteger(0);
    }

    public ReadWriteLock retryLock() {
        return retryLock;
    }

    public <Request extends BulkProcessorRequest, Response extends BulkProcessorResponse<?>> void retry(
                                                            final Request request,
                                                            final BulkRequestExecutor<Request, Response> executor,
                                                            boolean repeatingRetry,
                                                            ActionListener<Response> listener) {
        trace("doRetry");
        final RetryBulkActionListener<Response> retryBulkActionListener = new RetryBulkActionListener<>(listener);
        if (repeatingRetry) {
            try {
                Thread.sleep(currentDelay.getAndAdd(DELAY_INCREMENT));
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
            executor.execute(request, retryBulkActionListener);
        } else {
            // new retries will be spawned in new thread because they can block
            retryExecutorService.schedule(
                    new Runnable() {
                        @Override
                        public void run() {
                            LOGGER.trace("retry thread [{}] started", Thread.currentThread().getName());
                            // will block if other retries/writer are active
                            try {
                                retryLock.acquireWriteLock();
                            } catch (InterruptedException e) {
                                Thread.interrupted();
                            }
                            LOGGER.trace("retry thread [{}] executing", Thread.currentThread().getName());
                            executor.execute(request, retryBulkActionListener);
                        }
                    }, currentDelay.getAndAdd(DELAY_INCREMENT), TimeUnit.MILLISECONDS);
        }
    }

    private void trace(String message, Object ... args) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("BulkRetryCoordinator: active retries: {} - {}",
                    retryLock.activeWriters(), String.format(Locale.ENGLISH, message, args));
        }
    }

    public void close() {
        if (!retryExecutorService.isTerminated()) {
            retryExecutorService.shutdown();
            try {
                retryExecutorService.awaitTermination(currentDelay.get() + 100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                retryExecutorService.shutdownNow();
            }
        }
    }

    public void shutdown() {
        retryExecutorService.shutdownNow();
    }

    private class RetryBulkActionListener<Response> implements ActionListener<Response> {

        private final ActionListener<Response> listener;

        private RetryBulkActionListener(ActionListener<Response> listener) {
            this.listener = listener;
        }

        @Override
        public void onResponse(Response response) {
            currentDelay.set(0);
            try {
                retryLock.releaseWriteLock();
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
            listener.onResponse(response);
        }

        @Override
        public void onFailure(Throwable e) {
            listener.onFailure(e);
        }
    }


    /**
     * A {@link Semaphore} based read/write lock allowing multiple readers,
     * no reader will block others, and only 1 active writer. Writers take
     * precedence over readers, a writer will block all readers.
     * Compared to a {@link ReadWriteLock}, no lock is owned by a thread.
     */
    static class ReadWriteLock {
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

        public void releaseWriteLock() throws InterruptedException {
            if (activeWriters.decrementAndGet() == 0) {
                // unlock all readers
                readLock.release(waitingReaders.getAndSet(0)+1);
            }
            writeLock.release();
        }

        public void acquireReadLock() throws InterruptedException {
            // only acquire permit if writers are active
            if(activeWriters.get() > 0) {
                waitingReaders.getAndIncrement();
                readLock.acquire();
            }
        }

        public int activeWriters() {
            return activeWriters.get();
        }

    }
}
