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

import io.crate.executor.transport.ShardUpsertRequest;
import io.crate.executor.transport.ShardUpsertResponse;
import io.crate.executor.transport.SymbolBasedShardUpsertRequest;
import io.crate.executor.transport.TransportActionProvider;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
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
    private final TransportActionProvider transportActionProvider;

    private final ScheduledExecutorService retryExecutorService;

    public BulkRetryCoordinator(Settings settings,
                                TransportActionProvider transportActionProvider) {
        this.retryExecutorService = Executors.newSingleThreadScheduledExecutor(
                daemonThreadFactory(settings, getClass().getSimpleName()));
        this.transportActionProvider = transportActionProvider;
        this.retryLock = new ReadWriteLock();
        this.currentDelay = new AtomicInteger(0);
    }

    public ReadWriteLock retryLock() {
        return retryLock;
    }

    protected void execute(final SymbolBasedShardUpsertRequest updateRequest, final RequestActionListener<SymbolBasedShardUpsertRequest, ShardUpsertResponse> listener) {
        trace("execute symbol based shard request %d", updateRequest.shardId());
        transportActionProvider.symbolBasedTransportShardUpsertActionDelegate().execute(updateRequest, new BulkActionListener<>(listener, updateRequest));
    }

    protected void execute(final ShardUpsertRequest updateRequest, final RequestActionListener<ShardUpsertRequest, ShardUpsertResponse> listener) {
        trace("execute shard request %d", updateRequest.shardId());
        transportActionProvider.transportShardUpsertActionDelegate().execute(updateRequest, new BulkActionListener<>(listener, updateRequest));
    }

    public void retry(final ShardUpsertRequest request,
                      boolean repeatingRetry,
                      final RequestActionListener<ShardUpsertRequest, ShardUpsertResponse> retryListener) {
        trace("doRetry");
        final TransportShardUpsertActionDelegate delegate = transportActionProvider.transportShardUpsertActionDelegate();
        final RetryBulkActionListener<ShardUpsertRequest, ShardUpsertResponse> retryBulkActionListener = new RetryBulkActionListener<>(retryListener, request);
        if (repeatingRetry) {
            try {
                Thread.sleep(currentDelay.getAndAdd(DELAY_INCREMENT));
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
            delegate.execute(request, retryBulkActionListener);
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
                            delegate.execute(request, retryBulkActionListener);
                        }
                    }, currentDelay.getAndAdd(DELAY_INCREMENT), TimeUnit.MILLISECONDS);
        }
    }

    public void retry(final SymbolBasedShardUpsertRequest request,
                      boolean repeatingRetry,
                      final RequestActionListener<SymbolBasedShardUpsertRequest, ShardUpsertResponse> retryListener) {
        trace("doRetry symbol based");
        final SymbolBasedTransportShardUpsertActionDelegate delegate = transportActionProvider.symbolBasedTransportShardUpsertActionDelegate();
        final RetryBulkActionListener<SymbolBasedShardUpsertRequest, ShardUpsertResponse> retryBulkActionListener = new RetryBulkActionListener<>(retryListener, request);
        if (repeatingRetry) {
            try {
                Thread.sleep(currentDelay.incrementAndGet());
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
            delegate.execute(request, retryBulkActionListener);
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
                            delegate.execute(request, retryBulkActionListener);
                        }
                    }, currentDelay.getAndAdd(10), TimeUnit.MILLISECONDS);
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

    /**
     * keeping a reference to the request around
     */
    public static interface RequestActionListener<Request extends ActionRequest, Response extends ActionResponse> {

        public void onResponse(Request request, Response response);

        public void onFailure(Request request, Throwable e);
    }

    /**
     * ActionListener that keeps a reference to the request
     * and forwards its callbacks to a {@linkplain RequestActionListener}
     */
    private static class BulkActionListener<Request extends ActionRequest, Response extends ActionResponse> implements ActionListener<Response> {

        private final RequestActionListener<Request, Response> listener;
        private final Request request;

        private BulkActionListener(RequestActionListener<Request, Response> listener, Request request) {
            this.listener = listener;
            this.request = request;
        }

        @Override
        public void onResponse(Response response) {
            listener.onResponse(request, response);
        }

        @Override
        public void onFailure(Throwable e) {
            listener.onFailure(request, e);
        }
    }

    private class RetryBulkActionListener<Request extends ActionRequest, Response extends ActionResponse> extends BulkActionListener<Request, Response> {

        private RetryBulkActionListener(RequestActionListener<Request, Response> listener, Request request) {
            super(listener, request);
        }

        @Override
        public void onResponse(Response response) {
            currentDelay.set(0);
            try {
                retryLock.releaseWriteLock();
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
            super.onResponse(response);
        }
    }


    /**
     * A {@link Semaphore} based read/write lock allowing multiple readers,
     * no reader will block others, and only 1 active writer. Writers take
     * precedence over readers, a writer will block all readers.
     * Compared to a {@link ReadWriteLock}, no lock is owned by a thread.
     */
    class ReadWriteLock {
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
