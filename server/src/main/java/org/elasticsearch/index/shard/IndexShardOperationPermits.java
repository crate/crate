/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.shard;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.Assertions;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.CheckedRunnable;
import io.crate.common.collections.Tuple;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import io.crate.common.io.IOUtils;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Tracks shard operation permits. Each operation on the shard obtains a permit. When we need to block operations (e.g., to transition
 * between terms) we immediately delay all operations to a queue, obtain all available permits, and wait for outstanding operations to drain
 * and return their permits. Delayed operations will acquire permits and be completed after the operation that blocked all operations has
 * completed.
 */
final class IndexShardOperationPermits implements Closeable {

    private final ShardId shardId;
    private final Logger logger;
    private final ThreadPool threadPool;

    static final int TOTAL_PERMITS = Integer.MAX_VALUE;
    final Semaphore semaphore = new Semaphore(TOTAL_PERMITS, true); // fair to ensure a blocking thread is not starved
    private final List<DelayedOperation> delayedOperations = new ArrayList<>(); // operations that are delayed
    private volatile boolean closed;
    private int queuedBlockOperations; // does not need to be volatile as all accesses are done under a lock on this

    // only valid when assertions are enabled. Key is AtomicBoolean associated with each permit to ensure close once semantics.
    // Value is a tuple, with a some debug information supplied by the caller and a stack trace of the acquiring thread
    private final Map<AtomicBoolean, Tuple<String, StackTraceElement[]>> issuedPermits;

    /**
     * Construct operation permits for the specified shards.
     *
     * @param shardId    the shard
     * @param logger     the logger for the shard
     * @param threadPool the thread pool (used to execute delayed operations)
     */
    IndexShardOperationPermits(final ShardId shardId, final Logger logger, final ThreadPool threadPool) {
        this.shardId = shardId;
        this.logger = logger;
        this.threadPool = threadPool;
        if (Assertions.ENABLED) {
            issuedPermits = new ConcurrentHashMap<>();
        } else {
            issuedPermits = Collections.emptyMap();
        }
    }

    @Override
    public void close() {
        closed = true;
    }

    /**
     * Wait for in-flight operations to finish and executes {@code onBlocked} under the guarantee that no new operations are started. Queues
     * operations that are occurring in the meanwhile and runs them once {@code onBlocked} has executed.
     *
     * @param timeout   the maximum time to wait for the in-flight operations block
     * @param timeUnit  the time unit of the {@code timeout} argument
     * @param onBlocked the action to run once the block has been acquired
     * @param <E>       the type of checked exception thrown by {@code onBlocked}
     * @throws InterruptedException      if calling thread is interrupted
     * @throws TimeoutException          if timed out waiting for in-flight operations to finish
     * @throws IndexShardClosedException if operation permit has been closed
     */
    <E extends Exception> void blockOperations(
            final long timeout,
            final TimeUnit timeUnit,
            final CheckedRunnable<E> onBlocked) throws InterruptedException, TimeoutException, E {
        delayOperations();
        try (Releasable ignored = acquireAll(timeout, timeUnit)) {
            onBlocked.run();
        } finally {
            releaseDelayedOperations();
        }
    }

    /**
     * Immediately delays operations and on another thread waits for in-flight operations to finish and then acquires all permits. When all
     * permits are acquired, the provided {@link ActionListener} is called under the guarantee that no new operations are started. Delayed
     * operations are run once the {@link Releasable} is released or if a failure occurs while acquiring all permits; in this case the
     * {@code onFailure} handler will be invoked after delayed operations are released.
     *
     * @param onAcquired {@link ActionListener} that is invoked once acquisition is successful or failed
     * @param timeout    the maximum time to wait for the in-flight operations block
     * @param timeUnit   the time unit of the {@code timeout} argument
     */
    public void asyncBlockOperations(final ActionListener<Releasable> onAcquired, final long timeout, final TimeUnit timeUnit) {
        delayOperations();
        threadPool.executor(ThreadPool.Names.GENERIC).execute(new AbstractRunnable() {

            final AtomicBoolean released = new AtomicBoolean(false);

            @Override
            public void onFailure(final Exception e) {
                try {
                    releaseDelayedOperationsIfNeeded(); // resume delayed operations as soon as possible
                } finally {
                    onAcquired.onFailure(e);
                }
            }

            @Override
            protected void doRun() throws Exception {
                final Releasable releasable = acquireAll(timeout, timeUnit);
                onAcquired.onResponse(() -> {
                    try {
                        releasable.close();
                    } finally {
                        releaseDelayedOperationsIfNeeded();
                    }
                });
            }

            private void releaseDelayedOperationsIfNeeded() {
                if (released.compareAndSet(false, true)) {
                    releaseDelayedOperations();
                }
            }
        });
    }

    private void delayOperations() {
        if (closed) {
            throw new IndexShardClosedException(shardId);
        }
        synchronized (this) {
            assert queuedBlockOperations > 0 || delayedOperations.isEmpty();
            queuedBlockOperations++;
        }
    }

    private Releasable acquireAll(final long timeout, final TimeUnit timeUnit) throws InterruptedException, TimeoutException {
        if (Assertions.ENABLED) {
            // since delayed is not volatile, we have to synchronize even here for visibility
            synchronized (this) {
                assert queuedBlockOperations > 0;
            }
        }
        if (semaphore.tryAcquire(TOTAL_PERMITS, timeout, timeUnit)) {
            final AtomicBoolean closed = new AtomicBoolean();
            return () -> {
                if (closed.compareAndSet(false, true)) {
                    assert semaphore.availablePermits() == 0;
                    semaphore.release(TOTAL_PERMITS);
                }
            };
        } else {
            throw new TimeoutException("timeout while blocking operations");
        }
    }

    private void releaseDelayedOperations() {
        final List<DelayedOperation> queuedActions;
        synchronized (this) {
            assert queuedBlockOperations > 0;
            queuedBlockOperations--;
            if (queuedBlockOperations == 0) {
                queuedActions = new ArrayList<>(delayedOperations);
                delayedOperations.clear();
            } else {
                queuedActions = Collections.emptyList();
            }
        }
        if (!queuedActions.isEmpty()) {
            /*
             * Try acquiring permits on fresh thread (for two reasons):
             *   - blockOperations can be called on a recovery thread which can be expected to be interrupted when recovery is cancelled;
             *     interruptions are bad here as permit acquisition will throw an interrupted exception which will be swallowed by
             *     the threaded action listener if the queue of the thread pool on which it submits is full
             *   - if a permit is acquired and the queue of the thread pool which the threaded action listener uses is full, the
             *     onFailure handler is executed on the calling thread; this should not be the recovery thread as it would delay the
             *     recovery
             */
            threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                for (DelayedOperation queuedAction : queuedActions) {
                    acquire(queuedAction.listener, null, false, queuedAction.debugInfo, queuedAction.stackTrace);
                }
            });
        }
    }

    /**
     * Acquires a permit whenever permit acquisition is not blocked. If the permit is directly available, the provided
     * {@link ActionListener} will be called on the calling thread. During calls of
     * {@link #blockOperations(long, TimeUnit, CheckedRunnable)}, permit acquisition can be delayed.
     * The {@link ActionListener#onResponse(Object)} method will then be called using the provided executor once operations are no
     * longer blocked. Note that the executor will not be used for {@link ActionListener#onFailure(Exception)} calls. Those will run
     * directly on the calling thread, which in case of delays, will be a generic thread. Callers should thus make sure
     * that the {@link ActionListener#onFailure(Exception)} method provided here only contains lightweight operations.
     *
     * @param onAcquired      {@link ActionListener} that is invoked once acquisition is successful or failed
     * @param executorOnDelay executor to use for the possibly delayed {@link ActionListener#onResponse(Object)} call
     * @param forceExecution  whether the runnable should force its execution in case it gets rejected
     * @param debugInfo       an extra information that can be useful when tracing an unreleased permit. When assertions are enabled
     *                        the tracing will capture the supplied object's {@link Object#toString()} value. Otherwise the object
     *                        isn't used
     *
     */
    public void acquire(final ActionListener<Releasable> onAcquired, final String executorOnDelay, final boolean forceExecution,
                        final Object debugInfo) {
        final StackTraceElement[] stackTrace;
        if (Assertions.ENABLED) {
            stackTrace = Thread.currentThread().getStackTrace();
        } else {
            stackTrace = null;
        }
        acquire(onAcquired, executorOnDelay, forceExecution, debugInfo, stackTrace);
    }

    private void acquire(final ActionListener<Releasable> onAcquired, final String executorOnDelay, final boolean forceExecution,
                        final Object debugInfo, final StackTraceElement[] stackTrace) {
        if (closed) {
            onAcquired.onFailure(new IndexShardClosedException(shardId));
            return;
        }
        final Releasable releasable;
        try {
            synchronized (this) {
                if (queuedBlockOperations > 0) {
                    final ActionListener<Releasable> wrappedListener;
                    if (executorOnDelay != null) {
                        wrappedListener = new PermitAwareThreadedActionListener(
                            threadPool,
                            executorOnDelay,
                            onAcquired,
                            forceExecution
                        );
                    } else {
                        wrappedListener = onAcquired;
                    }
                    delayedOperations.add(new DelayedOperation(wrappedListener, debugInfo, stackTrace));
                    return;
                } else {
                    releasable = acquire(debugInfo, stackTrace);
                }
            }
        } catch (final InterruptedException e) {
            onAcquired.onFailure(e);
            return;
        }
        // execute this outside the synchronized block!
        onAcquired.onResponse(releasable);
    }

    private Releasable acquire(Object debugInfo, StackTraceElement[] stackTrace) throws InterruptedException {
        assert Thread.holdsLock(this);
        if (semaphore.tryAcquire(1, 0, TimeUnit.SECONDS)) { // the un-timed tryAcquire methods do not honor the fairness setting
            final AtomicBoolean closed = new AtomicBoolean();
            final Releasable releasable = () -> {
                if (closed.compareAndSet(false, true)) {
                    if (Assertions.ENABLED) {
                        Tuple<String, StackTraceElement[]> existing = issuedPermits.remove(closed);
                        assert existing != null;
                    }
                    semaphore.release(1);
                }
            };
            if (Assertions.ENABLED) {
                issuedPermits.put(closed, new Tuple<>(debugInfo.toString(), stackTrace));
            }
            return releasable;
        } else {
            // this should never happen, if it does something is deeply wrong
            throw new IllegalStateException("failed to obtain permit but operations are not delayed");
        }
    }

    /**
     * Obtain the active operation count, or zero if all permits are held (even if there are outstanding operations in flight).
     *
     * @return the active operation count, or zero when all permits ar eheld
     */
    int getActiveOperationsCount() {
        int availablePermits = semaphore.availablePermits();
        if (availablePermits == 0) {
            /*
             * This occurs when either doBlockOperations is holding all the permits or there are outstanding operations in flight and the
             * remainder of the permits are held by doBlockOperations. We do not distinguish between these two cases and simply say that
             * the active operations count is zero.
             */
            return 0;
        } else {
            return TOTAL_PERMITS - availablePermits;
        }
    }


    synchronized boolean isBlocked() {
        return queuedBlockOperations > 0;
    }

    /**
     * @return a list of describing each permit that wasn't released yet. The description consist of the debugInfo supplied
     *         when the permit was acquired plus a stack traces that was captured when the permit was request.
     */
    List<String> getActiveOperations() {
        return issuedPermits.values().stream().map(
            t -> t.v1() + "\n" + ExceptionsHelper.formatStackTrace(t.v2()))
            .collect(Collectors.toList());
    }

    private static class DelayedOperation {
        private final ActionListener<Releasable> listener;
        private final String debugInfo;
        private final StackTraceElement[] stackTrace;

        private DelayedOperation(ActionListener<Releasable> listener, Object debugInfo, StackTraceElement[] stackTrace) {
            this.listener = listener;
            if (Assertions.ENABLED) {
                this.debugInfo = "[delayed] " + debugInfo;
                this.stackTrace = stackTrace;
            } else {
                this.debugInfo = null;
                this.stackTrace = null;
            }
        }
    }

    /**
     * A permit-aware action listener wrapper that spawns onResponse listener invocations off on a configurable thread-pool.
     * Being permit-aware, it also releases the permit when hitting thread-pool rejections and falls back to the
     * invoker's thread to communicate failures.
     */
    private static class PermitAwareThreadedActionListener implements ActionListener<Releasable> {

        private final ThreadPool threadPool;
        private final String executor;
        private final ActionListener<Releasable> listener;
        private final boolean forceExecution;

        private PermitAwareThreadedActionListener(ThreadPool threadPool, String executor, ActionListener<Releasable> listener,
                                                  boolean forceExecution) {
            this.threadPool = threadPool;
            this.executor = executor;
            this.listener = listener;
            this.forceExecution = forceExecution;
        }

        @Override
        public void onResponse(final Releasable releasable) {
            threadPool.executor(executor).execute(new AbstractRunnable() {
                @Override
                public boolean isForceExecution() {
                    return forceExecution;
                }

                @Override
                protected void doRun() throws Exception {
                    listener.onResponse(releasable);
                }

                @Override
                public void onRejection(Exception e) {
                    IOUtils.closeWhileHandlingException(releasable);
                    super.onRejection(e);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e); // will possibly execute on the caller thread
                }
            });
        }

        @Override
        public void onFailure(final Exception e) {
            listener.onFailure(e);
        }
    }

}
