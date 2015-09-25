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

package io.crate.operation.collect.collectors;

import com.google.common.base.Throwables;
import io.crate.core.collections.Row;
import io.crate.jobs.ExecutionState;
import io.crate.operation.RowUpstream;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import javax.annotation.Nullable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class TopRowUpstream implements RowUpstream, ExecutionState {

    private final static ESLogger LOGGER = Loggers.getLogger(TopRowUpstream.class);

    private final Executor executor;
    private final Runnable resumeRunnable;
    private final Runnable repeatRunnable;
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final ReentrantLock pauseLock = new ReentrantLock();

    private volatile boolean killed = false;
    private volatile boolean pendingPause = false;
    private volatile Throwable killedThrowable = null;

    public TopRowUpstream(Executor executor,
                          Runnable resumeRunnable,
                          Runnable repeatRunnable)  {
        this.executor = executor;
        this.resumeRunnable = resumeRunnable;
        this.repeatRunnable = repeatRunnable;
    }

    public void kill(@Nullable Throwable throwable) {
        killed = true;
        if (throwable == null) {
            throwable = new CancellationException();
        }
        killedThrowable = throwable;
        if (paused.compareAndSet(true, false)) {
            resumeRunnable.run();;
        }
    }

    public void throwIfKilled() {
        if (killedThrowable != null) {
            throw Throwables.propagate(killedThrowable);
        }
    }


    /**
     * this methods checks if the downstream requested a pause.
     * It must be called after each {@link io.crate.operation.projectors.RowReceiver#setNextRow(Row)} call
     *
     * If it returns true it will also have acquired a lock which can only be released by calling {@link #pauseProcessed()}
     * So anyone who calls shouldPause must call pauseProcessed after it has saved its internal state.
     */
    public boolean shouldPause() {
        if (pendingPause) {
            try {
                pauseLock.lockInterruptibly();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return true;
        }
        return false;
    }

    /**
     * must be called after {@link #shouldPause()} if it returned true in order to indicate that state has been
     * stored (so that the resumeRunnable works) and to release the acquired pauseLock.
     */
    public void pauseProcessed() {
        if (pendingPause) {
            paused.set(true);
            pendingPause = false;
        } else {
            LOGGER.warn("possible pendingPause deadlock");
        }
        pauseLock.unlock();
    }

    @Override
    public boolean isKilled() {
        return killed;
    }

    @Override
    public void pause() {
        pendingPause = true;
    }

    @Override
    public void resume(boolean async) {
        boolean pendingPauseState;
        boolean wasPaused;
        try {
            pauseLock.lockInterruptibly();
            pendingPauseState = pendingPause;
            pendingPause = false;
            wasPaused = paused.compareAndSet(true, false);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error("Interrupted while trying to acquire pauseLock", e);
            return;
        } finally {
            pauseLock.unlock();
        }
        if (wasPaused) {
            if (async) {
                try {
                    executor.execute(resumeRunnable);
                } catch (RejectedExecutionException | EsRejectedExecutionException e) {
                    resumeRunnable.run();
                }
            } else {
                resumeRunnable.run();
            }
        } else {
            LOGGER.debug("Received resume but wasn't paused. PendingPause was {} and has been set to false", pendingPauseState);
        }
    }

    @Override
    public void repeat() {
        repeatRunnable.run();
    }
}
