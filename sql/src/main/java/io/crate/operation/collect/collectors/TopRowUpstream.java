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
import io.crate.jobs.ExecutionState;
import io.crate.operation.RowUpstream;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import javax.annotation.Nullable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class TopRowUpstream implements RowUpstream, ExecutionState {

    private final Executor executor;
    private final Runnable resumeRunnable;
    private final Runnable repeatRunnable;
    private final AtomicBoolean paused = new AtomicBoolean(false);

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

    public boolean shouldPause() {
        return pendingPause;
    }

    public void pauseProcessed() {
        // double check pendingPause.. maybe resume was called before pause could be processed
        if (pendingPause) {
            paused.set(true);
            pendingPause = false;
        }
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
        pendingPause = false;
        if (paused.compareAndSet(true, false)) {
            if (async) {
                try {
                    executor.execute(resumeRunnable);
                } catch (RejectedExecutionException | EsRejectedExecutionException e) {
                    resumeRunnable.run();
                }
            } else {
                resumeRunnable.run();
            }
        }
    }

    @Override
    public void repeat() {
        repeatRunnable.run();
    }
}
