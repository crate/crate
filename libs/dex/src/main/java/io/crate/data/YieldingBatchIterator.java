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

package io.crate.data;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import io.crate.common.concurrent.RejectableRunnable;

/**
 * A batch iterator that yields every N {@link Duration} of cpuTime on moveNext
 **/
public class YieldingBatchIterator<T> extends MappedForwardingBatchIterator<T, T> {

    private final BatchIterator<T> delegate;
    private final ThreadMXBean threadMXBean;
    private final Executor executor;
    private final Duration duration;
    private long lastCpuTime = -1;
    private boolean yield;

    public static <T> BatchIterator<T> of(BatchIterator<T> delegate, Executor executor, Duration duration) {
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        long currentThreadCpuTime = threadMXBean.getCurrentThreadCpuTime();
        if (currentThreadCpuTime == -1) {
            assert false : "Platform doesn't support monitoring thread cpu time";
            return delegate;
        }
        return new YieldingBatchIterator<>(delegate, executor, threadMXBean, duration);
    }

    private YieldingBatchIterator(BatchIterator<T> delegate,
                                  Executor executor,
                                  ThreadMXBean threadMXBean,
                                  Duration duration) {
        this.delegate = delegate;
        this.executor = executor;
        this.duration = duration;
        this.threadMXBean = ManagementFactory.getThreadMXBean();
    }

    @Override
    public T currentElement() {
        return delegate().currentElement();
    }

    @Override
    public boolean moveNext() {
        if (lastCpuTime == -1) {
            lastCpuTime = threadMXBean.getCurrentThreadCpuTime();
        } else if (yield) {
            yield = false;
            lastCpuTime = threadMXBean.getCurrentThreadCpuTime();
        } else {
            // TODO: only check this every nth row?
            long currentCpuTime = threadMXBean.getCurrentThreadCpuTime();
            if (currentCpuTime - lastCpuTime > duration.toNanos()) {
                // TODO: only yield if threadpool is fully occupied
                yield = true;
                return false;
            }
        }
        return delegate.moveNext();
    }

    @Override
    public CompletionStage<?> loadNextBatch() throws Exception {
        if (yield) {
            Reschedule reschedule = new Reschedule();
            executor.execute(reschedule);
            return reschedule.done;
        } else {
            return delegate.loadNextBatch();
        }
    }

    @Override
    public boolean allLoaded() {
        if (yield) {
            return false;
        }
        return super.allLoaded();
    }

    @Override
    protected BatchIterator<T> delegate() {
        return delegate;
    }

    static class Reschedule implements RejectableRunnable {

        private final CompletableFuture<Boolean> done = new CompletableFuture<>();

        @Override
        public boolean isForceExecution() {
            return true;
        }

        @Override
        public void onFailure(Exception e) {
            done.completeExceptionally(e);
        }

        @Override
        public void doRun() throws Exception {
            done.complete(true);
        }
    }
}
