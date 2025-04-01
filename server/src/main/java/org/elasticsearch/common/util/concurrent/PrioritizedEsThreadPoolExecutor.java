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

package org.elasticsearch.common.util.concurrent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.elasticsearch.common.Priority;

import io.crate.common.unit.TimeValue;

/**
 * A prioritizing executor which uses a priority queue as a work queue. The jobs that will be submitted will be treated
 * as {@link PrioritizedRunnable}, those tasks that are not instances of these will
 * be wrapped and assign a default {@link Priority#NORMAL} priority.
 * <p>
 * Note, if two tasks have the same priority, the first to arrive will be executed first (FIFO style).
 */
public class PrioritizedEsThreadPoolExecutor extends EsThreadPoolExecutor {

    private static final TimeValue NO_WAIT_TIME_VALUE = TimeValue.timeValueMillis(0);
    private final AtomicLong insertionOrder = new AtomicLong();
    private final Queue<Runnable> current = new ConcurrentLinkedQueue<>();
    private final ScheduledExecutorService timer;

    public PrioritizedEsThreadPoolExecutor(String name,
                                           int corePoolSize,
                                           int maximumPoolSize,
                                           long keepAliveTime,
                                           TimeUnit unit,
                                           ThreadFactory threadFactory,
                                           ScheduledExecutorService timer) {
        super(name, corePoolSize, maximumPoolSize, keepAliveTime, unit, new PriorityBlockingQueue<>(), threadFactory);
        this.timer = timer;
    }

    public List<Pending> getPending() {
        ArrayList<Pending> pending = new ArrayList<>();
        addPending(current, pending, true);
        addPending(getQueue(), pending, false);
        return pending;
    }

    public int getNumberOfPendingTasks() {
        int size = current.size();
        size += getQueue().size();
        return size;
    }

    /**
     * Returns the waiting time of the first task in the queue
     */
    public TimeValue getMaxTaskWaitTime() {
        if (getQueue().size() == 0) {
            return NO_WAIT_TIME_VALUE;
        }

        long now = System.nanoTime();
        long oldestCreationDateInNanos = now;
        for (Runnable queuedRunnable : getQueue()) {
            if (queuedRunnable instanceof PrioritizedRunnable) {
                oldestCreationDateInNanos = Math.min(oldestCreationDateInNanos,
                        ((PrioritizedRunnable) queuedRunnable).getCreationDateInNanos());
            }
        }

        return TimeValue.timeValueNanos(now - oldestCreationDateInNanos);
    }

    private void addPending(Collection<Runnable> runnables, List<Pending> pending, boolean executing) {
        for (Runnable runnable : runnables) {
            if (runnable instanceof TieBreakingPrioritizedRunnable tieBreaking) {
                PrioritizedRunnable innerRunnable = tieBreaking.runnable;
                if (innerRunnable != null) {
                    /** innerRunnable can be null if task is finished but not removed from executor yet,
                     * see {@link TieBreakingPrioritizedRunnable#run} and {@link TieBreakingPrioritizedRunnable#runAndClean}
                     */
                    pending.add(new Pending(innerRunnable, tieBreaking.priority(), tieBreaking.insertionOrder, executing));
                }
            }
        }
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        current.add(r);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        current.remove(r);
    }

    public void execute(Runnable command, final TimeValue timeout, final Runnable timeoutCallback) {
        TieBreakingPrioritizedRunnable tieBreaking = wrapRunnable(command);
        execute(tieBreaking);
        if (timeout.nanos() >= 0) {
            tieBreaking.scheduleTimeout(timer, timeoutCallback, timeout);
        }
    }

    @Override
    protected TieBreakingPrioritizedRunnable wrapRunnable(Runnable command) {
        if (command instanceof TieBreakingPrioritizedRunnable tieBreaking) {
            return tieBreaking;
        }
        if (command instanceof PrioritizedRunnable prioritized) {
            Priority priority = prioritized.priority();
            return new TieBreakingPrioritizedRunnable(
                prioritized,
                priority,
                insertionOrder.incrementAndGet()
            );
        }
        throw new UnsupportedOperationException("Cannot run non-prioritized runnable with " + getClass().getSimpleName());
    }

    public record Pending(PrioritizedRunnable task, Priority priority, long insertionOrder, boolean executing) {
    }

    private final class TieBreakingPrioritizedRunnable extends PrioritizedRunnable implements WrappedRunnable {

        private PrioritizedRunnable runnable;
        private final long insertionOrder;

        // these two variables are protected by 'this'
        private ScheduledFuture<?> timeoutFuture;
        private boolean started = false;

        TieBreakingPrioritizedRunnable(PrioritizedRunnable runnable, Priority priority, long insertionOrder) {
            super(priority);
            this.runnable = runnable;
            this.insertionOrder = insertionOrder;
        }

        @Override
        public void run() {
            synchronized (this) {
                // make the task as stared. This is needed for synchronization with the timeout handling
                // see  #scheduleTimeout()
                started = true;
                FutureUtils.cancel(timeoutFuture);
            }
            runAndClean(runnable);
        }

        @Override
        public int compareTo(PrioritizedRunnable pr) {
            int res = super.compareTo(pr);
            if (res != 0 || !(pr instanceof TieBreakingPrioritizedRunnable)) {
                return res;
            }
            return insertionOrder < ((TieBreakingPrioritizedRunnable) pr).insertionOrder ? -1 : 1;
        }

        public void scheduleTimeout(ScheduledExecutorService timer, final Runnable timeoutCallback, TimeValue timeValue) {
            synchronized (this) {
                if (timeoutFuture != null) {
                    throw new IllegalStateException("scheduleTimeout may only be called once");
                }
                if (started == false) {
                    timeoutFuture = timer.schedule(new Runnable() {
                        @Override
                        public void run() {
                            if (remove(TieBreakingPrioritizedRunnable.this)) {
                                runAndClean(timeoutCallback);
                            }
                        }
                    }, timeValue.nanos(), TimeUnit.NANOSECONDS);
                }
            }
        }

        /**
         * Timeout callback might remain in the timer scheduling queue for some time and it might hold
         * the pointers to other objects. As a result it's possible to run out of memory if a large number of
         * tasks are executed
         */
        private void runAndClean(Runnable run) {
            try {
                run.run();
            } finally {
                runnable = null;
                timeoutFuture = null;
            }
        }

        @Override
        public PrioritizedRunnable unwrap() {
            return runnable;
        }
    }
}
