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

package io.crate.jobs;

import io.crate.action.job.KeepAliveRequest;
import io.crate.action.job.TransportKeepAliveAction;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse;

import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Singleton
public class KeepAliveTimers {

    private static final ESLogger LOGGER = Loggers.getLogger(KeepAliveTimers.class);

    private final ThreadPool threadPool;
    private final TimeValue maxKeepAliveTime;
    private final TransportKeepAliveAction transportKeepAliveAction;

    @Inject
    public KeepAliveTimers(ThreadPool threadPool,
                           TransportKeepAliveAction transportKeepAliveAction) {
        this.threadPool = threadPool;
        this.maxKeepAliveTime = TimeValue.timeValueMillis(JobContextService.KEEP_ALIVE);
        this.transportKeepAliveAction = transportKeepAliveAction;
    }

    public ResettableTimer forJobOnNode(final UUID jobId, final String nodeId) {
        return forRunnable(new Runnable() {
            @Override
            public void run() {
                transportKeepAliveAction.keepAlive(nodeId, new KeepAliveRequest(jobId), new ActionListener<TransportResponse.Empty>() {
                    @Override
                    public void onResponse(TransportResponse.Empty empty) {
                        LOGGER.trace("keep alive for job [{}] on downstream [{}] finished successfully", jobId, nodeId);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        LOGGER.trace("keep alive for job [{}] on downstream [{}] failed", e,jobId, nodeId);
                    }
                });
            }
        });
    }

    public ResettableTimer forRunnable(Runnable runnable) {
        TimeValue delay = TimeValue.timeValueMillis(Math.max(1, maxKeepAliveTime.millis() / 3));
        return forRunnable(runnable, delay);
    }

    public ResettableTimer forRunnable(Runnable runnable, TimeValue delay) {
        return new ResettableTimer(threadPool, runnable, delay);
    }


    /**
     * A timer that executes a given runnable periodically with a given delay if
     * the delay duration was exceeded since the last reset.
     *
     * Execution example:
     *
     * ResettableTimer timer = new ResettableTimer(threadPool, new Runnable { System.out.println("hello world"); }, TimeValue.timeValueMillis(100));
     * timer.start();
     *
     * t+100 -> no reset yet -> "hello world"
     *
     * t+150 -> another thread calls reset()
     *
     * t+200 -> only 50 ms since last reset
     *
     * t+300 -> 150 ms since last reset -> "hello world"
     */
    public static class ResettableTimer {
        private final Runnable runnable;
        private final TimeValue delay;
        private final AtomicReference<ScheduledFuture<?>> futureHolder = new AtomicReference<>();
        private final AtomicLong lastReset = new AtomicLong(-1L);
        private final ThreadPool threadPool;

        public ResettableTimer(ThreadPool threadPool, Runnable runnable, TimeValue delay) {
            this.threadPool = threadPool;
            this.runnable = runnable;
            this.delay = delay;
        }

        public void start() {
            LOGGER.trace("starting ResettableTimer with delay of {}", this.delay);
            final long started = threadPool.estimatedTimeInMillis();
            lastReset.set(started);
            futureHolder.set(threadPool.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    long notAccessed = threadPool.estimatedTimeInMillis() - lastReset.get();
                    if (notAccessed >= delay.millis()){
                        // execute action only when delay expired since last reset
                        LOGGER.trace("not accessed in {}ms, running keep alive runnable", notAccessed);
                        runnable.run();
                    } else {
                        LOGGER.trace("not accessed in {}ms. will only execute keep alive runnable after {}ms", notAccessed, delay.millis());
                    }

                }
            }, delay));
        }

        public void reset() {
            lastReset.set(threadPool.estimatedTimeInMillis());
        }

        public void cancel() {
            ScheduledFuture<?> future = futureHolder.getAndSet(null);
            if (future != null) {
                future.cancel(true);
            }
        }
    }
}
