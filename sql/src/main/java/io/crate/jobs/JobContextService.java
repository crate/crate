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

package io.crate.jobs;

import io.crate.exceptions.ContextMissingException;
import io.crate.operation.collect.StatsTables;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;

@Singleton
public class JobContextService extends AbstractLifecycleComponent<JobContextService> {

    private static final ESLogger LOGGER = Loggers.getLogger(JobContextService.class);

    // TODO: maybe make configurable
    public static long KEEP_ALIVE = timeValueMinutes(5).millis();
    protected static TimeValue DEFAULT_KEEP_ALIVE_INTERVAL = timeValueMinutes(1);


    private final ThreadPool threadPool;
    private StatsTables statsTables;
    private final ScheduledFuture<?> keepAliveReaper;
    private final ConcurrentMap<UUID, JobExecutionContext> activeContexts =
            ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = rwLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = rwLock.writeLock();

    private final List<KillAllListener> killAllListeners = Collections.synchronizedList(new ArrayList<KillAllListener>());

    @Inject
    public JobContextService(Settings settings,
                             ThreadPool threadPool,
                             StatsTables statsTables) {
        super(settings);
        this.threadPool = threadPool;
        this.statsTables = statsTables;
        this.keepAliveReaper = threadPool.scheduleWithFixedDelay(new Reaper(), DEFAULT_KEEP_ALIVE_INTERVAL);
    }

    @Override
    protected void doStart() throws ElasticsearchException {
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        for (JobExecutionContext context : activeContexts.values()) {
            context.close();
        }
    }

    public void addListener(KillAllListener listener) {
        killAllListeners.add(listener);
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        keepAliveReaper.cancel(false);
    }

    public JobExecutionContext getContext(UUID jobId) {
        JobExecutionContext context = activeContexts.get(jobId);
        if (context == null) {
            throw new ContextMissingException(ContextMissingException.ContextType.JOB_EXECUTION_CONTEXT, jobId);
        }
        return context;
    }

    @Nullable
    public JobExecutionContext getContextOrNull(UUID jobId) {
        return activeContexts.get(jobId);
    }

    public JobExecutionContext.Builder newBuilder(UUID jobId) {
        return new JobExecutionContext.Builder(jobId, threadPool, statsTables);
    }

    public JobExecutionContext createContext(JobExecutionContext.Builder contextBuilder) {
        if (contextBuilder.isEmpty()) {
            throw new IllegalArgumentException("JobExecutionContext.Builder must at least contain 1 SubExecutionContext");
        }
        final UUID jobId = contextBuilder.jobId();
        JobExecutionContext newContext = contextBuilder.build();

        readLock.lock();
        try {
            newContext.contextCallback(new RemoveContextCallback(jobId));
            JobExecutionContext existing = activeContexts.putIfAbsent(jobId, newContext);
            if (existing != null) {
                throw new IllegalArgumentException(
                        String.format(Locale.ENGLISH, "context for job %s already exists:\n%s", jobId, existing));
            }
        } finally {
            readLock.unlock();
        }
        return newContext;
    }

    public long killAll() {
        long numKilled = 0L;
        long now = System.nanoTime();
        writeLock.lock();
        for (KillAllListener killAllListener : killAllListeners) {
            killAllListener.killAllCalled(now);
        }
        try {
            for (JobExecutionContext jobExecutionContext : activeContexts.values()) {
                jobExecutionContext.kill();
                 // don't use  numKilled = activeContext.size() because the content of activeContexts could change
                numKilled++;
            }
            assert activeContexts.size() == 0 :
                    "after killing all contexts, they should have been removed from the map due to the callbacks";
        } finally {
            writeLock.unlock();
        }
        return numKilled;
    }

    public long killJobs(Collection<UUID> toKill) {
        // TODO: when this is exposed some Actions should be stopped like it's done in killAll using the killAllListeners
        long numKilled = 0L;
        writeLock.lock();
        try {
            for (UUID jobId : toKill) {
                JobExecutionContext ctx = activeContexts.get(jobId);
                if (ctx != null) {
                    ctx.kill();
                    numKilled++;
                }
            }
        } finally {
            writeLock.unlock();
        }
        return numKilled;
    }

    private class RemoveContextCallback implements ContextCallback {
        private final UUID jobId;

        public RemoveContextCallback(UUID jobId) {
            this.jobId = jobId;
        }

        @Override
        public void onClose(@Nullable Throwable error, long bytesUsed) {
            activeContexts.remove(jobId);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("[{}]: JobExecutionContext called onClose for job {} removing it -" +
                                " {} executionContexts remaining",
                        System.identityHashCode(activeContexts), jobId, activeContexts.size());
            }
        }
    }

    class Reaper implements Runnable {

        @Override
        public void run() {
            final long time = threadPool.estimatedTimeInMillis();
            for (Map.Entry<UUID, JobExecutionContext> entry : activeContexts.entrySet()) {
                JobExecutionContext context = entry.getValue();
                // Use the same value for both checks since lastAccessTime can
                // be modified by another thread between checks!
                final long lastAccessTime = context.lastAccessTime();
                if (lastAccessTime == -1L) { // its being processed or timeout is disabled
                    continue;
                }
                if ((time - lastAccessTime > KEEP_ALIVE)) {
                    UUID id = entry.getKey();
                    logger.debug("closing job collect context [{}], time [{}], lastAccessTime [{}]",
                            id, time, lastAccessTime);
                    context.close();
                }
            }
        }
    }

}
