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

import io.crate.operation.collect.JobCollectContext;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;

import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;

@Singleton
public class JobContextService extends AbstractLifecycleComponent<JobContextService> {

    // TODO: maybe make configurable
    public static long DEFAULT_KEEP_ALIVE = timeValueMinutes(5).millis();
    protected static TimeValue DEFAULT_KEEP_ALIVE_INTERVAL = timeValueMinutes(1);


    private final ThreadPool threadPool;
    private final ScheduledFuture<?> keepAliveReaper;
    private final ConcurrentMap<UUID, JobExecutionContext> activeContexts =
            ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    @Inject
    public JobContextService(Settings settings,
                             ThreadPool threadPool) {
        super(settings);
        this.threadPool = threadPool;
        this.keepAliveReaper = threadPool.scheduleWithFixedDelay(
                new Reaper(),
                DEFAULT_KEEP_ALIVE_INTERVAL);
    }

    @Override
    protected void doStart() throws ElasticsearchException {
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        for (JobExecutionContext context : activeContexts.values()) {
            context.close();
        }
        activeContexts.clear();
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        keepAliveReaper.cancel(false);
    }

    /**
     * @return a {@link JobCollectContext} for given <code>jobId</code>, null if context doesn't exist.
     */
    @Nullable
    public JobExecutionContext getContext(UUID jobId) {
        JobExecutionContext jobExecutionContext = activeContexts.get(jobId);
        if (jobExecutionContext == null) {
            return null;
        }
        contextProcessing(jobExecutionContext);
        return jobExecutionContext;
    }

    /**
     * @return a {@link JobCollectContext} for given <code>jobId</code>, create new one if not found.
     */
    public JobExecutionContext getOrCreateContext(UUID jobId) {
        JobExecutionContext jobExecutionContext = activeContexts.get(jobId);
        if (jobExecutionContext != null) {
            return jobExecutionContext;
        }

        jobExecutionContext = new JobExecutionContext(jobId, DEFAULT_KEEP_ALIVE);
        JobExecutionContext existingContext = activeContexts.putIfAbsent(jobId, jobExecutionContext);
        if (existingContext != null) {
            jobExecutionContext = existingContext;
        }
        contextProcessing(jobExecutionContext);
        return jobExecutionContext;
    }

    /**
     * Release a {@link JobCollectContext}, just settings its last accessed time.
     */
    public void releaseContext(UUID jobId) {
        JobExecutionContext jobExecutionContext = activeContexts.get(jobId);
        if (jobExecutionContext != null) {
            contextProcessedSuccessfully(jobExecutionContext);
        }
    }

    /**
     * Close {@link JobCollectContext} for given <code>jobId</code> and remove if from active map.
     */
    public void closeContext(UUID jobId) {
        JobExecutionContext jobExecutionContext = activeContexts.get(jobId);
        if (jobExecutionContext != null) {
            activeContexts.remove(jobId, jobExecutionContext);
            jobExecutionContext.close();
        }
    }

    protected void contextProcessing(JobExecutionContext context) {
        // disable timeout while executing a job
        context.accessed(-1);
    }

    protected void contextProcessedSuccessfully(JobExecutionContext context) {
        context.accessed(threadPool.estimatedTimeInMillis());
    }

    public void initializeFinalMerge(UUID jobId, int executionNodeId, PageDownstreamContext pageDownstreamContext) {
        JobExecutionContext jobExecutionContext = getOrCreateContext(jobId);
        jobExecutionContext.setPageDownstreamContext(executionNodeId, pageDownstreamContext);
    }

    class Reaper implements Runnable {
        @Override
        public void run() {
            final long time = threadPool.estimatedTimeInMillis();
            for (JobExecutionContext context : activeContexts.values()) {
                // Use the same value for both checks since lastAccessTime can
                // be modified by another thread between checks!
                final long lastAccessTime = context.lastAccessTime();
                if (lastAccessTime == -1l) { // its being processed or timeout is disabled
                    continue;
                }
                if ((time - lastAccessTime > context.keepAlive())) {
                    logger.debug("closing job collect context [{}], time [{}], " +
                                    "lastAccessTime [{}], keepAlive [{}]",
                            context.id(), time, lastAccessTime, context.keepAlive());
                    closeContext(context.id());
                }
            }
        }
    }
}
