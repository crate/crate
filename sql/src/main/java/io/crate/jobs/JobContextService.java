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

import io.crate.concurrent.CompletionState;
import io.crate.exceptions.ContextMissingException;
import io.crate.concurrent.CompletionListener;
import io.crate.operation.collect.StatsTables;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Singleton
public class JobContextService extends AbstractLifecycleComponent<JobContextService> {

    private final ClusterService clusterService;
    private final StatsTables statsTables;
    private final ConcurrentMap<UUID, JobExecutionContext> activeContexts =
            ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = rwLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = rwLock.writeLock();

    private final List<KillAllListener> killAllListeners = Collections.synchronizedList(new ArrayList<KillAllListener>());

    @Inject
    public JobContextService(Settings settings, ClusterService clusterService, StatsTables statsTables) {
        super(settings);
        this.clusterService = clusterService;
        this.statsTables = statsTables;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        for (JobExecutionContext context : activeContexts.values()) {
            context.kill();
        }
    }

    public void addListener(KillAllListener listener) {
        killAllListeners.add(listener);
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    public JobExecutionContext getContext(UUID jobId) {
        JobExecutionContext context = activeContexts.get(jobId);
        if (context == null) {
            throw new ContextMissingException(ContextMissingException.ContextType.JOB_EXECUTION_CONTEXT, jobId);
        }
        return context;
    }

    public Collection<JobExecutionContext> getContextsByCoordinatorNode(final String coordinatorNodeId) {
        List<JobExecutionContext> contexts = new ArrayList<>();
        for (JobExecutionContext jobExecutionContext : activeContexts.values()) {
            if (jobExecutionContext.coordinatorNodeId().equals(coordinatorNodeId)) {
                contexts.add(jobExecutionContext);
            }
        }
        return contexts;
    }

    @Nullable
    public JobExecutionContext getContextOrNull(UUID jobId) {
        return activeContexts.get(jobId);
    }

    public JobExecutionContext.Builder newBuilder(UUID jobId) {
        return new JobExecutionContext.Builder(jobId, clusterService.localNode().id(), statsTables);
    }

    public JobExecutionContext.Builder newBuilder(UUID jobId, String coordinatorNodeId) {
        return new JobExecutionContext.Builder(jobId, coordinatorNodeId, statsTables);
    }

    public JobExecutionContext createContext(JobExecutionContext.Builder contextBuilder) {
        if (contextBuilder.isEmpty()) {
            throw new IllegalArgumentException("JobExecutionContext.Builder must at least contain 1 SubExecutionContext");
        }
        final UUID jobId = contextBuilder.jobId();
        JobExecutionContext newContext = contextBuilder.build();
        readLock.lock();
        try {
            newContext.addListener(new JobContextListener(jobId));
            JobExecutionContext existing = activeContexts.putIfAbsent(jobId, newContext);
            if (existing != null) {
                throw new IllegalArgumentException(
                        String.format(Locale.ENGLISH, "context for job %s already exists:%n%s", jobId, existing));
            }
        } finally {
            readLock.unlock();
        }
        if (logger.isTraceEnabled()) {
            logger.trace("JobExecutionContext created for job {},  activeContexts: {}",
                    jobId, activeContexts.size());
        }
        return newContext;
    }

    public long killAll() {
        long numKilled = 0L;
        long now = System.nanoTime();
        writeLock.lock();
        try {
            for (KillAllListener killAllListener : killAllListeners) {
                killAllListener.killAllJobs(now);
            }
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
        long numKilled = 0L;
        writeLock.lock();
        try {
            for (KillAllListener killAllListener : killAllListeners) {
                for (UUID job : toKill) {
                    killAllListener.killJob(job);
                }
            }
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

    private class JobContextListener implements CompletionListener {

        private UUID jobId;

        public JobContextListener(UUID jobId) {
            this.jobId = jobId;
        }

        private void remove(@Nullable Throwable throwable) {
            activeContexts.remove(jobId);
            if (logger.isTraceEnabled()) {
                logger.trace("JobExecutionContext closed for job {} removed it -" +
                             " {} executionContexts remaining",
                    throwable, jobId, activeContexts.size());
            }
        }

        @Override
        public void onSuccess(@Nullable CompletionState state) {
            remove(null);
        }

        @Override
        public void onFailure(@Nonnull Throwable throwable) {
            remove(throwable);
        }
    }

}
