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

package io.crate.execution.jobs;

import com.google.common.collect.ImmutableList;
import io.crate.concurrent.CountdownFutureCallback;
import io.crate.exceptions.ContextMissingException;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.execution.jobs.kill.KillAllListener;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

@Singleton
public class JobContextService extends AbstractLifecycleComponent {

    private final ClusterService clusterService;
    private final JobsLogs jobsLogs;
    private final ConcurrentMap<UUID, JobExecutionContext> activeContexts =
        ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    private final List<KillAllListener> killAllListeners = Collections.synchronizedList(new ArrayList<KillAllListener>());

    @Inject
    public JobContextService(Settings settings, ClusterService clusterService, JobsLogs jobsLogs) {
        super(settings);
        this.clusterService = clusterService;
        this.jobsLogs = jobsLogs;
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

    public Stream<UUID> getJobIdsByCoordinatorNode(final String coordinatorNodeId) {
        return activeContexts.values()
            .stream()
            .filter(jobExecutionContext -> jobExecutionContext.coordinatorNodeId().equals(coordinatorNodeId))
            .map(JobExecutionContext::jobId);
    }

    public Stream<UUID> getJobIdsByParticipatingNodes(final String nodeId) {
        return activeContexts.values().stream()
            .filter(i -> i.participatingNodes().contains(nodeId))
            .map(JobExecutionContext::jobId);
    }

    @Nullable
    public JobExecutionContext getContextOrNull(UUID jobId) {
        return activeContexts.get(jobId);
    }

    public JobExecutionContext.Builder newBuilder(UUID jobId) {
        return new JobExecutionContext.Builder(jobId, clusterService.localNode().getId(), Collections.emptyList(), jobsLogs);
    }

    public JobExecutionContext.Builder newBuilder(UUID jobId, String coordinatorNodeId) {
        return new JobExecutionContext.Builder(jobId, coordinatorNodeId, Collections.emptyList(), jobsLogs);
    }

    public JobExecutionContext.Builder newBuilder(UUID jobId, String coordinatorNodeId, Collection<String> participatingNodes) {
        return new JobExecutionContext.Builder(jobId, coordinatorNodeId, participatingNodes, jobsLogs);
    }

    public int numActive() {
        return activeContexts.size();
    }

    public JobExecutionContext createContext(JobExecutionContext.Builder contextBuilder) throws Exception {
        if (contextBuilder.isEmpty()) {
            throw new IllegalArgumentException("JobExecutionContext.Builder must at least contain 1 SubExecutionContext");
        }
        final UUID jobId = contextBuilder.jobId();
        JobExecutionContext newContext = contextBuilder.build();

        JobContextCallback jobContextCallback = new JobContextCallback(jobId);
        newContext.completionFuture().whenComplete(jobContextCallback);

        JobExecutionContext existing = activeContexts.putIfAbsent(jobId, newContext);
        if (existing != null) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "context for job %s already exists:%n%s", jobId, existing));
        }
        if (logger.isTraceEnabled()) {
            logger.trace("JobExecutionContext created for job {},  activeContexts: {}",
                jobId, activeContexts.size());
        }
        return newContext;
    }


    /**
     * kills all contexts which are active at the time of the call of this method.
     *
     * @return a future holding the number of contexts kill was called on, the future is finished when all contexts
     * are completed and never fails.
     */
    public CompletableFuture<Integer> killAll() {
        for (KillAllListener killAllListener : killAllListeners) {
            try {
                killAllListener.killAllJobs();
            } catch (Throwable t) {
                logger.error("Failed to call killAllJobs on listener {}", t, killAllListener);
            }
        }
        Collection<UUID> toKill = ImmutableList.copyOf(activeContexts.keySet());
        if (toKill.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        return killContexts(toKill);
    }

    private CompletableFuture<Integer> killContexts(Collection<UUID> toKill) {
        assert !toKill.isEmpty() : "toKill must not be empty";
        int numKilled = 0;
        CountdownFutureCallback countDownFuture = new CountdownFutureCallback(toKill.size());
        for (UUID jobId : toKill) {
            JobExecutionContext ctx = activeContexts.get(jobId);
            if (ctx != null) {
                ctx.completionFuture().whenComplete(countDownFuture);
                ctx.kill();
                numKilled++;
            } else {
                // no kill but we need to count down
                countDownFuture.onSuccess();
            }
        }
        final int finalNumKilled = numKilled;
        return countDownFuture.handle((r, f) -> finalNumKilled);
    }

    public CompletableFuture<Integer> killJobs(Collection<UUID> toKill) {
        for (KillAllListener killAllListener : killAllListeners) {
            for (UUID job : toKill) {
                try {
                    killAllListener.killJob(job);
                } catch (Throwable t) {
                    logger.error("Failed to call killJob on listener {}", t, killAllListener);
                }
            }
        }
        return killContexts(toKill);
    }

    private class JobContextCallback implements BiConsumer<Void, Throwable> {

        private final UUID jobId;

        JobContextCallback(UUID jobId) {
            this.jobId = jobId;
        }

        @Override
        public void accept(Void aVoid, Throwable throwable) {
            activeContexts.remove(jobId);
            if (logger.isTraceEnabled()) {
                logger.trace("JobExecutionContext closed and removed. jobId={} remainingContexts={} failure={}",
                    jobId, activeContexts.size(), throwable);
            }
        }
    }
}
