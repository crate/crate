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

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.crate.concurrent.CompletableFutures;
import io.crate.concurrent.CompletionListenable;
import io.crate.exceptions.ContextMissingException;
import io.crate.exceptions.SQLExceptions;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.profile.ProfilingContext;
import io.crate.profile.TimeMeasurable;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.query.QueryProfiler;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public class JobExecutionContext implements CompletionListenable {

    private static final Logger LOGGER = Loggers.getLogger(JobExecutionContext.class);

    private final UUID jobId;
    private final ConcurrentMap<Integer, ExecutionSubContext> subContexts;
    private final AtomicInteger numSubContexts;
    private final IntArrayList orderedContextIds;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final String coordinatorNodeId;
    private final JobsLogs jobsLogs;
    private final CompletableFuture<Void> finishedFuture = new CompletableFuture<>();
    private final AtomicBoolean killSubContextsOngoing = new AtomicBoolean(false);
    private final Collection<String> participatedNodes;
    private final ProfilingContext profiler;
    private volatile Throwable failure;

    @Nullable
    private final ConcurrentHashMap<Integer, TimeMeasurable> subContextTimers;
    @Nullable
    private final CompletableFuture<Map<String, Long>> profilingFuture;

    public static class Builder {

        private final UUID jobId;
        private final String coordinatorNode;
        private final JobsLogs jobsLogs;
        private final List<ExecutionSubContext> subContexts = new ArrayList<>();
        private final Collection<String> participatingNodes;

        private ProfilingContext profilingContext;

        Builder(UUID jobId, String coordinatorNode, Collection<String> participatingNodes, JobsLogs jobsLogs) {
            this.jobId = jobId;
            this.coordinatorNode = coordinatorNode;
            this.participatingNodes = participatingNodes;
            this.jobsLogs = jobsLogs;
        }

        public Builder profilingContext(ProfilingContext profilingContext) {
            this.profilingContext = profilingContext;
            return this;
        }

        public void addSubContext(ExecutionSubContext subContext) {
            subContexts.add(subContext);
        }

        boolean isEmpty() {
            return subContexts.isEmpty();
        }

        public UUID jobId() {
            return jobId;
        }

        JobExecutionContext build() throws Exception {
            return new JobExecutionContext(jobId, coordinatorNode, participatingNodes, jobsLogs, subContexts, profilingContext);
        }
    }


    private JobExecutionContext(UUID jobId,
                                String coordinatorNodeId,
                                Collection<String> participatingNodes,
                                JobsLogs jobsLogs,
                                List<ExecutionSubContext> orderedContexts,
                                ProfilingContext profilingContext) throws Exception {
        this.coordinatorNodeId = coordinatorNodeId;
        this.participatedNodes = participatingNodes;
        this.jobId = jobId;
        this.jobsLogs = jobsLogs;

        int numContexts = orderedContexts.size();

        profiler = profilingContext;
        if (profiler.enabled()) {
            subContextTimers = new ConcurrentHashMap<>(numContexts);
            profilingFuture = new CompletableFuture<>();
        } else {
            subContextTimers = null;
            profilingFuture = null;
        }

        orderedContextIds = new IntArrayList(numContexts);
        subContexts = new ConcurrentHashMap<>(numContexts);
        numSubContexts = new AtomicInteger(numContexts);

        boolean traceEnabled = LOGGER.isTraceEnabled();
        for (ExecutionSubContext context : orderedContexts) {
            int subContextId = context.id();
            orderedContextIds.add(subContextId);

            context.completionFuture().whenComplete(new RemoveSubContextListener(subContextId));

            if (subContexts.put(subContextId, context) != null) {
                throw new IllegalArgumentException("ExecutionSubContext for " + subContextId + " already added");
            }
            if (profiler.enabled()) {
                assert subContextTimers != null : "subContextTimers must not be null";
                String subContextName = String.format(Locale.ROOT, "%d-%s", context.id(), context.name());
                if (subContextTimers.put(subContextId, profiler.createMeasurable(subContextName)) != null) {
                    throw new IllegalArgumentException("TimeMeasurable for " + subContextId + " already added");
                }
            }
            if (traceEnabled) {
                LOGGER.trace("adding subContext {}, now there are {} subContexts", subContextId, subContexts.size());
            }
        }
        prepare(orderedContexts);
    }

    public UUID jobId() {
        return jobId;
    }

    String coordinatorNodeId() {
        return coordinatorNodeId;
    }

    Collection<String> participatingNodes() {
        return participatedNodes;
    }

    private void prepare(List<ExecutionSubContext> orderedContexts) throws Exception {
        for (int i = 0; i < orderedContextIds.size(); i++) {
            int id = orderedContextIds.get(i);
            ExecutionSubContext subContext = orderedContexts.get(i);
            jobsLogs.operationStarted(id, jobId, subContext.name());
            try {
                subContext.prepare();
            } catch (Exception e) {
                for (; i >= 0; i--) {
                    id = orderedContextIds.get(i);
                    subContext = orderedContexts.get(i);
                    subContext.cleanup();
                    jobsLogs.operationFinished(id, jobId, "Prepare: " + SQLExceptions.messageOf(e), -1);
                }
                throw e;
            }
        }
    }

    public void start() throws Throwable {
        for (IntCursor id : orderedContextIds) {
            ExecutionSubContext subContext = subContexts.get(id.value);
            if (subContext == null || closed.get()) {
                break; // got killed before start was called
            }
            if (profiler.enabled()) {
                assert subContextTimers != null : "subContextTimers must not be null";
                subContextTimers.get(id.value).start();
            }
            subContext.start();
        }
        if (failure != null) {
            throw failure;
        }
    }

    @Nullable
    public <T extends ExecutionSubContext> T getSubContextOrNull(int executionNodeId) {
        //noinspection unchecked
        return (T) subContexts.get(executionNodeId);
    }

    public <T extends ExecutionSubContext> T getSubContext(int executionNodeId) throws ContextMissingException {
        T subContext = getSubContextOrNull(executionNodeId);
        if (subContext == null) {
            throw new ContextMissingException(ContextMissingException.ContextType.SUB_CONTEXT, jobId, executionNodeId);
        }
        return subContext;
    }

    /**
     * Issues a kill on all active subcontexts. This method returns immediately. The caller should use the future
     * returned by {@link CompletionListenable#completionFuture()} to track EOL of the context.
     *
     * @return the number of contexts on which kill was called
     */
    public long kill() {
        int numKilled = 0;
        if (!closed.getAndSet(true)) {
            LOGGER.trace("kill called on JobExecutionContext {}", jobId);
            if (numSubContexts.get() == 0) {
                finish();
            } else {
                for (ExecutionSubContext executionSubContext : subContexts.values()) {
                    // kill will trigger the ContextCallback onClose too
                    // so it is not necessary to remove the executionSubContext from the map here as it will be done in the callback
                    executionSubContext.kill(null);
                    numKilled++;
                }
            }
        }
        return numKilled;
    }

    private void close() {
        if (failure != null) {
            finishedFuture.completeExceptionally(failure);
        } else {
            finishedFuture.complete(null);
        }
    }

    private void finish() {
        if (profiler.enabled()) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Profiling is enabled. JobExecutionContext will not be closed until results are collected!");
                LOGGER.trace("Profiling results for job {}: {}", jobId, profiler.getAsMap());
            }
            assert profilingFuture != null : "profilingFuture must not be null";
            profilingFuture.complete(executionTimes());
        } else {
            close();
        }
    }

    public CompletableFuture<Map<String, Long>> finishProfiling() {
        if (!profiler.enabled()) {
            // sanity check
            IllegalStateException stateException = new IllegalStateException(
                String.format(Locale.ENGLISH, "Tried to finish profiling job [id=%s], but profiling is not enabled.", jobId));
            return CompletableFutures.failedFuture(stateException);
        }
        assert profilingFuture != null : "profilingFuture must not be null";
        return profilingFuture.whenComplete((o, t) -> close());
    }

    @VisibleForTesting
    Map<String, Long> executionTimes() {
        return ImmutableMap.copyOf(this.profiler.getAsMap());
    }

    Map<String, Map<String, Long>> queryBreakdownTimes() {
        if(profiler.queryProfiler() != null) {
            MapBuilder<String, Map<String, Long>> queryTimesMap = MapBuilder.newMapBuilder();
            List<ProfileResult> profileTree = profiler.queryProfiler().getTree();
            if(profileTree != null) {
                profileTree.forEach((ProfileResult result) -> queryTimesMap.put(result.getLuceneDescription(), result.getTimeBreakdown()));
            }
            return queryTimesMap.immutableMap();
        }
        return Collections.emptyMap();
    }

    @Override
    public CompletableFuture<Void> completionFuture() {
        return finishedFuture;
    }

    @Override
    public String toString() {
        return "JobExecutionContext{" +
               "id=" + jobId +
               ", subContexts=" + subContexts.values() +
               ", closed=" + closed +
               '}';
    }

    private class RemoveSubContextListener implements BiConsumer<CompletionState, Throwable> {

        private final int id;

        private RemoveSubContextListener(int id) {
            this.id = id;
        }

        /**
         * Remove subcontext and finish {@link JobExecutionContext}
         *
         * @return true if removed subcontext was the last subcontext, otherwise false
         */
        private boolean removeAndFinishIfNeeded() {
            ExecutionSubContext removed = subContexts.remove(id);
            assert removed != null : "removed must not be null";
            if (numSubContexts.decrementAndGet() == 0) {
                finish();
                return true;
            }
            return false;
        }

        public void onSuccess(@Nullable CompletionState state) {
            assert state != null : "state must not be null";
            jobsLogs.operationFinished(id, jobId, null, state.bytesUsed());
            removeAndFinishIfNeeded();
        }

        public void onFailure(@Nonnull Throwable t) {
            failure = t;
            jobsLogs.operationFinished(id, jobId, SQLExceptions.messageOf(t), -1);
            if (removeAndFinishIfNeeded()) {
                return;
            }
            if (killSubContextsOngoing.compareAndSet(false, true)) {
                LOGGER.trace("onFailure killing all other subContexts..");
                for (ExecutionSubContext subContext : subContexts.values()) {
                    subContext.kill(t);
                }
            }
        }

        @Override
        public void accept(CompletionState completionState, Throwable throwable) {
            if (profiler.enabled()) {
                stopSubContextTimer();
            }
            if (throwable == null) {
                onSuccess(completionState);
            } else {
                onFailure(throwable);
            }
        }

        private void stopSubContextTimer() {
            assert subContextTimers != null : "subContextTimers must not be null";
            TimeMeasurable removed = subContextTimers.remove(id);
            assert removed != null : "removed must not be null";
            profiler.stopAndAddMeasurable(removed);
        }
    }
}
