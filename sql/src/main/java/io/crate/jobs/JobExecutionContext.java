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

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.cursors.IntCursor;
import io.crate.concurrent.CompletionListenable;
import io.crate.exceptions.ContextMissingException;
import io.crate.exceptions.Exceptions;
import io.crate.operation.collect.stats.JobsLogs;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class JobExecutionContext implements CompletionListenable {

    private static final Logger LOGGER = Loggers.getLogger(JobExecutionContext.class);
    public static final Function<? super JobExecutionContext, UUID> TO_ID = new Function<JobExecutionContext, UUID>() {
        @Nullable
        @Override
        public UUID apply(@Nullable JobExecutionContext input) {
            return input == null ? null : input.jobId();
        }
    };

    private final UUID jobId;
    private final ConcurrentMap<Integer, ExecutionSubContext> subContexts;
    private final AtomicInteger numSubContexts;
    private final IntArrayList orderedContextIds;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final String coordinatorNodeId;
    private final JobsLogs jobsLogs;
    private final CompletableFuture<Void> finishedFuture = new CompletableFuture();
    private final AtomicBoolean killSubContextsOngoing = new AtomicBoolean(false);
    private final Collection<String> participatedNodes;
    private volatile Throwable failure;


    public static class Builder {

        private final UUID jobId;
        private final String coordinatorNode;
        private final JobsLogs jobsLogs;
        private final List<ExecutionSubContext> subContexts = new ArrayList<>();
        private final Collection<String> participatingNodes;

        Builder(UUID jobId, String coordinatorNode, Collection<String> participatingNodes, JobsLogs jobsLogs) {
            this.jobId = jobId;
            this.coordinatorNode = coordinatorNode;
            this.participatingNodes = participatingNodes;
            this.jobsLogs = jobsLogs;
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
            return new JobExecutionContext(jobId, coordinatorNode, participatingNodes, jobsLogs, subContexts);
        }
    }


    private JobExecutionContext(UUID jobId,
                                String coordinatorNodeId,
                                Collection<String> participatingNodes,
                                JobsLogs jobsLogs,
                                List<ExecutionSubContext> orderedContexts) throws Exception {
        this.coordinatorNodeId = coordinatorNodeId;
        this.participatedNodes = participatingNodes;
        orderedContextIds = new IntArrayList(orderedContexts.size());
        this.jobId = jobId;
        this.jobsLogs = jobsLogs;

        subContexts = new ConcurrentHashMap<>(orderedContexts.size());
        numSubContexts = new AtomicInteger(orderedContexts.size());

        boolean traceEnabled = LOGGER.isTraceEnabled();
        for (ExecutionSubContext context : orderedContexts) {
            int subContextId = context.id();
            orderedContextIds.add(subContextId);

            context.completionFuture().whenComplete(new RemoveSubContextListener(subContextId));

            ExecutionSubContext existingContext = subContexts.put(subContextId, context);
            if (existingContext != null) {
                throw new IllegalArgumentException("ExecutionSubContext for " + subContextId + " already added");
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
                    jobsLogs.operationFinished(id, jobId, "Prepare: " + Exceptions.messageOf(e), -1);
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

    private void finish() {
        if (failure != null) {
            finishedFuture.completeExceptionally(failure);
        } else {
            finishedFuture.complete(null);
        }
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

        private RemoveSubContextPosition remove() {
            ExecutionSubContext removed = subContexts.remove(id);
            assert removed != null : "removed must not be null";
            if (numSubContexts.decrementAndGet() == 0) {
                finish();
                return RemoveSubContextPosition.LAST;
            }
            return RemoveSubContextPosition.UNKNOWN;
        }

        public void onSuccess(@Nullable CompletionState state) {
            assert state != null : "state must not be null";
            jobsLogs.operationFinished(id, jobId, null, state.bytesUsed());
            remove();
        }

        public void onFailure(@Nonnull Throwable t) {
            failure = t;
            jobsLogs.operationFinished(id, jobId, Exceptions.messageOf(t), -1);
            if (remove() == RemoveSubContextPosition.LAST) {
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
            if (throwable == null) {
                onSuccess(completionState);
            } else {
                onFailure(throwable);
            }
        }
    }

    private enum RemoveSubContextPosition {
        UNKNOWN,
        LAST
    }
}
