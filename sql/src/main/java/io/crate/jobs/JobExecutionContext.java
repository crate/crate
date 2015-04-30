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

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class JobExecutionContext {

    private static final ESLogger LOGGER = Loggers.getLogger(JobExecutionContext.class);

    private final UUID jobId;
    private final long keepAlive;
    private final ConcurrentMap<Integer, ExecutionSubContext> subContexts = new ConcurrentHashMap<>();
    private ThreadPool threadPool;

    volatile ContextCallback contextCallback;
    final Object mergeLock = new Object();

    private final AtomicInteger activeSubContexts = new AtomicInteger(0);

    private volatile long lastAccessTime = -1;

    public static class Builder {

        private final UUID jobId;
        private ThreadPool threadPool;
        private final long keepAlive = JobContextService.DEFAULT_KEEP_ALIVE;
        private final IntObjectOpenHashMap<ExecutionSubContext> subContexts = new IntObjectOpenHashMap<>();

        Builder(UUID jobId, ThreadPool threadPool) {
            this.jobId = jobId;
            this.threadPool = threadPool;
        }

        public void addSubContext(int executionNodeId, ExecutionSubContext subContext) {
            ExecutionSubContext collectContext = subContexts.put(executionNodeId, subContext);
            if (collectContext != null) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                        "ExecutionSubContext for %d already added", executionNodeId));
            }
        }

        boolean isEmpty() {
            return subContexts.isEmpty();
        }

        public UUID jobId() {
            return jobId;
        }

        public JobExecutionContext build() {
            return new JobExecutionContext(jobId, keepAlive, threadPool, subContexts);
        }
    }


    private JobExecutionContext(UUID jobId,
                                long keepAlive,
                                ThreadPool threadPool,
                                IntObjectOpenHashMap<ExecutionSubContext> subContexts) {
        this.jobId = jobId;
        this.keepAlive = keepAlive;
        this.threadPool = threadPool;

        for (IntObjectCursor<ExecutionSubContext> cursor : subContexts) {
            addContext(cursor.key, cursor.value);
        }
    }

    void contextCallback(ContextCallback contextCallback) {
        this.contextCallback = contextCallback;
    }

    void merge(JobExecutionContext executionContext) {
        for (Map.Entry<Integer, ExecutionSubContext> entry : executionContext.subContexts.entrySet()) {
            addContext(entry.getKey(), entry.getValue());
        }
    }

    private void addContext(int subContextId, ExecutionSubContext subContext) {
        int numActive = activeSubContexts.incrementAndGet();
        ExecutionSubContext existing = subContexts.putIfAbsent(subContextId, subContext);
        if (existing == null) {
            subContext.addCallback(new RemoveContextCallback(subContextId));
            LOGGER.trace("adding subContext {}, now there are {} subContexts", subContextId, numActive);
            return;
        }

        activeSubContexts.decrementAndGet();
        throw new IllegalArgumentException(String.format(Locale.ENGLISH, "subContext %d is already present", subContextId));
    }

    public UUID jobId() {
        return jobId;
    }

    @Nullable
    public <T extends ExecutionSubContext> T getSubContextOrNull(int executionNodeId) {
        lastAccessTime = threadPool.estimatedTimeInMillis();
        //noinspection unchecked
        return (T) subContexts.get(executionNodeId);
    }

    public <T extends ExecutionSubContext> T getSubContext(int executionNodeId) {
        lastAccessTime = threadPool.estimatedTimeInMillis();
        ExecutionSubContext subContext = subContexts.get(executionNodeId);
        if (subContext == null) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "ExecutionSubContext for %s/%d doesn't exist", jobId(), executionNodeId));
        }
        //noinspection unchecked
        return (T)subContext;
    }

    public long lastAccessTime() {
        return this.lastAccessTime;
    }

    public long keepAlive() {
        return this.keepAlive;
    }

    public void close() {
        LOGGER.trace("close called on JobExecutionContext {}", jobId);
        if (activeSubContexts.get() == 0) {
            callContextCallback();
        } else {
            for (ExecutionSubContext executionSubContext : subContexts.values()) {
                executionSubContext.close();
            }
        }
    }

    private void callContextCallback() {
        if (contextCallback == null) {
            return;
        }
        synchronized (mergeLock) {
            if (contextCallback != null && activeSubContexts.get() == 0) {
                contextCallback.onClose();
            }
        }
    }

    private class RemoveContextCallback implements ContextCallback {

        private final int executionNodeId;

        public RemoveContextCallback(int executionNodeId) {
            this.executionNodeId = executionNodeId;
        }

        @Override
        public void onClose() {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("[{}] Closing subContext {}",
                        System.identityHashCode(subContexts), executionNodeId);
            }

            Object remove = subContexts.remove(executionNodeId);
            int remaining;
            if (remove == null) {
                LOGGER.error("Closed context {} which was already closed.", executionNodeId);
                remaining = activeSubContexts.get();
            } else {
                remaining = activeSubContexts.decrementAndGet();
            }
            if (remaining == 0) {
                callContextCallback();
            } else {
                lastAccessTime = threadPool.estimatedTimeInMillis();
            }
        }
    }
}
