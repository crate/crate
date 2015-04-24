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
import io.crate.operation.collect.JobCollectContext;
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
    private final ConcurrentMap<Integer, JobCollectContext> collectContextMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, PageDownstreamContext> pageDownstreamContextMap = new ConcurrentHashMap<>();
    private ThreadPool threadPool;

    volatile ContextCallback contextCallback;
    final Object mergeLock = new Object();

    private final AtomicInteger activeSubContexts = new AtomicInteger(0);

    private volatile long lastAccessTime = -1;

    public static class Builder {

        private final UUID jobId;
        private ThreadPool threadPool;
        private final long keepAlive = JobContextService.DEFAULT_KEEP_ALIVE;
        private final IntObjectOpenHashMap<JobCollectContext> collectContextMap = new IntObjectOpenHashMap<>();
        private final IntObjectOpenHashMap<PageDownstreamContext> pageDownstreamContextMap = new IntObjectOpenHashMap<>();

        Builder(UUID jobId, ThreadPool threadPool) {
            this.jobId = jobId;
            this.threadPool = threadPool;
        }

        public void addCollectContext(int executionNodeId, JobCollectContext jobCollectContext) {
            JobCollectContext collectContext = collectContextMap.put(executionNodeId, jobCollectContext);
            if (collectContext != null) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                        "JobCollectContext for %d already added", executionNodeId));
            }
        }

        public void addPageDownstreamContext(int executionNodeId, PageDownstreamContext pageDownstreamContext) {
            PageDownstreamContext downstreamContext = pageDownstreamContextMap.put(executionNodeId, pageDownstreamContext);
            if (downstreamContext != null) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                        "PageDownstreamContext for %d already added", executionNodeId));
            }
        }

        public UUID jobId() {
            return jobId;
        }

        public JobExecutionContext build() {
            return new JobExecutionContext(
                    jobId,
                    keepAlive,
                    threadPool,
                    collectContextMap,
                    pageDownstreamContextMap
            );
        }
    }

    private JobExecutionContext(UUID jobId,
                                long keepAlive,
                                ThreadPool threadPool,
                                IntObjectOpenHashMap<JobCollectContext> collectContextMap,
                                IntObjectOpenHashMap<PageDownstreamContext> pageDownstreamContextMap) {
        this.jobId = jobId;
        this.keepAlive = keepAlive;
        this.threadPool = threadPool;

        for (IntObjectCursor<JobCollectContext> cursor : collectContextMap) {
            addContext(cursor.key, cursor.value, this.collectContextMap);
        }
        for (IntObjectCursor<PageDownstreamContext> cursor : pageDownstreamContextMap) {
            addContext(cursor.key, cursor.value, this.pageDownstreamContextMap);
        }
    }

    void contextCallback(ContextCallback contextCallback) {
        this.contextCallback = contextCallback;
    }

    void merge(JobExecutionContext executionContext) {
        for (Map.Entry<Integer, JobCollectContext> entry : executionContext.collectContextMap.entrySet()) {
            addContext(entry.getKey(), entry.getValue(), collectContextMap);
        }
        for (Map.Entry<Integer, PageDownstreamContext> entry : executionContext.pageDownstreamContextMap.entrySet()) {
            addContext(entry.getKey(), entry.getValue(), pageDownstreamContextMap);
        }
    }

    private <T extends ExecutionSubContext> void addContext(int subContextId, T subContext, ConcurrentMap<Integer, T> contextMap) {
        int numActive = activeSubContexts.incrementAndGet();
        T existing = contextMap.putIfAbsent(subContextId, subContext);
        if (existing == null) {
            subContext.addCallback(new RemoveContextCallback(subContextId, contextMap));
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
    public PageDownstreamContext getPageDownstreamContext(final int executionNodeId) {
        lastAccessTime = threadPool.estimatedTimeInMillis();
        return pageDownstreamContextMap.get(executionNodeId);
    }

    public JobCollectContext getCollectContext(int executionNodeId) {
        lastAccessTime = threadPool.estimatedTimeInMillis();
        JobCollectContext jobCollectContext = collectContextMap.get(executionNodeId);
        if (jobCollectContext == null) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "JobCollectContext for %s/%d doesn't exist", jobId(), executionNodeId));
        }
        return jobCollectContext;
    }

    @Nullable
    public JobCollectContext getCollectContextOrNull(int executionNodeId) {
        lastAccessTime = threadPool.estimatedTimeInMillis();
        return collectContextMap.get(executionNodeId);
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
            for (JobCollectContext jobCollectContext : collectContextMap.values()) {
                jobCollectContext.close();
            }
            for (PageDownstreamContext pageDownstreamContext : pageDownstreamContextMap.values()) {
                pageDownstreamContext.finish();
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
        private final Map<Integer, ?> subContextMap;

        public RemoveContextCallback(int executionNodeId,
                                     Map<Integer, ?> subContextMap) {
            this.executionNodeId = executionNodeId;
            this.subContextMap = subContextMap;
        }

        @Override
        public void onClose() {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("[{}] Closing subContext {}",
                        System.identityHashCode(subContextMap), executionNodeId);
            }

            Object remove = subContextMap.remove(executionNodeId);
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
