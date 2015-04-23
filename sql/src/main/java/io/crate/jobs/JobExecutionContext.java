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

import javax.annotation.Nullable;
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
    private final ContextCallback contextCloseCallback;

    private final AtomicInteger activeSubContexts = new AtomicInteger(0);

    private volatile long lastAccessTime = -1;

    public static class Builder {

        private final UUID jobId;
        private final long keepAlive = JobContextService.DEFAULT_KEEP_ALIVE;
        private final IntObjectOpenHashMap<JobCollectContext> collectContextMap = new IntObjectOpenHashMap<>();
        private final IntObjectOpenHashMap<PageDownstreamContext> pageDownstreamContextMap = new IntObjectOpenHashMap<>();

        public Builder(UUID jobId) {
            this.jobId = jobId;
        }

        public void addCollectContext(int executionNodeId, JobCollectContext jobCollectContext) {
            collectContextMap.put(executionNodeId, jobCollectContext);
        }

        public void addPageDownstreamContext(int executionNodeId, PageDownstreamContext pageDownstreamContext) {
            pageDownstreamContextMap.put(executionNodeId, pageDownstreamContext);
        }

        public UUID jobId() {
            return jobId;
        }

        public JobExecutionContext build(ContextCallback contextCallback) {
            return new JobExecutionContext(
                    jobId,
                    keepAlive,
                    collectContextMap,
                    pageDownstreamContextMap,
                    contextCallback
            );
        }
    }

    private JobExecutionContext(UUID jobId,
                                long keepAlive,
                                IntObjectOpenHashMap<JobCollectContext> collectContextMap,
                                IntObjectOpenHashMap<PageDownstreamContext> pageDownstreamContextMap,
                                ContextCallback contextCloseCallback) {
        this.jobId = jobId;
        this.keepAlive = keepAlive;
        this.contextCloseCallback = contextCloseCallback;

        for (IntObjectCursor<JobCollectContext> cursor : collectContextMap) {
            addContext(cursor.key, cursor.value, this.collectContextMap);
        }
        for (IntObjectCursor<PageDownstreamContext> cursor : pageDownstreamContextMap) {
            addContext(cursor.key, cursor.value, this.pageDownstreamContextMap);
        }
    }

    private <T extends ExecutionSubContext> void addContext(int subContextId, T subContext, Map<Integer, T> contextMap) {
        int numActive = activeSubContexts.incrementAndGet();
        contextMap.put(subContextId, subContext);
        subContext.addCallback(new RemoveContextCallback(
                subContextId, contextMap, activeSubContexts, contextCloseCallback));
        LOGGER.trace("added subContext {}, now there are {} subContexts", subContextId, numActive);
    }

    public UUID jobId() {
        return jobId;
    }

    public void start() {
        for (JobCollectContext collectContext : collectContextMap.values()) {
            collectContext.start();
        }
    }

    @Nullable
    public PageDownstreamContext getPageDownstreamContext(final int executionNodeId) {
        return pageDownstreamContextMap.get(executionNodeId);
    }

    @Nullable
    public JobCollectContext getCollectContext(int executionNodeId) {
        return collectContextMap.get(executionNodeId);
    }

    public void accessed(long accessTime) {
        this.lastAccessTime = accessTime;
    }

    public long lastAccessTime() {
        return this.lastAccessTime;
    }

    public long keepAlive() {
        return this.keepAlive;
    }

    void close() {
        if (activeSubContexts.get() == 0) {
            contextCloseCallback.onClose();
        } else {
            /*
            for (JobCollectContext jobCollectContext : collectContextMap.values()) {
                jobCollectContext.close();
            }
            */
        }
    }

    private static class RemoveContextCallback implements ContextCallback {

        private final int executionNodeId;
        private final Map<Integer, ?> subContextMap;
        private final AtomicInteger activeSubContexts;
        private final ContextCallback contextCloseCallback;

        public RemoveContextCallback(int executionNodeId,
                                     Map<Integer, ?> subContextMap,
                                     AtomicInteger activeSubContexts,
                                     ContextCallback contextCloseCallback) {
            this.executionNodeId = executionNodeId;
            this.subContextMap = subContextMap;
            this.activeSubContexts = activeSubContexts;
            this.contextCloseCallback = contextCloseCallback;
        }

        @Override
        public void onClose() {
            Object remove = subContextMap.remove(executionNodeId);
            int remaining = activeSubContexts.decrementAndGet();
            if (remove == null) {
                LOGGER.trace("[{}] closed subContext {}, but context was already closed(?). {} subContexts remaining.",
                        System.identityHashCode(subContextMap), executionNodeId, remaining);
            } else {
                LOGGER.trace("[{}] closed subContext {}, {} subContexts remaining.",
                        System.identityHashCode(subContextMap), executionNodeId, remaining);
            }
            if (remaining == 0) {
                LOGGER.trace("[{}] calling contextCloseCallback", System.identityHashCode(subContextMap));
                contextCloseCallback.onClose();
            }
        }
    }
}
