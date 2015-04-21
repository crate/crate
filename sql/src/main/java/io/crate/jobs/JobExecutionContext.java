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
import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.operation.collect.JobCollectContext;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lease.Releasable;

import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class JobExecutionContext implements Releasable {

    private final UUID jobId;
    private final long keepAlive;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final ConcurrentMap<Integer, JobCollectContext> collectContextMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, PageDownstreamContext> pageDownstreamMap = new ConcurrentHashMap<>();
    private final IntObjectOpenHashMap<SettableFuture<PageDownstreamContext>> pageDownstreamFuturesMap = new IntObjectOpenHashMap<>();

    private volatile long lastAccessTime = -1;


    public JobExecutionContext(UUID jobId, long keepAlive) {
        this.jobId = jobId;
        this.keepAlive = keepAlive;
    }

    public JobCollectContext collectContext(int executionNodeId) {
        if (closed.get()) {
            throw new IllegalStateException("Context already closed");
        }
        JobCollectContext collectContext = collectContextMap.get(executionNodeId);
        if (collectContext != null) {
            return collectContext;
        }
        collectContext = new JobCollectContext(jobId);
        JobCollectContext existingContext = collectContextMap.putIfAbsent(executionNodeId, collectContext);
        return MoreObjects.firstNonNull(existingContext, collectContext);
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

    @Override
    public void close() throws ElasticsearchException {
        if (closed.compareAndSet(false, true)) { // prevent double release
            for (JobCollectContext collectContext : collectContextMap.values()) {
                collectContext.close();
            }
        }
    }

    public UUID id() {
        return jobId;
    }

    public void pageDownstreamContext(int executionNodeId,
                                      PageDownstreamContext pageDownstreamContext) {
        PageDownstreamContext previousEntry = pageDownstreamMap.put(executionNodeId, pageDownstreamContext);
        if (previousEntry != null) {
            throw new IllegalStateException(String.format(Locale.ENGLISH,
                    "there is already a pageDownstream set for %d", executionNodeId));
        }
        synchronized (pageDownstreamFuturesMap) {
            SettableFuture<PageDownstreamContext> future = pageDownstreamFuturesMap.remove(executionNodeId);
            if (future != null) {
                future.set(pageDownstreamContext);
            }
        }
    }

    public ListenableFuture<PageDownstreamContext> pageDownstreamContext(int executionNodeId) {
        PageDownstreamContext pageDownstreamContext = pageDownstreamMap.get(executionNodeId);
        if (pageDownstreamContext == null) {
            SettableFuture<PageDownstreamContext> futureContext = SettableFuture.create();
            synchronized (pageDownstreamFuturesMap) {
                pageDownstreamContext = pageDownstreamMap.get(executionNodeId);
                if (pageDownstreamContext != null) {
                    return Futures.immediateFuture(pageDownstreamContext);
                }
                SettableFuture<PageDownstreamContext> existingFuture =
                        pageDownstreamFuturesMap.getOrDefault(executionNodeId, futureContext);

                if (existingFuture == futureContext) {
                    pageDownstreamFuturesMap.put(executionNodeId, futureContext);
                }
                return existingFuture;
            }
        }
        return Futures.immediateFuture(pageDownstreamContext);
    }
}
