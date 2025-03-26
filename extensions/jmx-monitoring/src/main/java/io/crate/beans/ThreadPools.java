/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.beans;

import static org.elasticsearch.threadpool.ThreadPool.Names.FETCH_SHARD_STARTED;
import static org.elasticsearch.threadpool.ThreadPool.Names.FETCH_SHARD_STORE;
import static org.elasticsearch.threadpool.ThreadPool.Names.FLUSH;
import static org.elasticsearch.threadpool.ThreadPool.Names.FORCE_MERGE;
import static org.elasticsearch.threadpool.ThreadPool.Names.GENERIC;
import static org.elasticsearch.threadpool.ThreadPool.Names.LISTENER;
import static org.elasticsearch.threadpool.ThreadPool.Names.LOGICAL_REPLICATION;
import static org.elasticsearch.threadpool.ThreadPool.Names.MANAGEMENT;
import static org.elasticsearch.threadpool.ThreadPool.Names.REFRESH;
import static org.elasticsearch.threadpool.ThreadPool.Names.SEARCH;
import static org.elasticsearch.threadpool.ThreadPool.Names.SNAPSHOT;
import static org.elasticsearch.threadpool.ThreadPool.Names.WRITE;

import java.beans.ConstructorProperties;

import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.jetbrains.annotations.Nullable;

public class ThreadPools implements ThreadPoolsMXBean {

    public static class ThreadPoolInfo {

        private final String name;
        private final int poolSize;
        private final int queueSize;
        private final int largestPoolSize;
        private final int active;
        private final long completed;
        private final long rejected;

        @ConstructorProperties({"name", "poolSize", "queueSize", "largestPoolSize", "active", "completed", "rejected"})
        public ThreadPoolInfo(String name,
                              int poolSize,
                              int queueSize,
                              int largestPoolSize,
                              int active,
                              long completed,
                              long rejected) {
            this.name = name;
            this.poolSize = poolSize;
            this.queueSize = queueSize;
            this.largestPoolSize = largestPoolSize;
            this.active = active;
            this.completed = completed;
            this.rejected = rejected;
        }

        @SuppressWarnings("unused")
        public String getName() {
            return name;
        }

        @SuppressWarnings("unused")
        public int getPoolSize() {
            return poolSize;
        }

        @SuppressWarnings("unused")
        public int getQueueSize() {
            return queueSize;
        }

        @SuppressWarnings("unused")
        public int getLargestPoolSize() {
            return largestPoolSize;
        }

        @SuppressWarnings("unused")
        public int getActive() {
            return active;
        }

        @SuppressWarnings("unused")
        public long getCompleted() {
            return completed;
        }

        @SuppressWarnings("unused")
        public long getRejected() {
            return rejected;
        }
    }

    public static final String NAME = "io.crate.monitoring:type=ThreadPools";

    private final ThreadPool threadPool;

    public ThreadPools(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    @Nullable
    private ThreadPoolInfo getThreadPoolInfo(String name) {
        for (ThreadPoolStats.Stats stats : threadPool.stats()) {
            if (stats.getName().equals(name)) {
                return new ThreadPoolInfo(
                    stats.getName(),
                    stats.getThreads(),
                    stats.getQueue(),
                    stats.getLargest(),
                    stats.getActive(),
                    stats.getCompleted(),
                    stats.getRejected());
            }
        }
        return null;
    }

    @Override
    public ThreadPoolInfo getGeneric() {
        return getThreadPoolInfo(GENERIC);
    }

    @Override
    public ThreadPoolInfo getListener() {
        return getThreadPoolInfo(LISTENER);
    }

    @Override
    public ThreadPoolInfo getWrite() {
        return getThreadPoolInfo(WRITE);
    }

    @Override
    public ThreadPoolInfo getSearch() {
        return getThreadPoolInfo(SEARCH);
    }

    @Override
    public ThreadPoolInfo getManagement() {
        return getThreadPoolInfo(MANAGEMENT);
    }

    @Override
    public ThreadPoolInfo getFlush() {
        return getThreadPoolInfo(FLUSH);
    }

    @Override
    public ThreadPoolInfo getRefresh() {
        return getThreadPoolInfo(REFRESH);
    }

    @Override
    public ThreadPoolInfo getSnapshot() {
        return getThreadPoolInfo(SNAPSHOT);
    }

    @Override
    public ThreadPoolInfo getForceMerge() {
        return getThreadPoolInfo(FORCE_MERGE);
    }

    @Override
    public ThreadPoolInfo getFetchShardStarted() {
        return getThreadPoolInfo(FETCH_SHARD_STARTED);
    }

    @Override
    public ThreadPoolInfo getFetchShardStore() {
        return getThreadPoolInfo(FETCH_SHARD_STORE);
    }

    @Override
    public ThreadPoolInfo getLogicalReplication() {
        return getThreadPoolInfo(LOGICAL_REPLICATION);
    }
}
