/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.beans;

import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;

import javax.annotation.Nullable;
import java.beans.ConstructorProperties;

import static org.elasticsearch.threadpool.ThreadPool.Names.FETCH_SHARD_STARTED;
import static org.elasticsearch.threadpool.ThreadPool.Names.FETCH_SHARD_STORE;
import static org.elasticsearch.threadpool.ThreadPool.Names.FLUSH;
import static org.elasticsearch.threadpool.ThreadPool.Names.FORCE_MERGE;
import static org.elasticsearch.threadpool.ThreadPool.Names.GENERIC;
import static org.elasticsearch.threadpool.ThreadPool.Names.GET;
import static org.elasticsearch.threadpool.ThreadPool.Names.LISTENER;
import static org.elasticsearch.threadpool.ThreadPool.Names.MANAGEMENT;
import static org.elasticsearch.threadpool.ThreadPool.Names.REFRESH;
import static org.elasticsearch.threadpool.ThreadPool.Names.SEARCH;
import static org.elasticsearch.threadpool.ThreadPool.Names.SNAPSHOT;
import static org.elasticsearch.threadpool.ThreadPool.Names.WRITE;

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
    public ThreadPoolInfo getGet() {
        return getThreadPoolInfo(GET);
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
}
