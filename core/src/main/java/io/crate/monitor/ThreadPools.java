/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.monitor;

import com.google.common.base.Objects;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.util.concurrent.XRejectedExecutionHandler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

public class ThreadPools implements Streamable, Iterable<Map.Entry<BytesRef, ThreadPools.ThreadPoolExecutorContext>> {

    private final Map<BytesRef, ThreadPoolExecutorContext> contexts;

    public static ThreadPools newInstance(ThreadPool threadPool) {
        ThreadPools threadPools = ThreadPools.newInstance();
        for (ThreadPool.Info info : threadPool.info()) {
            String name = info.getName();
            ThreadPoolExecutor executor = (ThreadPoolExecutor) threadPool.executor(name);
            threadPools.add(BytesRefs.toBytesRef(name), ThreadPoolExecutorContext.newInstance(executor));
        }
        return threadPools;
    }

    public static ThreadPools newInstance() {
        return new ThreadPools();
    }

    private ThreadPools() {
        this.contexts = new HashMap<>();
    }

    public void add(BytesRef threadPool, ThreadPoolExecutorContext context) {
        this.contexts.put(threadPool, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            BytesRef key = in.readBytesRef();
            ThreadPoolExecutorContext value = new ThreadPoolExecutorContext();
            value.readFrom(in);
            contexts.put(key, value);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(size());
        for (Map.Entry<BytesRef, ThreadPoolExecutorContext> entry : contexts.entrySet()) {
            out.writeBytesRef(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }

    public int size() {
        return contexts.size();
    }

    @Override
    public Iterator<Map.Entry<BytesRef, ThreadPoolExecutorContext>> iterator() {
        return contexts.entrySet().iterator();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ThreadPools that = (ThreadPools) o;
        return Objects.equal(contexts, that.contexts);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(contexts);
    }

    public static ThreadPools readThreadPools(StreamInput in) throws IOException {
        ThreadPools threadPools = new ThreadPools();
        threadPools.readFrom(in);
        return threadPools;
    }

    public static class ThreadPoolExecutorContext implements Streamable {

        private int queueSize;
        private int activeCount;
        private int largestPoolSize;
        private int poolSize;
        private long completedTaskCount;
        private long rejectedCount;

        public static ThreadPoolExecutorContext newInstance(ThreadPoolExecutor executor) {
            long rejectedCount = -1;
            RejectedExecutionHandler rejectedExecutionHandler = executor.getRejectedExecutionHandler();

            if (rejectedExecutionHandler instanceof XRejectedExecutionHandler) {
                rejectedCount = ((XRejectedExecutionHandler) rejectedExecutionHandler).rejected();
            }

            return new ThreadPoolExecutorContext(
                executor.getActiveCount(),
                executor.getQueue().size(),
                executor.getLargestPoolSize(),
                executor.getPoolSize(),
                executor.getCompletedTaskCount(),
                rejectedCount);

        }

        public ThreadPoolExecutorContext() {
        }

        public ThreadPoolExecutorContext(int queueSize,
                                         int activeCount,
                                         int largestPoolSize,
                                         int poolSize,
                                         long completedTaskCount,
                                         long rejectedCount) {
            this.queueSize = queueSize;
            this.activeCount = activeCount;
            this.largestPoolSize = largestPoolSize;
            this.poolSize = poolSize;
            this.completedTaskCount = completedTaskCount;
            this.rejectedCount = rejectedCount;
        }

        public int queueSize() {
            return queueSize;
        }

        public int activeCount() {
            return activeCount;
        }

        public int largestPoolSize() {
            return largestPoolSize;
        }

        public int poolSize() {
            return poolSize;
        }

        public long completedTaskCount() {
            return completedTaskCount;
        }

        public long rejectedCount() {
            return rejectedCount;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            queueSize = in.readInt();
            activeCount = in.readInt();
            largestPoolSize = in.readInt();
            poolSize = in.readInt();
            completedTaskCount = in.readLong();
            rejectedCount = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(queueSize);
            out.writeInt(activeCount);
            out.writeInt(largestPoolSize);
            out.writeInt(poolSize);
            out.writeLong(completedTaskCount);
            out.writeLong(rejectedCount);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ThreadPoolExecutorContext that = (ThreadPoolExecutorContext) o;
            return Objects.equal(queueSize, that.queueSize) &&
                   Objects.equal(activeCount, that.activeCount) &&
                   Objects.equal(largestPoolSize, that.largestPoolSize) &&
                   Objects.equal(poolSize, that.poolSize) &&
                   Objects.equal(completedTaskCount, that.completedTaskCount) &&
                   Objects.equal(rejectedCount, that.rejectedCount);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(queueSize, activeCount, largestPoolSize, poolSize, completedTaskCount, rejectedCount);
        }
    }
}
