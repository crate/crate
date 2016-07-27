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
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ThreadPools implements Streamable, Iterable<Map.Entry<String, ThreadPools.ThreadPoolExecutorContext>> {

    private Map<String, ThreadPoolExecutorContext> contexts;

    public ThreadPools() {
        this.contexts = new HashMap<>();
    }

    public void add(String threadPool, ThreadPoolExecutorContext context) {
        this.contexts.put(threadPool, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        contexts = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            String key = in.readString();
            ThreadPoolExecutorContext value = new ThreadPoolExecutorContext();
            value.readFrom(in);
            contexts.put(key, value);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(size());
        for (Map.Entry<String, ThreadPoolExecutorContext> entry : contexts.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }

    public int size() {
        return contexts.size();
    }

    @Override
    public Iterator<Map.Entry<String, ThreadPoolExecutorContext>> iterator() {
        return contexts.entrySet().iterator();
    }

    public ThreadPoolExecutorContext get(String name) {
        return contexts.get(name);
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

        private Integer queueSize;
        private Integer activeCount;
        private Integer largestPoolSize;
        private Integer poolSize;
        private Long completedTaskCount;
        private Long rejectedCount;

        public ThreadPoolExecutorContext() {}

        public ThreadPoolExecutorContext(Integer queueSize, Integer activeCount, Integer largestPoolSize, Integer poolSize,
                                         Long completedTaskCount, Long rejectedCount) {
            this.queueSize = queueSize;
            this.activeCount = activeCount;
            this.largestPoolSize = largestPoolSize;
            this.poolSize = poolSize;
            this.completedTaskCount = completedTaskCount;
            this.rejectedCount = rejectedCount;
        }

        public Integer queueSize() {
            return queueSize;
        }

        public Integer activeCount() {
            return activeCount;
        }

        public Integer largestPoolSize() {
            return largestPoolSize;
        }

        public Integer poolSize() {
            return poolSize;
        }

        public Long completedTaskCount() {
            return completedTaskCount;
        }

        public Long rejectedCount() {
            return rejectedCount;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            queueSize = DataTypes.INTEGER.readValueFrom(in);
            activeCount = DataTypes.INTEGER.readValueFrom(in);
            largestPoolSize = DataTypes.INTEGER.readValueFrom(in);
            poolSize = DataTypes.INTEGER.readValueFrom(in);
            completedTaskCount = DataTypes.LONG.readValueFrom(in);
            rejectedCount = DataTypes.LONG.readValueFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            DataTypes.INTEGER.writeValueTo(out, queueSize);
            DataTypes.INTEGER.writeValueTo(out, activeCount);
            DataTypes.INTEGER.writeValueTo(out, largestPoolSize);
            DataTypes.INTEGER.writeValueTo(out, poolSize);
            DataTypes.LONG.writeValueTo(out, completedTaskCount);
            DataTypes.LONG.writeValueTo(out, rejectedCount);
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
