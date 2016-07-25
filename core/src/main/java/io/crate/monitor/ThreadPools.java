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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ThreadPools implements Streamable, Iterable<String> {

    private Map<String, ThreadPoolExecutorContext> threadPoolExecutorContext = new HashMap<>();

    @Override
    public void readFrom(StreamInput in) throws IOException {

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

    public int size() {
        return threadPoolExecutorContext.size();
    }

    @Override
    public Iterator<String> iterator() {
        return threadPoolExecutorContext.keySet().iterator();
    }

    public ThreadPoolExecutorContext get(String name) {
        return threadPoolExecutorContext.get(name);
    }

    public void add(String name, ThreadPoolExecutorContext context) {
        threadPoolExecutorContext.put(name, context);
    }

    public static class ThreadPoolExecutorContext implements Streamable {

        public int activeCount;
        public int queueSize;
        public int largestPoolSize;
        public int poolSize;
        public long completedTaskCount;
        public long rejectedCount;

        public ThreadPoolExecutorContext() {
        }

        public ThreadPoolExecutorContext(int activeCount,
                                         int queueSize,
                                         int largestPoolSize,
                                         int poolSize,
                                         long completedTaskCount,
                                         long rejectedCount) {
            this.activeCount = activeCount;
            this.queueSize = queueSize;
            this.poolSize = poolSize;
            this.largestPoolSize = largestPoolSize;
            this.completedTaskCount = completedTaskCount;
            this.rejectedCount = rejectedCount;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {

        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }
    }
}
