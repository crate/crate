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

package io.crate.operation.reference.sys.node;

import io.crate.Build;
import io.crate.Version;
import io.crate.monitor.ExtendedFsStats;
import io.crate.monitor.ExtendedNetworkStats;
import io.crate.monitor.ExtendedOsStats;
import io.crate.monitor.ExtendedProcessCpuStats;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.monitor.process.ProcessStats;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class DiscoveryNodeContext {

    public String id;
    public String name;
    public String hostname;
    public Version version;
    public Build build;
    public String restUrl;
    public Map<String, Integer> port;
    public JvmStats jvmStats;
    public OsInfo osInfo;
    public ProcessStats processStats;
    public ExtendedOsStats osStats;
    public ExtendedNetworkStats networkStats;
    public ExtendedProcessCpuStats cpuStats;
    public ExtendedFsStats fsStats;
    public ThreadPools threadPools;

    public final BytesRef OS_NAME = BytesRefs.toBytesRef(Constants.OS_NAME);
    public final BytesRef OS_ARCH = BytesRefs.toBytesRef(Constants.OS_ARCH);
    public final BytesRef OS_VERSION = BytesRefs.toBytesRef(Constants.OS_VERSION);

    public final BytesRef JAVA_VERSION = BytesRefs.toBytesRef(Constants.JAVA_VERSION);
    public final BytesRef JVM_NAME = BytesRefs.toBytesRef(Constants.JVM_NAME);
    public final BytesRef JVM_VENDOR = BytesRefs.toBytesRef(Constants.JVM_VENDOR);
    public final BytesRef JVM_VERSION = BytesRefs.toBytesRef(Constants.JVM_VERSION);

    /**
     * For now this context only contains dummy values!
     * TODO: Populate context with correct data
     */
    public DiscoveryNodeContext() {
    }

    class ThreadPools implements Streamable, Iterable<String> {

        private Map<String, ThreadPoolExecutorContext> threadPoolExecutorContext;

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
    }

    class ThreadPoolExecutorContext implements Streamable {

        public Integer queueSize;
        public Integer activeCount;
        public Integer largestPoolSize;
        public Integer poolSize;
        public Long completedTaskCount;
        public Long rejectedCount;

        @Override
        public void readFrom(StreamInput in) throws IOException {

        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }
    }
}
