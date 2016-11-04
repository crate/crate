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
import io.crate.monitor.*;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.monitor.process.ProcessStats;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class NodeStatsContext implements Streamable {

    private final boolean complete;

    private BytesRef id;
    private BytesRef name;
    private BytesRef hostname;
    private long timestamp;
    private Version version;
    private Build build;
    private BytesRef restUrl;
    private Map<String, Integer> port;
    private JvmStats jvmStats;
    private OsInfo osInfo;
    private ProcessStats processStats;
    private OsStats osStats;
    private ExtendedOsStats extendedOsStats;
    private ExtendedNetworkStats networkStats;
    private ExtendedProcessCpuStats extendedProcessCpuStats;
    private ExtendedFsStats extendedFsStats;
    private ThreadPools threadPools;

    private BytesRef osName;
    private BytesRef osArch;
    private BytesRef osVersion;

    private BytesRef javaVersion;
    private BytesRef jvmName;
    private BytesRef jvmVendor;
    private BytesRef jvmVersion;

    public NodeStatsContext(String id, String name) {
        this(false);
        this.id = BytesRefs.toBytesRef(id);
        this.name = BytesRefs.toBytesRef(name);
    }

    public NodeStatsContext(boolean complete) {
        this.complete = complete;
        if (complete) {
            osName = BytesRefs.toBytesRef(Constants.OS_NAME);
            osArch = BytesRefs.toBytesRef(Constants.OS_ARCH);
            osVersion = BytesRefs.toBytesRef(Constants.OS_VERSION);
            javaVersion = BytesRefs.toBytesRef(Constants.JAVA_VERSION);
            jvmName = BytesRefs.toBytesRef(Constants.JVM_NAME);
            jvmVendor = BytesRefs.toBytesRef(Constants.JVM_VENDOR);
            jvmVersion = BytesRefs.toBytesRef(Constants.JVM_VERSION);
        }
    }

    public boolean isComplete() {
        return complete;
    }

    public BytesRef id() {
        return id;
    }

    public BytesRef name() {
        return name;
    }

    public BytesRef hostname() {
        return hostname;
    }

    public long timestamp() {
        return timestamp;
    }

    public Version version() {
        return version;
    }

    public Build build() {
        return build;
    }

    public BytesRef restUrl() {
        return restUrl;
    }

    public Map<String, Integer> port() {
        return port;
    }

    public JvmStats jvmStats() {
        return jvmStats;
    }

    public OsInfo osInfo() {
        return osInfo;
    }

    public ProcessStats processStats() {
        return processStats;
    }

    public OsStats osStats() {
        return osStats;
    }

    public ExtendedOsStats extendedOsStats() {
        return extendedOsStats;
    }

    public ExtendedNetworkStats networkStats() {
        return networkStats;
    }

    public ExtendedProcessCpuStats extendedProcessCpuStats() {
        return extendedProcessCpuStats;
    }

    public ExtendedFsStats extendedFsStats() {
        return extendedFsStats;
    }

    public ThreadPools threadPools() {
        return threadPools;
    }

    public BytesRef osName() {
        return osName;
    }

    public BytesRef osArch() {
        return osArch;
    }

    public BytesRef osVersion() {
        return osVersion;
    }

    public BytesRef javaVersion() {
        return javaVersion;
    }

    public BytesRef jvmName() {
        return jvmName;
    }

    public BytesRef jvmVendor() {
        return jvmVendor;
    }

    public BytesRef jvmVersion() {
        return jvmVersion;
    }

    public void id(BytesRef id) {
        this.id = id;
    }

    public void name(BytesRef name) {
        this.name = name;
    }

    public void hostname(BytesRef hostname) {
        this.hostname = hostname;
    }

    public void timestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void version(Version version) {
        this.version = version;
    }

    public void build(Build build) {
        this.build = build;
    }

    public void restUrl(BytesRef restUrl) {
        this.restUrl = restUrl;
    }

    public void port(Map<String, Integer> port) {
        this.port = port;
    }

    public void jvmStats(JvmStats jvmStats) {
        this.jvmStats = jvmStats;
    }

    public void osInfo(OsInfo osInfo) {
        this.osInfo = osInfo;
    }

    public void processStats(ProcessStats processStats) {
        this.processStats = processStats;
    }

    public void osStats(OsStats osStats) {
        this.osStats = osStats;
    }

    public void extendedOsStats(ExtendedOsStats extendedOsStats) {
        this.extendedOsStats = extendedOsStats;
    }

    public void networkStats(ExtendedNetworkStats networkStats) {
        this.networkStats = networkStats;
    }

    public void extendedProcessCpuStats(ExtendedProcessCpuStats extendedProcessCpuStats) {
        this.extendedProcessCpuStats = extendedProcessCpuStats;
    }

    public void extendedFsStats(ExtendedFsStats extendedFsStats) {
        this.extendedFsStats = extendedFsStats;
    }

    public void threadPools(ThreadPools threadPools) {
        this.threadPools = threadPools;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        id = DataTypes.STRING.readValueFrom(in);
        name = DataTypes.STRING.readValueFrom(in);
        hostname = DataTypes.STRING.readValueFrom(in);
        timestamp = in.readLong();
        version = in.readBoolean() ? Version.readVersion(in) : null;
        build = in.readBoolean() ? Build.readBuild(in) : null;
        restUrl = DataTypes.STRING.readValueFrom(in);
        if (in.readBoolean()) {
            int size = in.readVInt();
            port = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                port.put(in.readString(), in.readOptionalVInt());
            }
        } else {
            port = null;
        }
        jvmStats = in.readOptionalWriteable(JvmStats::new);
        osInfo = in.readOptionalWriteable(OsInfo::new);
        processStats = in.readOptionalWriteable(ProcessStats::new);
        osStats = in.readOptionalWriteable(OsStats::new);
        extendedOsStats = in.readBoolean() ? ExtendedOsStats.readExtendedOsStat(in) : null;
        networkStats = in.readBoolean() ? ExtendedNetworkStats.readExtendedNetworkStats(in) : null;
        extendedProcessCpuStats = in.readBoolean() ? ExtendedProcessCpuStats.readExtendedProcessCpuStats(in) : null;
        extendedFsStats = in.readBoolean() ? ExtendedFsStats.readExtendedFsStats(in) : null;
        threadPools = in.readBoolean() ? ThreadPools.readThreadPools(in) : null;

        osName = DataTypes.STRING.readValueFrom(in);
        osArch = DataTypes.STRING.readValueFrom(in);
        osVersion = DataTypes.STRING.readValueFrom(in);
        javaVersion = DataTypes.STRING.readValueFrom(in);
        jvmName = DataTypes.STRING.readValueFrom(in);
        jvmVendor = DataTypes.STRING.readValueFrom(in);
        jvmVersion = DataTypes.STRING.readValueFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        DataTypes.STRING.writeValueTo(out, id);
        DataTypes.STRING.writeValueTo(out, name);
        DataTypes.STRING.writeValueTo(out, hostname);
        out.writeLong(timestamp);
        out.writeBoolean(version != null);
        if (version != null) {
            Version.writeVersionTo(version, out);
        }
        out.writeBoolean(build != null);
        if (build != null) {
            Build.writeBuildTo(build, out);
        }
        DataTypes.STRING.writeValueTo(out, restUrl);
        out.writeBoolean(port != null);
        if (port != null) {
            out.writeVInt(port.size());
            for (Map.Entry<String, Integer> p : port.entrySet()) {
                out.writeString(p.getKey());
                out.writeOptionalVInt(p.getValue());
            }
        }
        out.writeOptionalWriteable(jvmStats);
        out.writeOptionalWriteable(osInfo);
        out.writeOptionalWriteable(processStats);
        out.writeOptionalWriteable(osStats);
        out.writeOptionalStreamable(extendedOsStats);
        out.writeOptionalStreamable(networkStats);
        out.writeOptionalStreamable(extendedProcessCpuStats);
        out.writeOptionalStreamable(extendedFsStats);
        out.writeOptionalStreamable(threadPools);

        DataTypes.STRING.writeValueTo(out, osName);
        DataTypes.STRING.writeValueTo(out, osArch);
        DataTypes.STRING.writeValueTo(out, osVersion);
        DataTypes.STRING.writeValueTo(out, javaVersion);
        DataTypes.STRING.writeValueTo(out, jvmName);
        DataTypes.STRING.writeValueTo(out, jvmVendor);
        DataTypes.STRING.writeValueTo(out, jvmVersion);
    }
}
