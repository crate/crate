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

package io.crate.expression.reference.sys.node;

import io.crate.Build;
import io.crate.monitor.ExtendedOsStats;
import io.crate.protocols.ConnectionStats;
import io.crate.types.DataTypes;
import org.apache.lucene.util.Constants;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.monitor.process.ProcessStats;
import org.elasticsearch.threadpool.ThreadPoolStats;

import java.io.IOException;

public class NodeStatsContext implements Streamable {

    private final boolean complete;

    private String id;
    private String name;
    private String hostname;
    private long timestamp;
    private Version version;
    private long clusterStateVersion;
    private Build build;
    private String restUrl;
    private JvmStats jvmStats;
    private OsInfo osInfo;
    private ProcessStats processStats;
    private OsStats osStats;
    private ExtendedOsStats extendedOsStats;
    private FsInfo fsInfo;
    private ThreadPoolStats threadPools;
    private HttpStats httpStats;
    private ConnectionStats psqlStats;

    private String osName;
    private String osArch;
    private String osVersion;

    private String javaVersion;
    private String jvmName;
    private String jvmVendor;
    private String jvmVersion;
    private long openTransportConnections = 0L;
    private Integer transportPort;
    private Integer httpPort;
    private Integer pgPort;

    public NodeStatsContext(String id, String name) {
        this(false);
        this.id = id;
        this.name = name;
    }

    public NodeStatsContext(boolean complete) {
        this.complete = complete;
        if (complete) {
            osName = Constants.OS_NAME;
            osArch = Constants.OS_ARCH;
            osVersion = Constants.OS_VERSION;
            javaVersion = Constants.JAVA_VERSION;
            jvmName = Constants.JVM_NAME;
            jvmVendor = Constants.JVM_VENDOR;
            jvmVersion = Constants.JVM_VERSION;
        }
    }

    public boolean isComplete() {
        return complete;
    }

    public String id() {
        return id;
    }

    public String name() {
        return name;
    }

    public String hostname() {
        return hostname;
    }

    public long timestamp() {
        return timestamp;
    }

    public Version version() {
        return version;
    }

    public long clusterStateVersion() {
        return clusterStateVersion;
    }

    public Build build() {
        return build;
    }

    public String restUrl() {
        return restUrl;
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

    public FsInfo fsInfo() {
        return fsInfo;
    }

    public ExtendedOsStats extendedOsStats() {
        return extendedOsStats;
    }

    public ThreadPoolStats threadPools() {
        return threadPools;
    }

    public String osName() {
        return osName;
    }

    public String osArch() {
        return osArch;
    }

    public String osVersion() {
        return osVersion;
    }

    public String javaVersion() {
        return javaVersion;
    }

    public String jvmName() {
        return jvmName;
    }

    public String jvmVendor() {
        return jvmVendor;
    }

    public String jvmVersion() {
        return jvmVersion;
    }

    public HttpStats httpStats() {
        return httpStats;
    }

    public ConnectionStats psqlStats() {
        return psqlStats;
    }

    public long openTransportConnections() {
        return openTransportConnections;
    }

    public Integer httpPort() {
        return httpPort;
    }

    public Integer pgPort() {
        return pgPort;
    }

    public Integer transportPort() {
        return transportPort;
    }

    public void id(String id) {
        this.id = id;
    }

    public void name(String name) {
        this.name = name;
    }

    public void hostname(String hostname) {
        this.hostname = hostname;
    }

    public void timestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void version(Version version) {
        this.version = version;
    }

    public void clusterStateVersion(long clusterStateVersion) {
        this.clusterStateVersion = clusterStateVersion;
    }

    public void build(Build build) {
        this.build = build;
    }

    public void restUrl(String restUrl) {
        this.restUrl = restUrl;
    }

    public void httpPort(Integer http) {
        this.httpPort = http;
    }

    public void pgPort(Integer pgPort) {
        this.pgPort = pgPort;
    }

    public void transportPort(Integer transportPort) {
        this.transportPort = transportPort;
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

    public void fsInfo(FsInfo fsInfo) {
        this.fsInfo = fsInfo;
    }

    public void extendedOsStats(ExtendedOsStats extendedOsStats) {
        this.extendedOsStats = extendedOsStats;
    }

    public void threadPools(ThreadPoolStats threadPools) {
        this.threadPools = threadPools;
    }

    public void httpStats(HttpStats httpStats) {
        this.httpStats = httpStats;
    }

    public void psqlStats(ConnectionStats psqlStats) {
        this.psqlStats = psqlStats;
    }

    void openTransportConnections(long openTransportConnections) {
        this.openTransportConnections = openTransportConnections;
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
        pgPort = in.readOptionalVInt();
        httpPort = in.readOptionalVInt();
        transportPort = in.readOptionalVInt();
        jvmStats = in.readOptionalWriteable(JvmStats::new);
        osInfo = in.readOptionalWriteable(OsInfo::new);
        processStats = in.readOptionalWriteable(ProcessStats::new);
        osStats = in.readOptionalWriteable(OsStats::new);
        fsInfo = in.readOptionalWriteable(FsInfo::new);
        extendedOsStats = in.readBoolean() ? ExtendedOsStats.readExtendedOsStat(in) : null;
        threadPools = in.readOptionalWriteable(ThreadPoolStats::new);
        httpStats = in.readOptionalWriteable(HttpStats::new);
        psqlStats = in.readOptionalWriteable(ConnectionStats::new);
        openTransportConnections = in.readLong();
        clusterStateVersion = in.readLong();

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
            Version.writeVersion(version, out);
        }
        out.writeBoolean(build != null);
        if (build != null) {
            Build.writeBuildTo(build, out);
        }
        DataTypes.STRING.writeValueTo(out, restUrl);
        out.writeOptionalVInt(pgPort);
        out.writeOptionalVInt(httpPort);
        out.writeOptionalVInt(transportPort);
        out.writeOptionalWriteable(jvmStats);
        out.writeOptionalWriteable(osInfo);
        out.writeOptionalWriteable(processStats);
        out.writeOptionalWriteable(osStats);
        out.writeOptionalWriteable(fsInfo);
        out.writeOptionalStreamable(extendedOsStats);
        out.writeOptionalWriteable(threadPools);
        out.writeOptionalWriteable(httpStats);
        out.writeOptionalWriteable(psqlStats);
        out.writeLong(openTransportConnections);
        out.writeLong(clusterStateVersion);

        DataTypes.STRING.writeValueTo(out, osName);
        DataTypes.STRING.writeValueTo(out, osArch);
        DataTypes.STRING.writeValueTo(out, osVersion);
        DataTypes.STRING.writeValueTo(out, javaVersion);
        DataTypes.STRING.writeValueTo(out, jvmName);
        DataTypes.STRING.writeValueTo(out, jvmVendor);
        DataTypes.STRING.writeValueTo(out, jvmVersion);
    }
}
