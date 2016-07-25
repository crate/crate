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

public class DiscoveryNodeContext implements Streamable {

    private final boolean timedOut;

    public BytesRef id;
    public BytesRef name;
    public BytesRef hostname;
    public Version version;
    public Build build;
    public BytesRef restUrl;
    public Map<String, Integer> port;
    public JvmStats jvmStats;
    public OsInfo osInfo;
    public ProcessStats processStats;
    public OsStats osStats;
    public ExtendedOsStats extendedOsStats;
    public ExtendedNetworkStats networkStats;
    public ExtendedProcessCpuStats extendedProcessCpuStats;
    public ExtendedFsStats extendedFsStats;
    public ThreadPools threadPools;

    public BytesRef osName;
    public BytesRef osArch;
    public BytesRef osVersion;

    public BytesRef javaVersion;
    public BytesRef jvmName;
    public BytesRef jvmVendor;
    public BytesRef jvmVersion;


    /**
     * For now this context only contains dummy values!
     * TODO: Populate context with correct data
     */
    public DiscoveryNodeContext(boolean timedOut) {
        this.timedOut = timedOut;
        if (!timedOut) {
            osName = BytesRefs.toBytesRef(Constants.OS_NAME);
            osArch = BytesRefs.toBytesRef(Constants.OS_ARCH);
            osVersion = BytesRefs.toBytesRef(Constants.OS_VERSION);
            javaVersion = BytesRefs.toBytesRef(Constants.JAVA_VERSION);
            jvmName = BytesRefs.toBytesRef(Constants.JVM_NAME);
            jvmVendor = BytesRefs.toBytesRef(Constants.JVM_VENDOR);
            jvmVersion = BytesRefs.toBytesRef(Constants.JVM_VERSION);
        }
    }

    public DiscoveryNodeContext() {
        this(true);
    }

    public boolean timedOut() {
        return timedOut;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        id = DataTypes.STRING.readValueFrom(in);
        name = DataTypes.STRING.readValueFrom(in);
        hostname = DataTypes.STRING.readValueFrom(in);
        version = in.readBoolean() ? null : Version.fromStream(in);
        build = in.readBoolean() ? null : Build.fromStream(in);
        restUrl = DataTypes.STRING.readValueFrom(in);
        if (in.readBoolean()) {
            port = null;
        } else {
            int size = in.readVInt();
            port = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                port.put(in.readString(), in.readVInt());
            }
        }
//        jvmStats = in.readOptionalStreamable(new JvmStats(0, 0));
//        osInfo = in.readOptionalStreamable(new OsInfo());
//        in.readOptionalStreamable(processStats);
//        in.readOptionalStreamable(osStats);
//        in.readOptionalStreamable(extendedOsStats);
//        in.readOptionalStreamable(networkStats);
//        in.readOptionalStreamable(cpuStats);
//        in.readOptionalStreamable(fsStats);
//        in.readOptionalStreamable(threadPools);

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
        out.writeBoolean(version == null);
        if (version != null) {
            Version.writeVersion(version, out);
        }
        out.writeBoolean(build == null);
        if (build != null) {
            Build.writeBuild(build, out);
        }
        DataTypes.STRING.writeValueTo(out, restUrl);
        out.writeBoolean(port == null);
        if (port != null) {
            out.writeBoolean(true);
            out.writeVInt(port.size());
            for (Map.Entry<String, Integer> p : port.entrySet()) {
                out.writeString(p.getKey());
                out.writeVInt(p.getValue());
            }
        }
//        out.writeOptionalStreamable(jvmStats);
//        out.writeOptionalStreamable(osInfo);
//        out.writeOptionalStreamable(processStats);
//        out.writeOptionalStreamable(osStats);
//        out.writeOptionalStreamable(extendedOsStats);
//        out.writeOptionalStreamable(networkStats);
//        out.writeOptionalStreamable(cpuStats);
//        out.writeOptionalStreamable(fsStats);
//        out.writeOptionalStreamable(threadPools);

        DataTypes.STRING.writeValueTo(out, osName);
        DataTypes.STRING.writeValueTo(out, osArch);
        DataTypes.STRING.writeValueTo(out, osVersion);
        DataTypes.STRING.writeValueTo(out, javaVersion);
        DataTypes.STRING.writeValueTo(out, jvmName);
        DataTypes.STRING.writeValueTo(out, jvmVendor);
        DataTypes.STRING.writeValueTo(out, jvmVersion);
    }

}
