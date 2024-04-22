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

package io.crate.metadata.sys;

import static io.crate.types.DataTypes.DOUBLE;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.LONG;
import static io.crate.types.DataTypes.SHORT;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.TIMESTAMPZ;
import static io.crate.types.DataTypes.UNTYPED_OBJECT;

import org.elasticsearch.Version;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.threadpool.ThreadPoolStats;

import io.crate.expression.reference.sys.node.NodeStatsContext;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;
import io.crate.monitor.FsInfoHelpers;
import io.crate.types.DataTypes;

public class SysNodesTableInfo {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "nodes");

    private static final String SYS_COL_ID = "id";
    private static final String SYS_COL_NODE_NAME = "name";
    private static final String SYS_COL_HOSTNAME = "hostname";
    private static final String SYS_COL_REST_URL = "rest_url";
    private static final String SYS_COL_ATTRIBUTES = "attributes";
    private static final String SYS_COL_PORT = "port";
    private static final String SYS_COL_CLUSTER_STATE_VERSION = "cluster_state_version";
    private static final String SYS_COL_LOAD = "load";
    private static final String SYS_COL_MEM = "mem";
    private static final String SYS_COL_HEAP = "heap";
    private static final String SYS_COL_VERSION = "version";
    private static final String SYS_COL_THREAD_POOLS = "thread_pools";
    private static final String SYS_COL_OS = "os";
    private static final String SYS_COL_OS_INFO = "os_info";
    private static final String SYS_COL_PROCESS = "process";
    private static final String SYS_COL_FS = "fs";

    public static class Columns {
        public static final ColumnIdent ID = new ColumnIdent(SYS_COL_ID);
        public static final ColumnIdent NAME = new ColumnIdent(SYS_COL_NODE_NAME);
        public static final ColumnIdent HOSTNAME = new ColumnIdent(SYS_COL_HOSTNAME);
        public static final ColumnIdent REST_URL = new ColumnIdent(SYS_COL_REST_URL);
        public static final ColumnIdent ATTRIBUTES = new ColumnIdent(SYS_COL_ATTRIBUTES);

        public static final ColumnIdent PORT = new ColumnIdent(SYS_COL_PORT);
        public static final ColumnIdent CLUSTER_STATE_VERSION = new ColumnIdent(SYS_COL_CLUSTER_STATE_VERSION);

        public static final ColumnIdent LOAD = new ColumnIdent(SYS_COL_LOAD);

        public static final ColumnIdent MEM = new ColumnIdent(SYS_COL_MEM);

        public static final ColumnIdent HEAP = new ColumnIdent(SYS_COL_HEAP);

        public static final ColumnIdent VERSION = new ColumnIdent(SYS_COL_VERSION);

        public static final ColumnIdent THREAD_POOLS = new ColumnIdent(SYS_COL_THREAD_POOLS);

        public static final ColumnIdent CONNECTIONS = new ColumnIdent("connections");

        public static final ColumnIdent OS = new ColumnIdent(SYS_COL_OS);

        public static final ColumnIdent OS_INFO = new ColumnIdent(SYS_COL_OS_INFO);

        public static final ColumnIdent PROCESS = new ColumnIdent(SYS_COL_PROCESS);

        public static final ColumnIdent FS = new ColumnIdent(SYS_COL_FS);
    }


    public static SystemTable<NodeStatsContext> create() {
        return SystemTable.<NodeStatsContext>builder(IDENT)
            .add("id", STRING, NodeStatsContext::id)
            .add("name", STRING, NodeStatsContext::name)
            .add("hostname", STRING, NodeStatsContext::hostname)
            .add("rest_url", STRING, NodeStatsContext::restUrl)
            .add("attributes", UNTYPED_OBJECT, NodeStatsContext::attributes)
            .startObject("port")
                .add("http", INTEGER, NodeStatsContext::httpPort)
                .add("transport", INTEGER, NodeStatsContext::transportPort)
                .add("psql", INTEGER, NodeStatsContext::pgPort)
            .endObject()
            .startObject("load")
                .add("1", DOUBLE, x -> x.extendedOsStats().loadAverage()[0])
                .add("5", DOUBLE, x -> x.extendedOsStats().loadAverage()[1])
                .add("15", DOUBLE, x -> x.extendedOsStats().loadAverage()[2])
                .add("probe_timestamp", TIMESTAMPZ, x -> x.extendedOsStats().timestamp())
            .endObject()
            .startObject("mem")
                .add("free", LONG, x -> x.osStats().getMem().getFree().getBytes())
                .add("used", LONG, x -> x.osStats().getMem().getUsed().getBytes())
                .add("free_percent", SHORT, x -> x.osStats().getMem().getFreePercent())
                .add("used_percent", SHORT, x -> x.osStats().getMem().getUsedPercent())
                .add("probe_timestamp", TIMESTAMPZ, x -> x.osStats().getTimestamp())
            .endObject()
            .startObject("heap")
                .add("free", LONG, x -> {
                    JvmStats.Mem mem = x.jvmStats().getMem();
                    return mem.getHeapMax().getBytes() - mem.getHeapUsed().getBytes();
                })
                .add("used", LONG, x -> x.jvmStats().getMem().getHeapUsed().getBytes())
                .add("max", LONG, x -> x.jvmStats().getMem().getHeapMax().getBytes())
                .add("probe_timestamp", TIMESTAMPZ, x -> x.jvmStats().getTimestamp())
            .endObject()
            .startObject("version")
                .add("number", STRING, x -> x.version().externalNumber())
                .add("build_hash", STRING, x -> x.build().hash())
                .add("build_snapshot", DataTypes.BOOLEAN, x -> x.version().isSnapshot())
                .add("minimum_index_compatibility_version", STRING, x -> Version.CURRENT.minimumIndexCompatibilityVersion().externalNumber())
                .add("minimum_wire_compatibility_version", STRING, x -> Version.CURRENT.minimumCompatibilityVersion().externalNumber())
            .endObject()
            .add("cluster_state_version", LONG, NodeStatsContext::clusterStateVersion)
            .startObjectArray("thread_pools", NodeStatsContext::threadPools)
                .add("name", STRING, ThreadPoolStats.Stats::getName)
                .add("active", INTEGER, ThreadPoolStats.Stats::getActive)
                .add("rejected", LONG, ThreadPoolStats.Stats::getRejected)
                .add("largest", INTEGER, ThreadPoolStats.Stats::getLargest)
                .add("completed", LONG, ThreadPoolStats.Stats::getCompleted)
                .add("threads", INTEGER, ThreadPoolStats.Stats::getThreads)
                .add("queue", INTEGER, ThreadPoolStats.Stats::getQueue)
            .endObjectArray()
            .startObject("connections")
                .startObject("http")
                    .add("open", LONG, x -> x.httpStats().open())
                    .add("total", LONG, x -> x.httpStats().total())
                .endObject()
                .startObject("psql")
                    .add("open", LONG, x -> x.psqlStats().open())
                    .add("total", LONG, x -> x.psqlStats().total())
                .endObject()
                .startObject("transport")
                    .add("open", LONG, x -> x.transportStats().open())
                    .add("total", LONG, x -> x.transportStats().total())
                .endObject()
            .endObject()
            .startObject("os")
                .add("uptime", LONG, x -> x.extendedOsStats().uptime().millis())
                .add("timestamp", TIMESTAMPZ, NodeStatsContext::timestamp)
                .add("probe_timestamp", TIMESTAMPZ, x -> x.extendedOsStats().timestamp())
                .startObject("cpu")
                    .add("used", SHORT, x -> (short) -1)
                    .add("system", SHORT, x -> (short) - 1)
                    .add("user", SHORT, x -> (short) -1)
                .endObject()
                .startObject("cgroup")
                    .startObject("cpuacct")
                        .add("control_group", STRING, x -> x.extendedOsStats().osStats().getCgroup().getCpuAcctControlGroup())
                        .add("usage_nanos", LONG, x -> x.extendedOsStats().osStats().getCgroup().getCpuAcctUsageNanos())
                    .endObject()
                    .startObject("cpu")
                        .add("control_group", STRING, x -> x.extendedOsStats().osStats().getCgroup().getCpuControlGroup())
                        .add("cfs_period_micros", LONG, x -> x.extendedOsStats().osStats().getCgroup().getCpuCfsPeriodMicros())
                        .add("cfs_quota_micros", LONG, x -> x.extendedOsStats().osStats().getCgroup().getCpuCfsQuotaMicros())
                        .add("num_elapsed_periods", LONG,
                            x -> x.extendedOsStats().osStats().getCgroup().getCpuStat().getNumberOfElapsedPeriods())
                        .add("num_times_throttled", LONG,
                            x -> x.extendedOsStats().osStats().getCgroup().getCpuStat().getNumberOfTimesThrottled())
                        .add("time_throttled_nanos", LONG,
                            x -> x.extendedOsStats().osStats().getCgroup().getCpuStat().getTimeThrottledNanos())
                    .endObject()
                    .startObject("mem")
                        .add("control_group", STRING, x -> x.extendedOsStats().osStats().getCgroup().getMemoryControlGroup())
                        .add("limit_bytes", STRING, x -> x.extendedOsStats().osStats().getCgroup().getMemoryLimitInBytes())
                        .add("usage_bytes", STRING, x -> x.extendedOsStats().osStats().getCgroup().getMemoryUsageInBytes())
                    .endObject()
                .endObject()
            .endObject()
            .startObject("os_info")
                .add("available_processors", INTEGER, x -> x.osInfo().getAvailableProcessors())
                .add("name", STRING, NodeStatsContext::osName)
                .add("arch", STRING, NodeStatsContext::osArch)
                .add("version", STRING, NodeStatsContext::osVersion)
                .startObject("jvm")
                    .add("version", STRING, NodeStatsContext::javaVersion)
                    .add("vm_name", STRING, NodeStatsContext::jvmName)
                    .add("vm_vendor", STRING, NodeStatsContext::jvmVendor)
                    .add("vm_version", STRING, NodeStatsContext::jvmVersion)
                .endObject()
            .endObject()
            .startObject("process")
                .add("open_file_descriptors", LONG, x -> x.processStats().getOpenFileDescriptors())
                .add("max_open_file_descriptors", LONG, x -> x.processStats().getMaxFileDescriptors())
                .add("probe_timestamp", TIMESTAMPZ, x -> x.processStats().getTimestamp())
                .startObject("cpu")
                    .add("percent", SHORT, x -> x.processStats().getCpu().getPercent())
                .endObject()
            .endObject()
            .startObject("fs")
                .startObject("total")
                    .add("size", LONG, x -> FsInfoHelpers.Path.size(x.fsInfo().getTotal()))
                    .add("used", LONG, x -> FsInfoHelpers.Path.used(x.fsInfo().getTotal()))
                    .add("available", LONG, x -> FsInfoHelpers.Path.available(x.fsInfo().getTotal()))
                    .add("reads", LONG, x -> FsInfoHelpers.Stats.readOperations(x.fsInfo().getIoStats()))
                    .add("bytes_read", LONG, x -> FsInfoHelpers.Stats.bytesRead(x.fsInfo().getIoStats()))
                    .add("writes", LONG, x -> FsInfoHelpers.Stats.writeOperations(x.fsInfo().getIoStats()))
                    .add("bytes_written", LONG, x -> FsInfoHelpers.Stats.bytesWritten(x.fsInfo().getIoStats()))
                .endObject()
                .startObjectArray("disks", NodeStatsContext::fsInfo)
                    .add("dev", STRING, FsInfoHelpers.Path::dev)
                    .add("size", LONG, FsInfoHelpers.Path::size)
                    .add("used", LONG, FsInfoHelpers.Path::used)
                    .add("available", LONG, FsInfoHelpers.Path::available)
                .endObjectArray()
                .startObjectArray("data", NodeStatsContext::fsInfo)
                    .add("dev", STRING, FsInfoHelpers.Path::dev)
                    .add("path", STRING, FsInfo.Path::getPath)
                .endObjectArray()
            .endObject()
            .setPrimaryKeys(new ColumnIdent("id"))
            .build();
    }
}
