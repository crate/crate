/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.expression.reference.sys.node.NodeHeapStatsExpression;
import io.crate.expression.reference.sys.node.NodeLoadStatsExpression;
import io.crate.expression.reference.sys.node.NodeMemoryStatsExpression;
import io.crate.expression.reference.sys.node.NodeNetworkStatsExpression;
import io.crate.expression.reference.sys.node.NodeOsInfoStatsExpression;
import io.crate.expression.reference.sys.node.NodeOsStatsExpression;
import io.crate.expression.reference.sys.node.NodePortStatsExpression;
import io.crate.expression.reference.sys.node.NodeProcessStatsExpression;
import io.crate.expression.reference.sys.node.NodeStatsContext;
import io.crate.expression.reference.sys.node.NodeStatsThreadPoolExpression;
import io.crate.expression.reference.sys.node.NodeThreadPoolsExpression;
import io.crate.expression.reference.sys.node.NodeVersionStatsExpression;
import io.crate.expression.reference.sys.node.fs.NodeFsStatsExpression;
import io.crate.expression.reference.sys.node.fs.NodeFsTotalStatsExpression;
import io.crate.expression.reference.sys.node.fs.NodeStatsFsArrayExpression;
import io.crate.expression.reference.sys.node.fs.NodeStatsFsDataExpression;
import io.crate.expression.reference.sys.node.fs.NodeStatsFsDisksExpression;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.monitor.FsInfoHelpers;
import io.crate.monitor.ThreadPools;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.StringType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.monitor.fs.FsInfo;

import java.util.Map;

public class SysNodesTableInfo extends StaticTableInfo {

    public static final String SYS_COL_NAME = "_node";

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "nodes");
    private static final ImmutableList<ColumnIdent> PRIMARY_KEY = ImmutableList.of(new ColumnIdent("id"));

    private static final RowGranularity GRANULARITY = RowGranularity.DOC;

    public static final String SYS_COL_ID = "id";
    public static final String SYS_COL_NODE_NAME = "name";
    public static final String SYS_COL_HOSTNAME = "hostname";
    public static final String SYS_COL_REST_URL = "rest_url";
    public static final String SYS_COL_PORT = "port";
    public static final String SYS_COL_LOAD = "load";
    public static final String SYS_COL_MEM = "mem";
    public static final String SYS_COL_HEAP = "heap";
    public static final String SYS_COL_VERSION = "version";
    public static final String SYS_COL_THREAD_POOLS = "thread_pools";
    public static final String SYS_COL_NETWORK = "network";
    public static final String SYS_COL_OS = "os";
    public static final String SYS_COL_OS_INFO = "os_info";
    public static final String SYS_COL_PROCESS = "process";
    public static final String SYS_COL_FS = "fs";

    private static final DataType OBJECT_ARRAY_TYPE = new ArrayType(DataTypes.OBJECT);

    public static class Columns {
        public static final ColumnIdent ID = new ColumnIdent(SYS_COL_ID);
        public static final ColumnIdent NAME = new ColumnIdent(SYS_COL_NODE_NAME);
        public static final ColumnIdent HOSTNAME = new ColumnIdent(SYS_COL_HOSTNAME);
        public static final ColumnIdent REST_URL = new ColumnIdent(SYS_COL_REST_URL);

        public static final ColumnIdent PORT = new ColumnIdent(SYS_COL_PORT);
        static final ColumnIdent PORT_HTTP = new ColumnIdent(SYS_COL_PORT, ImmutableList.of("http"));
        static final ColumnIdent PORT_TRANSPORT = new ColumnIdent(SYS_COL_PORT, ImmutableList.of("transport"));
        static final ColumnIdent PORT_PSQL = new ColumnIdent(SYS_COL_PORT, ImmutableList.of("psql"));

        public static final ColumnIdent LOAD = new ColumnIdent(SYS_COL_LOAD);
        static final ColumnIdent LOAD_1 = new ColumnIdent(SYS_COL_LOAD, ImmutableList.of("1"));
        static final ColumnIdent LOAD_5 = new ColumnIdent(SYS_COL_LOAD, ImmutableList.of("5"));
        static final ColumnIdent LOAD_15 = new ColumnIdent(SYS_COL_LOAD, ImmutableList.of("15"));
        static final ColumnIdent LOAD_PROBE_TS = new ColumnIdent(SYS_COL_LOAD, ImmutableList.of("probe_timestamp"));

        public static final ColumnIdent MEM = new ColumnIdent(SYS_COL_MEM);
        static final ColumnIdent MEM_FREE = new ColumnIdent(SYS_COL_MEM, ImmutableList.of("free"));
        static final ColumnIdent MEM_USED = new ColumnIdent(SYS_COL_MEM, ImmutableList.of("used"));
        static final ColumnIdent MEM_FREE_PERCENT = new ColumnIdent(SYS_COL_MEM, ImmutableList.of("free_percent"));
        static final ColumnIdent MEM_USED_PERCENT = new ColumnIdent(SYS_COL_MEM, ImmutableList.of("used_percent"));
        static final ColumnIdent MEM_PROBE_TS = new ColumnIdent(SYS_COL_MEM, ImmutableList.of("probe_timestamp"));

        public static final ColumnIdent HEAP = new ColumnIdent(SYS_COL_HEAP);
        static final ColumnIdent HEAP_FREE = new ColumnIdent(SYS_COL_HEAP, ImmutableList.of("free"));
        static final ColumnIdent HEAP_USED = new ColumnIdent(SYS_COL_HEAP, ImmutableList.of("used"));
        static final ColumnIdent HEAP_MAX = new ColumnIdent(SYS_COL_HEAP, ImmutableList.of("max"));
        static final ColumnIdent HEAP_PROBE_TS = new ColumnIdent(SYS_COL_HEAP, ImmutableList.of("probe_timestamp"));

        public static final ColumnIdent VERSION = new ColumnIdent(SYS_COL_VERSION);
        static final ColumnIdent VERSION_NUMBER = new ColumnIdent(SYS_COL_VERSION, ImmutableList.of("number"));
        static final ColumnIdent VERSION_BUILD_HASH = new ColumnIdent(SYS_COL_VERSION, ImmutableList.of("build_hash"));
        static final ColumnIdent VERSION_BUILD_SNAPSHOT = new ColumnIdent(SYS_COL_VERSION, ImmutableList.of("build_snapshot"));

        public static final ColumnIdent THREAD_POOLS = new ColumnIdent(SYS_COL_THREAD_POOLS);
        static final ColumnIdent THREAD_POOLS_NAME = new ColumnIdent(SYS_COL_THREAD_POOLS, ImmutableList.of("name"));
        static final ColumnIdent THREAD_POOLS_ACTIVE = new ColumnIdent(SYS_COL_THREAD_POOLS, ImmutableList.of("active"));
        static final ColumnIdent THREAD_POOLS_REJECTED = new ColumnIdent(SYS_COL_THREAD_POOLS, ImmutableList.of("rejected"));
        static final ColumnIdent THREAD_POOLS_LARGEST = new ColumnIdent(SYS_COL_THREAD_POOLS, ImmutableList.of("largest"));
        static final ColumnIdent THREAD_POOLS_COMPLETED = new ColumnIdent(SYS_COL_THREAD_POOLS, ImmutableList.of("completed"));
        static final ColumnIdent THREAD_POOLS_THREADS = new ColumnIdent(SYS_COL_THREAD_POOLS, ImmutableList.of("threads"));
        static final ColumnIdent THREAD_POOLS_QUEUE = new ColumnIdent(SYS_COL_THREAD_POOLS, ImmutableList.of("queue"));

        public static final ColumnIdent NETWORK = new ColumnIdent(SYS_COL_NETWORK);
        static final ColumnIdent NETWORK_PROBE_TS = new ColumnIdent(SYS_COL_NETWORK, ImmutableList.of("probe_timestamp"));
        static final ColumnIdent NETWORK_TCP = new ColumnIdent(SYS_COL_NETWORK, ImmutableList.of("tcp"));
        static final ColumnIdent NETWORK_TCP_CONNECTIONS = new ColumnIdent(SYS_COL_NETWORK, ImmutableList.of("tcp", "connections"));
        static final ColumnIdent NETWORK_TCP_CONNECTIONS_INITIATED = new ColumnIdent(SYS_COL_NETWORK, ImmutableList.of("tcp", "connections", "initiated"));
        static final ColumnIdent NETWORK_TCP_CONNECTIONS_ACCEPTED = new ColumnIdent(SYS_COL_NETWORK, ImmutableList.of("tcp", "connections", "accepted"));
        static final ColumnIdent NETWORK_TCP_CONNECTIONS_CURR_ESTABLISHED = new ColumnIdent(SYS_COL_NETWORK, ImmutableList.of("tcp", "connections", "curr_established"));
        static final ColumnIdent NETWORK_TCP_CONNECTIONS_DROPPED = new ColumnIdent(SYS_COL_NETWORK, ImmutableList.of("tcp", "connections", "dropped"));
        static final ColumnIdent NETWORK_TCP_CONNECTIONS_EMBRYONIC_DROPPED = new ColumnIdent(SYS_COL_NETWORK, ImmutableList.of("tcp", "connections", "embryonic_dropped"));
        static final ColumnIdent NETWORK_TCP_PACKETS = new ColumnIdent(SYS_COL_NETWORK, ImmutableList.of("tcp", "packets"));
        static final ColumnIdent NETWORK_TCP_PACKETS_SENT = new ColumnIdent(SYS_COL_NETWORK, ImmutableList.of("tcp", "packets", "sent"));
        static final ColumnIdent NETWORK_TCP_PACKETS_RECEIVED = new ColumnIdent(SYS_COL_NETWORK, ImmutableList.of("tcp", "packets", "received"));
        static final ColumnIdent NETWORK_TCP_PACKETS_RETRANSMITTED = new ColumnIdent(SYS_COL_NETWORK, ImmutableList.of("tcp", "packets", "retransmitted"));
        static final ColumnIdent NETWORK_TCP_PACKETS_ERRORS_RECEIVED = new ColumnIdent(SYS_COL_NETWORK, ImmutableList.of("tcp", "packets", "errors_received"));
        static final ColumnIdent NETWORK_TCP_PACKETS_RST_SENT = new ColumnIdent(SYS_COL_NETWORK, ImmutableList.of("tcp", "packets", "rst_sent"));

        public static final ColumnIdent OS = new ColumnIdent(SYS_COL_OS);
        static final ColumnIdent OS_UPTIME = new ColumnIdent(SYS_COL_OS, ImmutableList.of("uptime"));
        static final ColumnIdent OS_TIMESTAMP = new ColumnIdent(SYS_COL_OS, ImmutableList.of("timestamp"));
        static final ColumnIdent OS_PROBE_TS = new ColumnIdent(SYS_COL_OS, ImmutableList.of("probe_timestamp"));
        static final ColumnIdent OS_CPU = new ColumnIdent(SYS_COL_OS, ImmutableList.of("cpu"));
        static final ColumnIdent OS_CPU_SYSTEM = new ColumnIdent(SYS_COL_OS, ImmutableList.of("cpu", "system"));
        static final ColumnIdent OS_CPU_USER = new ColumnIdent(SYS_COL_OS, ImmutableList.of("cpu", "user"));
        static final ColumnIdent OS_CPU_IDLE = new ColumnIdent(SYS_COL_OS, ImmutableList.of("cpu", "idle"));
        static final ColumnIdent OS_CPU_USED = new ColumnIdent(SYS_COL_OS, ImmutableList.of("cpu", "used"));
        static final ColumnIdent OS_CPU_STOLEN = new ColumnIdent(SYS_COL_OS, ImmutableList.of("cpu", "stolen"));
        static final ColumnIdent OS_CGROUP = new ColumnIdent(SYS_COL_OS, ImmutableList.of("cgroup"));
        static final ColumnIdent OS_CGROUP_CPUACCT = new ColumnIdent(SYS_COL_OS, ImmutableList.of("cgroup", "cpuacct"));
        static final ColumnIdent OS_CGROUP_CPUACCT_CONTROL_GROUP = new ColumnIdent(SYS_COL_OS, ImmutableList.of("cgroup", "cpuacct", "control_group"));
        static final ColumnIdent OS_CGROUP_CPUACCT_USAGE_NANOS = new ColumnIdent(SYS_COL_OS, ImmutableList.of("cgroup", "cpuacct", "usage_nanos"));
        static final ColumnIdent OS_CGROUP_CPU = new ColumnIdent(SYS_COL_OS, ImmutableList.of("cgroup", "cpu"));
        static final ColumnIdent OS_CGROUP_CPU_CONTROL_GROUP = new ColumnIdent(SYS_COL_OS, ImmutableList.of("cgroup", "cpu", "control_group"));
        static final ColumnIdent OS_CGROUP_CPU_CFS_PERIOD_MICROS = new ColumnIdent(SYS_COL_OS, ImmutableList.of("cgroup", "cpu", "cfs_period_micros"));
        static final ColumnIdent OS_CGROUP_CPU_CFS_QUOTA_MICROS = new ColumnIdent(SYS_COL_OS, ImmutableList.of("cgroup", "cpu", "cfs_quota_micros"));
        static final ColumnIdent OS_CGROUP_CPU_NUM_ELAPSED_PERIODS = new ColumnIdent(SYS_COL_OS, ImmutableList.of("cgroup", "cpu", "num_elapsed_periods"));
        static final ColumnIdent OS_CGROUP_CPU_NUM_TIMES_THROTTLED = new ColumnIdent(SYS_COL_OS, ImmutableList.of("cgroup", "cpu", "num_times_throttled"));
        static final ColumnIdent OS_CGROUP_CPU_TIME_THROTTLED_NANOS = new ColumnIdent(SYS_COL_OS, ImmutableList.of("cgroup", "cpu", "time_throttled_nanos"));
        static final ColumnIdent OS_CGROUP_MEM = new ColumnIdent(SYS_COL_OS, ImmutableList.of("cgroup", "mem"));
        static final ColumnIdent OS_CGROUP_MEM_CONTROL_GROUP = new ColumnIdent(SYS_COL_OS, ImmutableList.of("cgroup", "mem", "control_group"));
        static final ColumnIdent OS_CGROUP_MEM_LIMIT_BYTES = new ColumnIdent(SYS_COL_OS, ImmutableList.of("cgroup", "mem", "limit_bytes"));
        static final ColumnIdent OS_CGROUP_MEM_USAGE_BYTES = new ColumnIdent(SYS_COL_OS, ImmutableList.of("cgroup", "mem", "usage_bytes"));

        public static final ColumnIdent OS_INFO = new ColumnIdent(SYS_COL_OS_INFO);
        static final ColumnIdent OS_INFO_AVAIL_PROCESSORS = new ColumnIdent(SYS_COL_OS_INFO, ImmutableList.of("available_processors"));
        static final ColumnIdent OS_INFO_NAME = new ColumnIdent(SYS_COL_OS_INFO, ImmutableList.of("name"));
        static final ColumnIdent OS_INFO_ARCH = new ColumnIdent(SYS_COL_OS_INFO, ImmutableList.of("arch"));
        static final ColumnIdent OS_INFO_VERSION = new ColumnIdent(SYS_COL_OS_INFO, ImmutableList.of("version"));
        public static final ColumnIdent OS_INFO_JVM = new ColumnIdent(SYS_COL_OS_INFO, ImmutableList.of("jvm"));
        static final ColumnIdent OS_INFO_JVM_VERSION = new ColumnIdent(SYS_COL_OS_INFO, ImmutableList.of("jvm", "version"));
        static final ColumnIdent OS_INFO_JVM_NAME = new ColumnIdent(SYS_COL_OS_INFO, ImmutableList.of("jvm", "vm_name"));
        static final ColumnIdent OS_INFO_JVM_VENDOR = new ColumnIdent(SYS_COL_OS_INFO, ImmutableList.of("jvm", "vm_vendor"));
        static final ColumnIdent OS_INFO_JVM_VM_VERSION = new ColumnIdent(SYS_COL_OS_INFO, ImmutableList.of("jvm", "vm_version"));

        public static final ColumnIdent PROCESS = new ColumnIdent(SYS_COL_PROCESS);
        static final ColumnIdent PROCESS_OPEN_FILE_DESCR = new ColumnIdent(SYS_COL_PROCESS, ImmutableList.of("open_file_descriptors"));
        static final ColumnIdent PROCESS_MAX_OPEN_FILE_DESCR = new ColumnIdent(SYS_COL_PROCESS, ImmutableList.of("max_open_file_descriptors"));
        static final ColumnIdent PROCESS_PROBE_TS = new ColumnIdent(SYS_COL_PROCESS, ImmutableList.of("probe_timestamp"));
        static final ColumnIdent PROCESS_CPU = new ColumnIdent(SYS_COL_PROCESS, ImmutableList.of("cpu"));
        static final ColumnIdent PROCESS_CPU_PERCENT = new ColumnIdent(SYS_COL_PROCESS, ImmutableList.of("cpu", "percent"));
        static final ColumnIdent PROCESS_CPU_USER = new ColumnIdent(SYS_COL_PROCESS, ImmutableList.of("cpu", "user"));
        static final ColumnIdent PROCESS_CPU_SYSTEM = new ColumnIdent(SYS_COL_PROCESS, ImmutableList.of("cpu", "system"));

        public static final ColumnIdent FS = new ColumnIdent(SYS_COL_FS);
        static final ColumnIdent FS_TOTAL = new ColumnIdent(SYS_COL_FS, ImmutableList.of("total"));
        static final ColumnIdent FS_TOTAL_SIZE = new ColumnIdent(SYS_COL_FS, ImmutableList.of("total", "size"));
        static final ColumnIdent FS_TOTAL_USED = new ColumnIdent(SYS_COL_FS, ImmutableList.of("total", "used"));
        static final ColumnIdent FS_TOTAL_AVAILABLE = new ColumnIdent(SYS_COL_FS, ImmutableList.of("total", "available"));
        static final ColumnIdent FS_TOTAL_READS = new ColumnIdent(SYS_COL_FS, ImmutableList.of("total", "reads"));
        static final ColumnIdent FS_TOTAL_BYTES_READ = new ColumnIdent(SYS_COL_FS, ImmutableList.of("total", "bytes_read"));
        static final ColumnIdent FS_TOTAL_WRITES = new ColumnIdent(SYS_COL_FS, ImmutableList.of("total", "writes"));
        static final ColumnIdent FS_TOTAL_BYTES_WRITTEN = new ColumnIdent(SYS_COL_FS, ImmutableList.of("total", "bytes_written"));
        static final ColumnIdent FS_DISKS = new ColumnIdent(SYS_COL_FS, ImmutableList.of("disks"));
        static final ColumnIdent FS_DISKS_DEV = new ColumnIdent(SYS_COL_FS, ImmutableList.of("disks", "dev"));
        static final ColumnIdent FS_DISKS_SIZE = new ColumnIdent(SYS_COL_FS, ImmutableList.of("disks", "size"));
        static final ColumnIdent FS_DISKS_USED = new ColumnIdent(SYS_COL_FS, ImmutableList.of("disks", "used"));
        static final ColumnIdent FS_DISKS_AVAILABLE = new ColumnIdent(SYS_COL_FS, ImmutableList.of("disks", "available"));
        static final ColumnIdent FS_DISKS_READS = new ColumnIdent(SYS_COL_FS, ImmutableList.of("disks", "reads"));
        static final ColumnIdent FS_DISKS_BYTES_READ = new ColumnIdent(SYS_COL_FS, ImmutableList.of("disks", "bytes_read"));
        static final ColumnIdent FS_DISKS_WRITES = new ColumnIdent(SYS_COL_FS, ImmutableList.of("disks", "writes"));
        static final ColumnIdent FS_DISKS_BYTES_WRITTEN = new ColumnIdent(SYS_COL_FS, ImmutableList.of("disks", "bytes_written"));
        static final ColumnIdent FS_DATA = new ColumnIdent(SYS_COL_FS, ImmutableList.of("data"));
        static final ColumnIdent FS_DATA_DEV = new ColumnIdent(SYS_COL_FS, ImmutableList.of("data", "dev"));
        static final ColumnIdent FS_DATA_PATH = new ColumnIdent(SYS_COL_FS, ImmutableList.of("data", "path"));
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<NodeStatsContext>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<NodeStatsContext>>builder()
            .put(SysNodesTableInfo.Columns.ID,
                () -> RowContextCollectorExpression.objToBytesRef(NodeStatsContext::id))
            .put(SysNodesTableInfo.Columns.NAME,
                () -> RowContextCollectorExpression.objToBytesRef(NodeStatsContext::name))
            .put(SysNodesTableInfo.Columns.HOSTNAME,
                () -> RowContextCollectorExpression.objToBytesRef(r -> r.isComplete() ? r.hostname() : null))
            .put(SysNodesTableInfo.Columns.REST_URL,
                () -> RowContextCollectorExpression.objToBytesRef(r -> r.isComplete() ? r.restUrl() : null))
            .put(SysNodesTableInfo.Columns.PORT, NodePortStatsExpression::new)
            .put(SysNodesTableInfo.Columns.LOAD, NodeLoadStatsExpression::new)
            .put(SysNodesTableInfo.Columns.MEM, NodeMemoryStatsExpression::new)
            .put(SysNodesTableInfo.Columns.HEAP, NodeHeapStatsExpression::new)
            .put(SysNodesTableInfo.Columns.VERSION, NodeVersionStatsExpression::new)
            .put(SysNodesTableInfo.Columns.THREAD_POOLS, NodeThreadPoolsExpression::new)
            .put(SysNodesTableInfo.Columns.THREAD_POOLS_NAME, () -> new NodeStatsThreadPoolExpression<BytesRef>() {
                @Override
                protected BytesRef valueForItem(Map.Entry<BytesRef, ThreadPools.ThreadPoolExecutorContext> input) {
                    return input.getKey();
                }
            })
            .put(SysNodesTableInfo.Columns.THREAD_POOLS_ACTIVE, () -> new NodeStatsThreadPoolExpression<Integer>() {
                @Override
                protected Integer valueForItem(Map.Entry<BytesRef, ThreadPools.ThreadPoolExecutorContext> input) {
                    return input.getValue().activeCount();
                }
            })
            .put(SysNodesTableInfo.Columns.THREAD_POOLS_REJECTED, () -> new NodeStatsThreadPoolExpression<Long>() {
                @Override
                protected Long valueForItem(Map.Entry<BytesRef, ThreadPools.ThreadPoolExecutorContext> input) {
                    return input.getValue().rejectedCount();
                }
            })
            .put(SysNodesTableInfo.Columns.THREAD_POOLS_LARGEST, () -> new NodeStatsThreadPoolExpression<Integer>() {
                @Override
                protected Integer valueForItem(Map.Entry<BytesRef, ThreadPools.ThreadPoolExecutorContext> input) {
                    return input.getValue().largestPoolSize();
                }
            })
            .put(SysNodesTableInfo.Columns.THREAD_POOLS_COMPLETED, () -> new NodeStatsThreadPoolExpression<Long>() {
                @Override
                protected Long valueForItem(Map.Entry<BytesRef, ThreadPools.ThreadPoolExecutorContext> input) {
                    return input.getValue().completedTaskCount();
                }
            })
            .put(SysNodesTableInfo.Columns.THREAD_POOLS_THREADS, () -> new NodeStatsThreadPoolExpression<Integer>() {
                @Override
                protected Integer valueForItem(Map.Entry<BytesRef, ThreadPools.ThreadPoolExecutorContext> input) {
                    return input.getValue().poolSize();
                }
            })
            .put(SysNodesTableInfo.Columns.THREAD_POOLS_QUEUE, () -> new NodeStatsThreadPoolExpression<Integer>() {
                @Override
                protected Integer valueForItem(Map.Entry<BytesRef, ThreadPools.ThreadPoolExecutorContext> input) {
                    return input.getValue().queueSize();
                }
            })
            .put(SysNodesTableInfo.Columns.NETWORK, NodeNetworkStatsExpression::new)
            .put(SysNodesTableInfo.Columns.OS, NodeOsStatsExpression::new)
            .put(SysNodesTableInfo.Columns.OS_INFO, NodeOsInfoStatsExpression::new)
            .put(SysNodesTableInfo.Columns.PROCESS, NodeProcessStatsExpression::new)
            .put(SysNodesTableInfo.Columns.FS, NodeFsStatsExpression::new)
            .put(SysNodesTableInfo.Columns.FS_TOTAL, NodeFsTotalStatsExpression::new)
            .put(SysNodesTableInfo.Columns.FS_TOTAL_SIZE,
                () -> RowContextCollectorExpression.forFunction(r -> r.isComplete() ? FsInfoHelpers.Path.size(r.fsInfo().getTotal()) : null))
            .put(SysNodesTableInfo.Columns.FS_TOTAL_USED,
                () -> RowContextCollectorExpression.forFunction(r -> r.isComplete() ? FsInfoHelpers.Path.used(r.fsInfo().getTotal()) : null))
            .put(SysNodesTableInfo.Columns.FS_TOTAL_AVAILABLE,
                () -> RowContextCollectorExpression.forFunction(r -> r.isComplete() ? FsInfoHelpers.Path.available(r.fsInfo().getTotal()) : null))
            .put(SysNodesTableInfo.Columns.FS_TOTAL_READS,
                () -> RowContextCollectorExpression.forFunction(r -> r.isComplete() ? FsInfoHelpers.Stats.readOperations(r.fsInfo().getIoStats()) : null))
            .put(SysNodesTableInfo.Columns.FS_TOTAL_BYTES_READ,
                () -> RowContextCollectorExpression.forFunction(r -> r.isComplete() ? FsInfoHelpers.Stats.bytesRead(r.fsInfo().getIoStats()) : null))
            .put(SysNodesTableInfo.Columns.FS_TOTAL_WRITES,
                () -> RowContextCollectorExpression.forFunction(r -> r.isComplete() ? FsInfoHelpers.Stats.writeOperations(r.fsInfo().getIoStats()) : null))
            .put(SysNodesTableInfo.Columns.FS_TOTAL_BYTES_WRITTEN,
                () -> RowContextCollectorExpression.forFunction(r -> r.isComplete() ? FsInfoHelpers.Stats.bytesWritten(r.fsInfo().getIoStats()) : null))
            .put(SysNodesTableInfo.Columns.FS_DISKS, NodeStatsFsDisksExpression::new)
            .put(SysNodesTableInfo.Columns.FS_DISKS_DEV, () -> new NodeStatsFsArrayExpression<BytesRef>() {
                @Override
                protected BytesRef valueForItem(FsInfo.Path input) {
                    return BytesRefs.toBytesRef(FsInfoHelpers.Path.dev(input));
                }
            })
            .put(SysNodesTableInfo.Columns.FS_DISKS_SIZE, () -> new NodeStatsFsArrayExpression<Long>() {
                @Override
                protected Long valueForItem(FsInfo.Path input) {
                    return FsInfoHelpers.Path.size(input);
                }
            })
            .put(SysNodesTableInfo.Columns.FS_DISKS_USED, () -> new NodeStatsFsArrayExpression<Long>() {
                @Override
                protected Long valueForItem(FsInfo.Path input) {
                    return FsInfoHelpers.Path.used(input);
                }
            })
            .put(SysNodesTableInfo.Columns.FS_DISKS_AVAILABLE, () -> new NodeStatsFsArrayExpression<Long>() {
                @Override
                protected Long valueForItem(FsInfo.Path input) {
                    return FsInfoHelpers.Path.available(input);
                }
            })
            .put(SysNodesTableInfo.Columns.FS_DISKS_READS, () -> new NodeStatsFsArrayExpression<Long>() {
                @Override
                protected Long valueForItem(FsInfo.Path input) {
                    return -1L;
                }
            })
            .put(SysNodesTableInfo.Columns.FS_DISKS_BYTES_READ, () -> new NodeStatsFsArrayExpression<Long>() {
                @Override
                protected Long valueForItem(FsInfo.Path input) {
                    return -1L;
                }
            })
            .put(SysNodesTableInfo.Columns.FS_DISKS_WRITES, () -> new NodeStatsFsArrayExpression<Long>() {
                @Override
                protected Long valueForItem(FsInfo.Path input) {
                    return -1L;
                }
            })
            .put(SysNodesTableInfo.Columns.FS_DISKS_BYTES_WRITTEN, () -> new NodeStatsFsArrayExpression<Long>() {
                @Override
                protected Long valueForItem(FsInfo.Path input) {
                    return -1L;
                }
            })
            .put(SysNodesTableInfo.Columns.FS_DATA, NodeStatsFsDataExpression::new)
            .put(SysNodesTableInfo.Columns.FS_DATA_DEV, () -> new NodeStatsFsArrayExpression<BytesRef>() {
                @Override
                protected BytesRef valueForItem(FsInfo.Path input) {
                    return BytesRefs.toBytesRef(FsInfoHelpers.Path.dev(input));
                }
            })
            .put(SysNodesTableInfo.Columns.FS_DATA_PATH, () -> new NodeStatsFsArrayExpression<BytesRef>() {
                @Override
                protected BytesRef valueForItem(FsInfo.Path input) {
                    return BytesRefs.toBytesRef(input.getPath());
                }
            })
            .build();
    }

    public SysNodesTableInfo() {
        super(IDENT, new ColumnRegistrar(IDENT, GRANULARITY)
                .register(Columns.ID, DataTypes.STRING)
                .register(Columns.NAME, DataTypes.STRING)
                .register(Columns.HOSTNAME, DataTypes.STRING)
                .register(Columns.REST_URL, DataTypes.STRING)

                .register(Columns.PORT, DataTypes.OBJECT)
                .register(Columns.PORT_HTTP, DataTypes.INTEGER)
                .register(Columns.PORT_TRANSPORT, DataTypes.INTEGER)
                .register(Columns.PORT_PSQL, DataTypes.INTEGER)

                .register(Columns.LOAD, DataTypes.OBJECT)
                .register(Columns.LOAD_1, DataTypes.DOUBLE)
                .register(Columns.LOAD_5, DataTypes.DOUBLE)
                .register(Columns.LOAD_15, DataTypes.DOUBLE)
                .register(Columns.LOAD_PROBE_TS, DataTypes.TIMESTAMP)

                .register(Columns.MEM, DataTypes.OBJECT)
                .register(Columns.MEM_FREE, DataTypes.LONG)
                .register(Columns.MEM_USED, DataTypes.LONG)
                .register(Columns.MEM_FREE_PERCENT, DataTypes.SHORT)
                .register(Columns.MEM_USED_PERCENT, DataTypes.SHORT)
                .register(Columns.MEM_PROBE_TS, DataTypes.TIMESTAMP)

                .register(Columns.HEAP, DataTypes.OBJECT)
                .register(Columns.HEAP_FREE, DataTypes.LONG)
                .register(Columns.HEAP_USED, DataTypes.LONG)
                .register(Columns.HEAP_MAX, DataTypes.LONG)
                .register(Columns.HEAP_PROBE_TS, DataTypes.TIMESTAMP)

                .register(Columns.VERSION, DataTypes.OBJECT)
                .register(Columns.VERSION_NUMBER, StringType.INSTANCE)
                .register(Columns.VERSION_BUILD_HASH, StringType.INSTANCE)
                .register(Columns.VERSION_BUILD_SNAPSHOT, DataTypes.BOOLEAN)

                .register(Columns.THREAD_POOLS, OBJECT_ARRAY_TYPE)
                .register(Columns.THREAD_POOLS_NAME, DataTypes.STRING)
                .register(Columns.THREAD_POOLS_ACTIVE, DataTypes.INTEGER)
                .register(Columns.THREAD_POOLS_REJECTED, DataTypes.LONG)
                .register(Columns.THREAD_POOLS_LARGEST, DataTypes.INTEGER)
                .register(Columns.THREAD_POOLS_COMPLETED, DataTypes.LONG)
                .register(Columns.THREAD_POOLS_THREADS, DataTypes.INTEGER)
                .register(Columns.THREAD_POOLS_QUEUE, DataTypes.INTEGER)

                .register(Columns.NETWORK, DataTypes.OBJECT)
                .register(Columns.NETWORK_PROBE_TS, DataTypes.TIMESTAMP)
                .register(Columns.NETWORK_TCP, DataTypes.OBJECT)
                .register(Columns.NETWORK_TCP_CONNECTIONS, DataTypes.OBJECT)
                .register(Columns.NETWORK_TCP_CONNECTIONS_INITIATED, DataTypes.LONG)
                .register(Columns.NETWORK_TCP_CONNECTIONS_ACCEPTED, DataTypes.LONG)
                .register(Columns.NETWORK_TCP_CONNECTIONS_CURR_ESTABLISHED, DataTypes.LONG)
                .register(Columns.NETWORK_TCP_CONNECTIONS_DROPPED, DataTypes.LONG)
                .register(Columns.NETWORK_TCP_CONNECTIONS_EMBRYONIC_DROPPED, DataTypes.LONG)
                .register(Columns.NETWORK_TCP_PACKETS, DataTypes.OBJECT)
                .register(Columns.NETWORK_TCP_PACKETS_SENT, DataTypes.LONG)
                .register(Columns.NETWORK_TCP_PACKETS_RECEIVED, DataTypes.LONG)
                .register(Columns.NETWORK_TCP_PACKETS_RETRANSMITTED, DataTypes.LONG)
                .register(Columns.NETWORK_TCP_PACKETS_ERRORS_RECEIVED, DataTypes.LONG)
                .register(Columns.NETWORK_TCP_PACKETS_RST_SENT, DataTypes.LONG)

                .register(Columns.OS, DataTypes.OBJECT)
                .register(Columns.OS_UPTIME, DataTypes.LONG)
                .register(Columns.OS_TIMESTAMP, DataTypes.TIMESTAMP)
                .register(Columns.OS_PROBE_TS, DataTypes.TIMESTAMP)
                .register(Columns.OS_CPU, DataTypes.OBJECT)
                .register(Columns.OS_CPU_SYSTEM, DataTypes.SHORT)
                .register(Columns.OS_CPU_USER, DataTypes.SHORT)
                .register(Columns.OS_CPU_IDLE, DataTypes.SHORT)
                .register(Columns.OS_CPU_USED, DataTypes.SHORT)
                .register(Columns.OS_CPU_STOLEN, DataTypes.SHORT)
                .register(Columns.OS_CGROUP, DataTypes.OBJECT)
                .register(Columns.OS_CGROUP_CPUACCT, DataTypes.OBJECT)
                .register(Columns.OS_CGROUP_CPUACCT_CONTROL_GROUP, DataTypes.STRING)
                .register(Columns.OS_CGROUP_CPUACCT_USAGE_NANOS, DataTypes.LONG)
                .register(Columns.OS_CGROUP_CPU, DataTypes.OBJECT)
                .register(Columns.OS_CGROUP_CPU_CONTROL_GROUP, DataTypes.STRING)
                .register(Columns.OS_CGROUP_CPU_CFS_PERIOD_MICROS, DataTypes.LONG)
                .register(Columns.OS_CGROUP_CPU_CFS_QUOTA_MICROS, DataTypes.LONG)
                .register(Columns.OS_CGROUP_CPU_NUM_ELAPSED_PERIODS, DataTypes.LONG)
                .register(Columns.OS_CGROUP_CPU_NUM_TIMES_THROTTLED, DataTypes.LONG)
                .register(Columns.OS_CGROUP_CPU_TIME_THROTTLED_NANOS, DataTypes.LONG)
                .register(Columns.OS_CGROUP_MEM, DataTypes.OBJECT)
                .register(Columns.OS_CGROUP_MEM_CONTROL_GROUP, DataTypes.STRING)
                .register(Columns.OS_CGROUP_MEM_LIMIT_BYTES, DataTypes.STRING)
                .register(Columns.OS_CGROUP_MEM_USAGE_BYTES, DataTypes.STRING)

                .register(Columns.OS_INFO, DataTypes.OBJECT)
                .register(Columns.OS_INFO_AVAIL_PROCESSORS, DataTypes.INTEGER)
                .register(Columns.OS_INFO_NAME, DataTypes.STRING)
                .register(Columns.OS_INFO_ARCH, DataTypes.STRING)
                .register(Columns.OS_INFO_VERSION, DataTypes.STRING)
                .register(Columns.OS_INFO_JVM, DataTypes.OBJECT)
                .register(Columns.OS_INFO_JVM_VERSION, DataTypes.STRING)
                .register(Columns.OS_INFO_JVM_NAME, DataTypes.STRING)
                .register(Columns.OS_INFO_JVM_VENDOR, DataTypes.STRING)
                .register(Columns.OS_INFO_JVM_VM_VERSION, DataTypes.STRING)

                .register(Columns.PROCESS, DataTypes.OBJECT)
                .register(Columns.PROCESS_OPEN_FILE_DESCR, DataTypes.LONG)
                .register(Columns.PROCESS_MAX_OPEN_FILE_DESCR, DataTypes.LONG)
                .register(Columns.PROCESS_PROBE_TS, DataTypes.TIMESTAMP)
                .register(Columns.PROCESS_CPU, DataTypes.OBJECT)
                .register(Columns.PROCESS_CPU_PERCENT, DataTypes.SHORT)
                .register(Columns.PROCESS_CPU_USER, DataTypes.LONG)
                .register(Columns.PROCESS_CPU_SYSTEM, DataTypes.LONG)

                .register(Columns.FS, DataTypes.OBJECT)
                .register(Columns.FS_TOTAL, DataTypes.OBJECT)
                .register(Columns.FS_TOTAL_SIZE, DataTypes.LONG)
                .register(Columns.FS_TOTAL_USED, DataTypes.LONG)
                .register(Columns.FS_TOTAL_AVAILABLE, DataTypes.LONG)
                .register(Columns.FS_TOTAL_READS, DataTypes.LONG)
                .register(Columns.FS_TOTAL_BYTES_READ, DataTypes.LONG)
                .register(Columns.FS_TOTAL_WRITES, DataTypes.LONG)
                .register(Columns.FS_TOTAL_BYTES_WRITTEN, DataTypes.LONG)
                .register(Columns.FS_DISKS, OBJECT_ARRAY_TYPE)
                .register(Columns.FS_DISKS_DEV, DataTypes.STRING)
                .register(Columns.FS_DISKS_SIZE, DataTypes.LONG)
                .register(Columns.FS_DISKS_USED, DataTypes.LONG)
                .register(Columns.FS_DISKS_AVAILABLE, DataTypes.LONG)
                .register(Columns.FS_DISKS_READS, DataTypes.LONG)
                .register(Columns.FS_DISKS_BYTES_READ, DataTypes.LONG)
                .register(Columns.FS_DISKS_WRITES, DataTypes.LONG)
                .register(Columns.FS_DISKS_BYTES_WRITTEN, DataTypes.LONG)
                .register(Columns.FS_DATA, OBJECT_ARRAY_TYPE)
                .register(Columns.FS_DATA_DEV, DataTypes.STRING)
                .register(Columns.FS_DATA_PATH, DataTypes.STRING),
            PRIMARY_KEY);
    }

    @Override
    public RowGranularity rowGranularity() {
        return GRANULARITY;
    }

    @Override
    public Routing getRouting(ClusterState clusterState,
                              RoutingProvider routingProvider,
                              WhereClause whereClause,
                              RoutingProvider.ShardSelection shardSelection,
                              SessionContext sessionContext) {
        return Routing.forTableOnSingleNode(IDENT, clusterState.getNodes().getLocalNodeId());
    }
}
