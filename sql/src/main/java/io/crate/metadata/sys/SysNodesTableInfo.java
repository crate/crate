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
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.reference.ObjectCollectExpression;
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
import io.crate.expression.reference.sys.node.SimpleNodeStatsExpression;
import io.crate.expression.reference.sys.node.fs.NodeFsStatsExpression;
import io.crate.expression.reference.sys.node.fs.NodeFsTotalStatsExpression;
import io.crate.expression.reference.sys.node.fs.NodeStatsFsArrayExpression;
import io.crate.expression.reference.sys.node.fs.NodeStatsFsDataExpression;
import io.crate.expression.reference.sys.node.fs.NodeStatsFsDisksExpression;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.monitor.FsInfoHelpers;
import io.crate.protocols.ConnectionStats;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.threadpool.ThreadPoolStats;

import java.util.Map;

import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;

public class SysNodesTableInfo extends StaticTableInfo {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "nodes");
    private static final ImmutableList<ColumnIdent> PRIMARY_KEY = ImmutableList.of(new ColumnIdent("id"));

    private static final RowGranularity GRANULARITY = RowGranularity.DOC;

    private static final String SYS_COL_ID = "id";
    private static final String SYS_COL_NODE_NAME = "name";
    private static final String SYS_COL_HOSTNAME = "hostname";
    private static final String SYS_COL_REST_URL = "rest_url";
    private static final String SYS_COL_PORT = "port";
    private static final String SYS_COL_CLUSTER_STATE_VERSION = "cluster_state_version";
    private static final String SYS_COL_LOAD = "load";
    private static final String SYS_COL_MEM = "mem";
    private static final String SYS_COL_HEAP = "heap";
    private static final String SYS_COL_VERSION = "version";
    private static final String SYS_COL_THREAD_POOLS = "thread_pools";
    private static final String SYS_COL_NETWORK = "network";
    private static final String SYS_COL_OS = "os";
    private static final String SYS_COL_OS_INFO = "os_info";
    private static final String SYS_COL_PROCESS = "process";
    private static final String SYS_COL_FS = "fs";

    public static class Columns {
        public static final ColumnIdent ID = new ColumnIdent(SYS_COL_ID);
        public static final ColumnIdent NAME = new ColumnIdent(SYS_COL_NODE_NAME);
        public static final ColumnIdent HOSTNAME = new ColumnIdent(SYS_COL_HOSTNAME);
        public static final ColumnIdent REST_URL = new ColumnIdent(SYS_COL_REST_URL);

        public static final ColumnIdent PORT = new ColumnIdent(SYS_COL_PORT);
        public static final ColumnIdent CLUSTER_STATE_VERSION = new ColumnIdent(SYS_COL_CLUSTER_STATE_VERSION);

        public static final ColumnIdent LOAD = new ColumnIdent(SYS_COL_LOAD);

        public static final ColumnIdent MEM = new ColumnIdent(SYS_COL_MEM);

        public static final ColumnIdent HEAP = new ColumnIdent(SYS_COL_HEAP);

        public static final ColumnIdent VERSION = new ColumnIdent(SYS_COL_VERSION);

        public static final ColumnIdent THREAD_POOLS = new ColumnIdent(SYS_COL_THREAD_POOLS);
        static final ColumnIdent THREAD_POOLS_NAME = new ColumnIdent(SYS_COL_THREAD_POOLS, ImmutableList.of("name"));
        static final ColumnIdent THREAD_POOLS_ACTIVE = new ColumnIdent(SYS_COL_THREAD_POOLS, ImmutableList.of("active"));
        static final ColumnIdent THREAD_POOLS_REJECTED = new ColumnIdent(SYS_COL_THREAD_POOLS, ImmutableList.of("rejected"));
        static final ColumnIdent THREAD_POOLS_LARGEST = new ColumnIdent(SYS_COL_THREAD_POOLS, ImmutableList.of("largest"));
        static final ColumnIdent THREAD_POOLS_COMPLETED = new ColumnIdent(SYS_COL_THREAD_POOLS, ImmutableList.of("completed"));
        static final ColumnIdent THREAD_POOLS_THREADS = new ColumnIdent(SYS_COL_THREAD_POOLS, ImmutableList.of("threads"));
        static final ColumnIdent THREAD_POOLS_QUEUE = new ColumnIdent(SYS_COL_THREAD_POOLS, ImmutableList.of("queue"));

        public static final ColumnIdent NETWORK = new ColumnIdent(SYS_COL_NETWORK);

        public static final ColumnIdent CONNECTIONS = new ColumnIdent("connections");
        static final ColumnIdent CONNECTIONS_HTTP = ColumnIdent.getChild(CONNECTIONS, "http");
        static final ColumnIdent CONNECTIONS_HTTP_OPEN = ColumnIdent.getChild(CONNECTIONS_HTTP, "open");
        static final ColumnIdent CONNECTIONS_HTTP_TOTAL = ColumnIdent.getChild(CONNECTIONS_HTTP, "total");
        static final ColumnIdent CONNECTIONS_PSQL = ColumnIdent.getChild(CONNECTIONS, "psql");
        static final ColumnIdent CONNECTIONS_PSQL_OPEN = ColumnIdent.getChild(CONNECTIONS_PSQL, "open");
        static final ColumnIdent CONNECTIONS_PSQL_TOTAL = ColumnIdent.getChild(CONNECTIONS_PSQL, "total");
        static final ColumnIdent CONNECTIONS_TRANSPORT = ColumnIdent.getChild(CONNECTIONS, "transport");
        static final ColumnIdent CONNECTIONS_TRANSPORT_OPEN = ColumnIdent.getChild(CONNECTIONS_TRANSPORT, "open");

        public static final ColumnIdent OS = new ColumnIdent(SYS_COL_OS);

        public static final ColumnIdent OS_INFO = new ColumnIdent(SYS_COL_OS_INFO);

        public static final ColumnIdent PROCESS = new ColumnIdent(SYS_COL_PROCESS);

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
            .put(Columns.ID,
                () -> NestableCollectExpression.forFunction(NodeStatsContext::id))
            .put(Columns.NAME,
                () -> NestableCollectExpression.forFunction(NodeStatsContext::name))
            .put(Columns.HOSTNAME,
                () -> NestableCollectExpression.forFunction(r -> r.isComplete() ? r.hostname() : null))
            .put(Columns.REST_URL,
                () -> NestableCollectExpression.forFunction(r -> r.isComplete() ? r.restUrl() : null))
            .put(Columns.PORT, NodePortStatsExpression::new)
            .put(Columns.LOAD, NodeLoadStatsExpression::new)
            .put(Columns.MEM, NodeMemoryStatsExpression::new)
            .put(Columns.HEAP, NodeHeapStatsExpression::new)
            .put(Columns.VERSION, NodeVersionStatsExpression::new)
            .put(Columns.CLUSTER_STATE_VERSION, () -> new SimpleNodeStatsExpression<Long>() {
                @Override
                public Long innerValue(NodeStatsContext nodeStatsContext) {
                    return nodeStatsContext.clusterStateVersion();
                }
            })
            .put(Columns.THREAD_POOLS, NodeThreadPoolsExpression::new)
            .put(Columns.THREAD_POOLS_NAME, () -> new NodeStatsThreadPoolExpression<String>() {
                @Override
                protected String valueForItem(ThreadPoolStats.Stats input) {
                    return input.getName();
                }
            })
            .put(Columns.THREAD_POOLS_ACTIVE, () -> new NodeStatsThreadPoolExpression<Integer>() {
                @Override
                protected Integer valueForItem(ThreadPoolStats.Stats stats) {
                    return stats.getActive();
                }
            })
            .put(Columns.THREAD_POOLS_REJECTED, () -> new NodeStatsThreadPoolExpression<Long>() {
                @Override
                protected Long valueForItem(ThreadPoolStats.Stats stats) {
                    return stats.getRejected();
                }
            })
            .put(Columns.THREAD_POOLS_LARGEST, () -> new NodeStatsThreadPoolExpression<Integer>() {
                @Override
                protected Integer valueForItem(ThreadPoolStats.Stats stats) {
                    return stats.getLargest();
                }
            })
            .put(Columns.THREAD_POOLS_COMPLETED, () -> new NodeStatsThreadPoolExpression<Long>() {
                @Override
                protected Long valueForItem(ThreadPoolStats.Stats stats) {
                    return stats.getCompleted();
                }
            })
            .put(Columns.THREAD_POOLS_THREADS, () -> new NodeStatsThreadPoolExpression<Integer>() {
                @Override
                protected Integer valueForItem(ThreadPoolStats.Stats stats) {
                    return stats.getThreads();
                }
            })
            .put(Columns.THREAD_POOLS_QUEUE, () -> new NodeStatsThreadPoolExpression<Integer>() {
                @Override
                protected Integer valueForItem(ThreadPoolStats.Stats stats) {
                    return stats.getQueue();
                }
            })
            .put(Columns.NETWORK, NodeNetworkStatsExpression::new)
            .put(Columns.OS, NodeOsStatsExpression::new)
            .put(Columns.OS_INFO, NodeOsInfoStatsExpression::new)
            .put(Columns.PROCESS, NodeProcessStatsExpression::new)
            .put(Columns.FS, NodeFsStatsExpression::new)
            .put(Columns.FS_TOTAL, NodeFsTotalStatsExpression::new)
            .put(Columns.FS_TOTAL_SIZE,
                () -> forFunction(r -> r.isComplete() ? FsInfoHelpers.Path.size(r.fsInfo().getTotal()) : null))
            .put(Columns.FS_TOTAL_USED,
                () -> forFunction(r -> r.isComplete() ? FsInfoHelpers.Path.used(r.fsInfo().getTotal()) : null))
            .put(Columns.FS_TOTAL_AVAILABLE,
                () -> forFunction(r -> r.isComplete() ? FsInfoHelpers.Path.available(r.fsInfo().getTotal()) : null))
            .put(Columns.FS_TOTAL_READS,
                () -> forFunction(r -> r.isComplete() ? FsInfoHelpers.Stats.readOperations(r.fsInfo().getIoStats()) : null))
            .put(Columns.FS_TOTAL_BYTES_READ,
                () -> forFunction(r -> r.isComplete() ? FsInfoHelpers.Stats.bytesRead(r.fsInfo().getIoStats()) : null))
            .put(Columns.FS_TOTAL_WRITES,
                () -> forFunction(r -> r.isComplete() ? FsInfoHelpers.Stats.writeOperations(r.fsInfo().getIoStats()) : null))
            .put(Columns.FS_TOTAL_BYTES_WRITTEN,
                () -> forFunction(r -> r.isComplete() ? FsInfoHelpers.Stats.bytesWritten(r.fsInfo().getIoStats()) : null))
            .put(Columns.FS_DISKS, NodeStatsFsDisksExpression::new)
            .put(Columns.FS_DISKS_DEV, () -> new NodeStatsFsArrayExpression<String>() {
                @Override
                protected String valueForItem(FsInfo.Path input) {
                    return FsInfoHelpers.Path.dev(input);
                }
            })
            .put(Columns.FS_DISKS_SIZE, () -> new NodeStatsFsArrayExpression<Long>() {
                @Override
                protected Long valueForItem(FsInfo.Path input) {
                    return FsInfoHelpers.Path.size(input);
                }
            })
            .put(Columns.FS_DISKS_USED, () -> new NodeStatsFsArrayExpression<Long>() {
                @Override
                protected Long valueForItem(FsInfo.Path input) {
                    return FsInfoHelpers.Path.used(input);
                }
            })
            .put(Columns.FS_DISKS_AVAILABLE, () -> new NodeStatsFsArrayExpression<Long>() {
                @Override
                protected Long valueForItem(FsInfo.Path input) {
                    return FsInfoHelpers.Path.available(input);
                }
            })
            .put(Columns.FS_DISKS_READS, () -> new NodeStatsFsArrayExpression<Long>() {
                @Override
                protected Long valueForItem(FsInfo.Path input) {
                    return -1L;
                }
            })
            .put(Columns.FS_DISKS_BYTES_READ, () -> new NodeStatsFsArrayExpression<Long>() {
                @Override
                protected Long valueForItem(FsInfo.Path input) {
                    return -1L;
                }
            })
            .put(Columns.FS_DISKS_WRITES, () -> new NodeStatsFsArrayExpression<Long>() {
                @Override
                protected Long valueForItem(FsInfo.Path input) {
                    return -1L;
                }
            })
            .put(Columns.FS_DISKS_BYTES_WRITTEN, () -> new NodeStatsFsArrayExpression<Long>() {
                @Override
                protected Long valueForItem(FsInfo.Path input) {
                    return -1L;
                }
            })
            .put(Columns.FS_DATA, NodeStatsFsDataExpression::new)
            .put(Columns.FS_DATA_DEV, () -> new NodeStatsFsArrayExpression<String>() {
                @Override
                protected String valueForItem(FsInfo.Path input) {
                    return FsInfoHelpers.Path.dev(input);
                }
            })
            .put(Columns.FS_DATA_PATH, () -> new NodeStatsFsArrayExpression<String>() {
                @Override
                protected String valueForItem(FsInfo.Path input) {
                    return input.getPath();
                }
            })
            .put(Columns.CONNECTIONS, SysNodesTableInfo::createConnectionsExpression)
            .build();
    }

    private static ObjectCollectExpression<NodeStatsContext> createConnectionsExpression() {
        return new ObjectCollectExpression<>(
            ImmutableMap.of(
                Columns.CONNECTIONS_HTTP.path().get(0),
                new ObjectCollectExpression<NodeStatsContext>(
                    ImmutableMap.of(
                        Columns.CONNECTIONS_HTTP_OPEN.path().get(1),
                        NestableCollectExpression.<NodeStatsContext, HttpStats>withNullableProperty(
                            NodeStatsContext::httpStats,
                            HttpStats::getServerOpen),
                        Columns.CONNECTIONS_HTTP_TOTAL.path().get(1),
                        NestableCollectExpression.<NodeStatsContext, HttpStats>withNullableProperty(
                            NodeStatsContext::httpStats,
                            HttpStats::getTotalOpen)
                    )
                ),
                Columns.CONNECTIONS_PSQL.path().get(0),
                new ObjectCollectExpression<NodeStatsContext>(
                    ImmutableMap.of(
                        Columns.CONNECTIONS_PSQL_OPEN.path().get(1),
                        NestableCollectExpression.<NodeStatsContext, ConnectionStats>withNullableProperty(
                            NodeStatsContext::psqlStats,
                            ConnectionStats::open),
                        Columns.CONNECTIONS_PSQL_TOTAL.path().get(1),
                        NestableCollectExpression.<NodeStatsContext, ConnectionStats>withNullableProperty(
                            NodeStatsContext::psqlStats,
                            ConnectionStats::total)
                    )
                ),
                Columns.CONNECTIONS_TRANSPORT.path().get(0),
                new ObjectCollectExpression<NodeStatsContext>(
                    ImmutableMap.of(
                        Columns.CONNECTIONS_TRANSPORT_OPEN.path().get(1),
                        forFunction(NodeStatsContext::openTransportConnections)
                    )
                )
            )
        );
    }

    public SysNodesTableInfo() {
        super(IDENT, new ColumnRegistrar(IDENT, GRANULARITY)
                .register(Columns.ID, DataTypes.STRING)
                .register(Columns.NAME, DataTypes.STRING)
                .register(Columns.HOSTNAME, DataTypes.STRING)
                .register(Columns.REST_URL, DataTypes.STRING)

                .register(Columns.PORT, ObjectType.builder()
                    .setInnerType("http", DataTypes.INTEGER)
                    .setInnerType("transport", DataTypes.INTEGER)
                    .setInnerType("psql", DataTypes.INTEGER)
                    .build())

                .register(Columns.LOAD, ObjectType.builder()
                    .setInnerType("1", DataTypes.DOUBLE)
                    .setInnerType("5", DataTypes.DOUBLE)
                    .setInnerType("15", DataTypes.DOUBLE)
                    .setInnerType("probe_timestamp", DataTypes.TIMESTAMPZ)
                    .build())

                .register(Columns.MEM, ObjectType.builder()
                    .setInnerType("free", DataTypes.LONG)
                    .setInnerType("used", DataTypes.LONG)
                    .setInnerType("free_percent", DataTypes.SHORT)
                    .setInnerType("used_percent", DataTypes.SHORT)
                    .setInnerType("probe_timestamp", DataTypes.TIMESTAMPZ)
                    .build())

                .register(Columns.HEAP, ObjectType.builder()
                    .setInnerType("free", DataTypes.LONG)
                    .setInnerType("used", DataTypes.LONG)
                    .setInnerType("max", DataTypes.LONG)
                    .setInnerType("probe_timestamp", DataTypes.TIMESTAMPZ)
                    .build())

                .register(Columns.VERSION, ObjectType.builder()
                    .setInnerType("number", DataTypes.STRING)
                    .setInnerType("build_hash", DataTypes.STRING)
                    .setInnerType("build_snapshot", DataTypes.BOOLEAN)
                    .build())

                .register(Columns.CLUSTER_STATE_VERSION, DataTypes.LONG)

                .register(Columns.THREAD_POOLS, new ArrayType(ObjectType.builder()
                    .setInnerType("name", DataTypes.STRING)
                    .setInnerType("active", DataTypes.INTEGER)
                    .setInnerType("rejected", DataTypes.LONG)
                    .setInnerType("largest", DataTypes.INTEGER)
                    .setInnerType("completed", DataTypes.LONG)
                    .setInnerType("threads", DataTypes.INTEGER)
                    .setInnerType("queue", DataTypes.INTEGER)
                    .build()))

                .register(Columns.NETWORK, ObjectType.builder()
                    .setInnerType("probe_timestamp", DataTypes.TIMESTAMPZ)
                    .setInnerType("tcp", ObjectType.builder()
                        .setInnerType("connections", ObjectType.builder()
                            .setInnerType("initiated", DataTypes.LONG)
                            .setInnerType("accepted", DataTypes.LONG)
                            .setInnerType("curr_established", DataTypes.LONG)
                            .setInnerType("dropped", DataTypes.LONG)
                            .setInnerType("embryonic_dropped", DataTypes.LONG)
                            .build())
                        .setInnerType("packets", ObjectType.builder()
                            .setInnerType("sent", DataTypes.LONG)
                            .setInnerType("received", DataTypes.LONG)
                            .setInnerType("retransmitted", DataTypes.LONG)
                            .setInnerType("errors_received", DataTypes.LONG)
                            .setInnerType("rst_sent", DataTypes.LONG)
                            .build())
                        .build())
                    .build())

                .register(Columns.CONNECTIONS, ObjectType.builder()
                    .setInnerType("http", ObjectType.builder()
                        .setInnerType("open", DataTypes.LONG)
                        .setInnerType("total", DataTypes.LONG)
                        .build())
                    .setInnerType("psql", ObjectType.builder()
                        .setInnerType("open", DataTypes.LONG)
                        .setInnerType("total", DataTypes.LONG)
                        .build())
                    .setInnerType("transport", ObjectType.builder()
                        .setInnerType("open", DataTypes.LONG)
                        .build())
                    .build())

                .register(Columns.OS, ObjectType.builder()
                    .setInnerType("uptime", DataTypes.LONG)
                    .setInnerType("timestamp", DataTypes.TIMESTAMPZ)
                    .setInnerType("probe_timestamp", DataTypes.TIMESTAMPZ)
                    .setInnerType("cpu", ObjectType.builder()
                        .setInnerType("system", DataTypes.SHORT)
                        .setInnerType("user", DataTypes.SHORT)
                        .setInnerType("idle", DataTypes.SHORT)
                        .setInnerType("used", DataTypes.SHORT)
                        .setInnerType("stolen", DataTypes.SHORT)
                        .build())
                    .setInnerType("cgroup", ObjectType.builder()
                        .setInnerType("cpuacct", ObjectType.builder()
                            .setInnerType("control_group", DataTypes.STRING)
                            .setInnerType("usage_nanos", DataTypes.LONG)
                            .build())
                        .setInnerType("cpu", ObjectType.builder()
                            .setInnerType("control_group", DataTypes.STRING)
                            .setInnerType("cfs_period_micros", DataTypes.LONG)
                            .setInnerType("cfs_quota_micros", DataTypes.LONG)
                            .setInnerType("num_elapsed_periods", DataTypes.LONG)
                            .setInnerType("num_times_throttled", DataTypes.LONG)
                            .setInnerType("time_throttled_nanos", DataTypes.LONG)
                            .build())
                        .setInnerType("mem", ObjectType.builder()
                            .setInnerType("control_group", DataTypes.STRING)
                            .setInnerType("limit_bytes", DataTypes.STRING)
                            .setInnerType("usage_bytes", DataTypes.STRING)
                            .build())
                        .build())
                    .build())

                .register(Columns.OS_INFO, ObjectType.builder()
                    .setInnerType("available_processors", DataTypes.INTEGER)
                    .setInnerType("name", DataTypes.STRING)
                    .setInnerType("arch", DataTypes.STRING)
                    .setInnerType("version", DataTypes.STRING)
                    .setInnerType("jvm", ObjectType.builder()
                        .setInnerType("version", DataTypes.STRING)
                        .setInnerType("vm_name", DataTypes.STRING)
                        .setInnerType("vm_vendor", DataTypes.STRING)
                        .setInnerType("vm_version", DataTypes.STRING)
                        .build())
                    .build())

                .register(Columns.PROCESS, ObjectType.builder()
                    .setInnerType("open_file_descriptors", DataTypes.LONG)
                    .setInnerType("max_open_file_descriptors", DataTypes.LONG)
                    .setInnerType("probe_timestamp", DataTypes.TIMESTAMPZ)
                    .setInnerType("cpu", ObjectType.builder()
                        .setInnerType("percent", DataTypes.SHORT)
                        .setInnerType("user", DataTypes.LONG)
                        .setInnerType("system", DataTypes.LONG)
                        .build())
                    .build())

                .register(Columns.FS, ObjectType.builder()
                    .setInnerType("total", ObjectType.builder()
                        .setInnerType("size", DataTypes.LONG)
                        .setInnerType("used", DataTypes.LONG)
                        .setInnerType("available", DataTypes.LONG)
                        .setInnerType("reads", DataTypes.LONG)
                        .setInnerType("bytes_read", DataTypes.LONG)
                        .setInnerType("writes", DataTypes.LONG)
                        .setInnerType("bytes_written", DataTypes.LONG)
                        .build())
                    .setInnerType("disks", new ArrayType(ObjectType.builder()
                        .setInnerType("dev", DataTypes.STRING)
                        .setInnerType("size", DataTypes.LONG)
                        .setInnerType("used", DataTypes.LONG)
                        .setInnerType("available", DataTypes.LONG)
                        .setInnerType("reads", DataTypes.LONG)
                        .setInnerType("bytes_read", DataTypes.LONG)
                        .setInnerType("writes", DataTypes.LONG)
                        .setInnerType("bytes_written", DataTypes.LONG)
                        .build()))
                    .setInnerType("data", new ArrayType(ObjectType.builder()
                        .setInnerType("dev", DataTypes.STRING)
                        .setInnerType("path", DataTypes.STRING)
                        .build()))
                    .build()),
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
