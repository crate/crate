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
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.LONG;
import static io.crate.types.DataTypes.DOUBLE;
import static io.crate.types.DataTypes.SHORT;
import static io.crate.types.DataTypes.TIMESTAMPZ;
import static io.crate.types.DataTypes.INTEGER;

public class SysNodesTableInfo extends StaticTableInfo<NodeStatsContext> {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "nodes");

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

        public static final ColumnIdent NETWORK = new ColumnIdent(SYS_COL_NETWORK);

        public static final ColumnIdent CONNECTIONS = new ColumnIdent("connections");

        public static final ColumnIdent OS = new ColumnIdent(SYS_COL_OS);

        public static final ColumnIdent OS_INFO = new ColumnIdent(SYS_COL_OS_INFO);

        public static final ColumnIdent PROCESS = new ColumnIdent(SYS_COL_PROCESS);

        public static final ColumnIdent FS = new ColumnIdent(SYS_COL_FS);
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<NodeStatsContext>> expressions() {
        return columnRegistrar().expressions();
    }

    private static ObjectCollectExpression<NodeStatsContext> createConnectionsExpression() {
        return new ObjectCollectExpression<>(
            ImmutableMap.of(
                "http",
                new ObjectCollectExpression<NodeStatsContext>(
                    ImmutableMap.of(
                        "open",
                        NestableCollectExpression.<NodeStatsContext, HttpStats>withNullableProperty(
                            NodeStatsContext::httpStats,
                            HttpStats::getServerOpen),
                        "total",
                        NestableCollectExpression.<NodeStatsContext, HttpStats>withNullableProperty(
                            NodeStatsContext::httpStats,
                            HttpStats::getTotalOpen)
                    )
                ),
                "psql",
                new ObjectCollectExpression<NodeStatsContext>(
                    ImmutableMap.of(
                        "open",
                        NestableCollectExpression.<NodeStatsContext, ConnectionStats>withNullableProperty(
                            NodeStatsContext::psqlStats,
                            ConnectionStats::open),
                        "total",
                        NestableCollectExpression.<NodeStatsContext, ConnectionStats>withNullableProperty(
                            NodeStatsContext::psqlStats,
                            ConnectionStats::total)
                    )
                ),
                "transport",
                new ObjectCollectExpression<NodeStatsContext>(
                    ImmutableMap.of(
                        "open",
                        forFunction(NodeStatsContext::openTransportConnections)
                    )
                )
            )
        );
    }

    @SuppressWarnings({"unchecked"})
    private static ColumnRegistrar<NodeStatsContext> columnRegistrar() {
        return new ColumnRegistrar<NodeStatsContext>(IDENT, GRANULARITY)
            .register("id", STRING, () -> forFunction(NodeStatsContext::id))
            .register("name", STRING, () -> forFunction(NodeStatsContext::name))
            .register("hostname", STRING, () -> forFunction(r -> r.isComplete() ? r.hostname() : null))
            .register("rest_url", STRING, () -> forFunction(r -> r.isComplete() ? r.restUrl() : null))

            .register("port", ObjectType.builder()
                .setInnerType("http", INTEGER)
                .setInnerType("transport", INTEGER)
                .setInnerType("psql", INTEGER)
                .build(), NodePortStatsExpression::new)

            .register("load", ObjectType.builder()
                .setInnerType("1", DOUBLE)
                .setInnerType("5", DOUBLE)
                .setInnerType("15", DOUBLE)
                .setInnerType("probe_timestamp", TIMESTAMPZ)
                .build(), NodeLoadStatsExpression::new)

            .register("mem", ObjectType.builder()
                .setInnerType("free", LONG)
                .setInnerType("used", LONG)
                .setInnerType("free_percent", SHORT)
                .setInnerType("used_percent", SHORT)
                .setInnerType("probe_timestamp", TIMESTAMPZ)
                .build(), NodeMemoryStatsExpression::new)

            .register("heap", ObjectType.builder()
                .setInnerType("free", LONG)
                .setInnerType("used", LONG)
                .setInnerType("max", LONG)
                .setInnerType("probe_timestamp", TIMESTAMPZ)
                .build(), NodeHeapStatsExpression::new)

            .register("version", ObjectType.builder()
                .setInnerType("number", STRING)
                .setInnerType("build_hash", STRING)
                .setInnerType("build_snapshot", DataTypes.BOOLEAN)
                .setInnerType("minimum_index_compatibility_version", STRING)
                .setInnerType("minimum_wire_compatibility_version", STRING)
                .build(), NodeVersionStatsExpression::new)

            .register("cluster_state_version", LONG, () -> new SimpleNodeStatsExpression<Long>() {
                @Override
                public Long innerValue(NodeStatsContext nodeStatsContext) {
                    return nodeStatsContext.clusterStateVersion();
                }
            })

            .register("thread_pools", "name", STRING, () -> new NodeStatsThreadPoolExpression<String>() {
                @Override
                protected String valueForItem(ThreadPoolStats.Stats input) {
                    return input.getName();
                }
            })

            .register("thread_pools", new ArrayType(ObjectType.untyped()), NodeThreadPoolsExpression::new)

            .register("thread_pools", "active", INTEGER, () -> new NodeStatsThreadPoolExpression<Integer>() {
                @Override
                protected Integer valueForItem(ThreadPoolStats.Stats stats) {
                    return stats.getActive();
                }
            })
            .register("thread_pools", "rejected", LONG, () -> new NodeStatsThreadPoolExpression<Long>() {
                @Override
                protected Long valueForItem(ThreadPoolStats.Stats stats) {
                    return stats.getRejected();
                }
            })
            .register("thread_pools","largest", INTEGER, () -> new NodeStatsThreadPoolExpression<Integer>() {
                @Override
                protected Integer valueForItem(ThreadPoolStats.Stats stats) {
                    return stats.getLargest();
                }
            })
            .register("thread_pools", "completed", LONG, () -> new NodeStatsThreadPoolExpression<Long>() {
                @Override
                protected Long valueForItem(ThreadPoolStats.Stats stats) {
                    return stats.getCompleted();
                }
            })
            .register("thread_pools", "threads", INTEGER, () -> new NodeStatsThreadPoolExpression<Integer>() {
                @Override
                protected Integer valueForItem(ThreadPoolStats.Stats stats) {
                    return stats.getThreads();
                }
            })
            .register("thread_pools","queue", INTEGER, () -> new NodeStatsThreadPoolExpression<Integer>() {
                @Override
                protected Integer valueForItem(ThreadPoolStats.Stats stats) {
                    return stats.getQueue();
                }
            })

            .register("network", ObjectType.builder()
                .setInnerType("probe_timestamp", TIMESTAMPZ)
                .setInnerType("tcp", ObjectType.builder()
                    .setInnerType("connections", ObjectType.builder()
                        .setInnerType("initiated", LONG)
                        .setInnerType("accepted", LONG)
                        .setInnerType("curr_established", LONG)
                        .setInnerType("dropped", LONG)
                        .setInnerType("embryonic_dropped", LONG)
                        .build())
                    .setInnerType("packets", ObjectType.builder()
                        .setInnerType("sent", LONG)
                        .setInnerType("received", LONG)
                        .setInnerType("retransmitted", LONG)
                        .setInnerType("errors_received", LONG)
                        .setInnerType("rst_sent", LONG)
                        .build())
                    .build())
                .build(), NodeNetworkStatsExpression::new)

            .register("connections", ObjectType.builder()
                .setInnerType("http", ObjectType.builder()
                    .setInnerType("open", LONG)
                    .setInnerType("total", LONG)
                    .build())
                .setInnerType("psql", ObjectType.builder()
                    .setInnerType("open", LONG)
                    .setInnerType("total", LONG)
                    .build())
                .setInnerType("transport", ObjectType.builder()
                    .setInnerType("open", LONG)
                    .build())
                .build(),SysNodesTableInfo::createConnectionsExpression)

            .register("os", ObjectType.builder()
                .setInnerType("uptime", LONG)
                .setInnerType("timestamp", TIMESTAMPZ)
                .setInnerType("probe_timestamp", TIMESTAMPZ)
                .setInnerType("cpu", ObjectType.builder()
                    .setInnerType("used", SHORT)
                    .build())
                .setInnerType("cgroup", ObjectType.builder()
                    .setInnerType("cpuacct", ObjectType.builder()
                        .setInnerType("control_group", STRING)
                        .setInnerType("usage_nanos", LONG)
                        .build())
                    .setInnerType("cpu", ObjectType.builder()
                        .setInnerType("control_group", STRING)
                        .setInnerType("cfs_period_micros", LONG)
                        .setInnerType("cfs_quota_micros", LONG)
                        .setInnerType("num_elapsed_periods", LONG)
                        .setInnerType("num_times_throttled", LONG)
                        .setInnerType("time_throttled_nanos", LONG)
                        .build())
                    .setInnerType("mem", ObjectType.builder()
                        .setInnerType("control_group", STRING)
                        .setInnerType("limit_bytes", STRING)
                        .setInnerType("usage_bytes", STRING)
                        .build())
                    .build())
                .build(), NodeOsStatsExpression::new)

            .register("os_info", ObjectType.builder()
                .setInnerType("available_processors", INTEGER)
                .setInnerType("name", STRING)
                .setInnerType("arch", STRING)
                .setInnerType("version", STRING)
                .setInnerType("jvm", ObjectType.builder()
                    .setInnerType("version", STRING)
                    .setInnerType("vm_name", STRING)
                    .setInnerType("vm_vendor", STRING)
                    .setInnerType("vm_version", STRING)
                    .build())
                .build(), NodeOsInfoStatsExpression::new)

            .register("process", ObjectType.builder()
                .setInnerType("open_file_descriptors", LONG)
                .setInnerType("max_open_file_descriptors", LONG)
                .setInnerType("probe_timestamp", TIMESTAMPZ)
                .setInnerType("cpu", ObjectType.builder()
                    .setInnerType("percent", SHORT)
                    .build())
                .build(), NodeProcessStatsExpression::new)

            .register("fs", ObjectType.builder()
                .setInnerType("total", ObjectType.builder()
                    .setInnerType("size", LONG)
                    .setInnerType("used", LONG)
                    .setInnerType("available", LONG)
                    .setInnerType("reads", LONG)
                    .setInnerType("bytes_read", LONG)
                    .setInnerType("writes", LONG)
                    .setInnerType("bytes_written", LONG)
                    .build())
                .setInnerType("disks", new ArrayType(ObjectType.builder()
                                                         .setInnerType("dev", STRING)
                                                         .setInnerType("size", LONG)
                                                         .setInnerType("used", LONG)
                                                         .setInnerType("available", LONG)
                                                         .build()))
                .setInnerType("data", new ArrayType(ObjectType.builder()
                                                        .setInnerType("dev", STRING)
                                                        .setInnerType("path", STRING)
                                                        .build()))
                .build(), NodeFsStatsExpression::new)

            .register("fs", "total", ObjectType.untyped(), NodeFsTotalStatsExpression::new)
            .register("fs", ImmutableList.of("total", "size"), LONG,
                () -> forFunction((NodeStatsContext r) -> r.isComplete() ? FsInfoHelpers.Path.size(r.fsInfo().getTotal()) : null))
            .register("fs", ImmutableList.of("total", "used"), LONG,
                () -> forFunction((NodeStatsContext r) -> r.isComplete() ? FsInfoHelpers.Path.used(r.fsInfo().getTotal()) : null))
            .register("fs", ImmutableList.of("total", "available"), LONG,
                () -> forFunction((NodeStatsContext r) -> r.isComplete() ? FsInfoHelpers.Path.available(r.fsInfo().getTotal()) : null))
            .register("fs", ImmutableList.of("total", "reads"), LONG,
                () -> forFunction((NodeStatsContext r) -> r.isComplete() ? FsInfoHelpers.Stats.readOperations(r.fsInfo().getIoStats()) : null))
            .register("fs", ImmutableList.of("total", "bytes_read"), LONG,
                () -> forFunction((NodeStatsContext r) -> r.isComplete() ? FsInfoHelpers.Stats.bytesRead(r.fsInfo().getIoStats()) : null))
            .register("fs", ImmutableList.of("total", "writes"), LONG,
                () -> forFunction((NodeStatsContext r) -> r.isComplete() ? FsInfoHelpers.Stats.writeOperations(r.fsInfo().getIoStats()) : null))
            .register("fs", ImmutableList.of("total", "bytes_written"), LONG,
                () -> forFunction((NodeStatsContext r) -> r.isComplete() ? FsInfoHelpers.Stats.bytesWritten(r.fsInfo().getIoStats()) : null))
            .register("fs", ImmutableList.of("disks"), ObjectType.untyped(), NodeStatsFsDisksExpression::new)
            .register("fs", ImmutableList.of("disks", "dev"), ObjectType.untyped(), () -> new NodeStatsFsArrayExpression<String>() {
                @Override
                protected String valueForItem(FsInfo.Path input) {
                    return FsInfoHelpers.Path.dev(input);
                }
            })
            .register("fs", ImmutableList.of("disks", "size"), ObjectType.untyped(), () -> new NodeStatsFsArrayExpression<Long>() {
                @Override
                protected Long valueForItem(FsInfo.Path input) {
                    return FsInfoHelpers.Path.size(input);
                }
            })
            .register("fs", ImmutableList.of("disks", "used"), ObjectType.untyped(), () -> new NodeStatsFsArrayExpression<Long>() {
                @Override
                protected Long valueForItem(FsInfo.Path input) {
                    return FsInfoHelpers.Path.used(input);
                }
            })
            .register("fs", ImmutableList.of("disks", "available"), ObjectType.untyped(), () -> new NodeStatsFsArrayExpression<Long>() {
                @Override
                protected Long valueForItem(FsInfo.Path input) {
                    return FsInfoHelpers.Path.available(input);
                }
            })
            .register("fs","data", ObjectType.untyped(), NodeStatsFsDataExpression::new)
            .register("fs", ImmutableList.of("data", "dev"), ObjectType.untyped(), () -> new NodeStatsFsArrayExpression<String>() {
                @Override
                protected String valueForItem(FsInfo.Path input) {
                    return FsInfoHelpers.Path.dev(input);
                }
            })
            .register("fs", ImmutableList.of("data", "path"), ObjectType.untyped(), () -> new NodeStatsFsArrayExpression<String>() {
                @Override
                protected String valueForItem(FsInfo.Path input) {
                    return input.getPath();
                }
            });
    }

    public SysNodesTableInfo() {
        super(IDENT, columnRegistrar(), "id");
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
