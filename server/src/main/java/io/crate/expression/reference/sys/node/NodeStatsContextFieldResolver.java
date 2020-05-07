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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.monitor.ExtendedNodeInfo;
import io.crate.protocols.ConnectionStats;
import io.crate.protocols.postgres.PostgresNetty;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.monitor.fs.FsService;
import org.elasticsearch.monitor.jvm.JvmService;
import org.elasticsearch.monitor.os.OsService;
import org.elasticsearch.monitor.process.ProcessService;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nullable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static io.crate.expression.reference.sys.node.Ports.portFromAddress;

@Singleton
public class NodeStatsContextFieldResolver {

    private static final Logger LOGGER = LogManager.getLogger(NodeStatsContextFieldResolver.class);
    private final Supplier<DiscoveryNode> localNode;
    private final Supplier<TransportAddress> boundHttpAddress;
    private final Supplier<HttpStats> httpStatsSupplier;
    private final ThreadPool threadPool;
    private final ExtendedNodeInfo extendedNodeInfo;
    private final Supplier<ConnectionStats> psqlStats;
    private final Supplier<TransportAddress> boundPostgresAddress;
    private final LongSupplier numOpenTransportConnections;
    private final ProcessService processService;
    private final OsService osService;
    private final JvmService jvmService;
    private final FsService fsService;
    private final LongSupplier clusterStateVersion;

    @Inject
    @SuppressWarnings("unused")
    public NodeStatsContextFieldResolver(ClusterService clusterService,
                                         NodeService nodeService,
                                         @Nullable HttpServerTransport httpServerTransport,
                                         TransportService transportService,
                                         ThreadPool threadPool,
                                         ExtendedNodeInfo extendedNodeInfo,
                                         PostgresNetty postgresNetty) {
        this(
            clusterService::localNode,
            nodeService.getMonitorService(),
            () -> httpServerTransport == null ? null : httpServerTransport.info().getAddress().publishAddress(),
            () -> httpServerTransport == null ? null : httpServerTransport.stats(),
            threadPool,
            extendedNodeInfo,
            () -> new ConnectionStats(postgresNetty.openConnections(), postgresNetty.totalConnections()),
            () -> {
                BoundTransportAddress boundTransportAddress = postgresNetty.boundAddress();
                if (boundTransportAddress == null) {
                    return null;
                }
                return boundTransportAddress.publishAddress();
            },
            () -> transportService.stats().getServerOpen(),
            () -> clusterService.state().version()
        );
    }

    @VisibleForTesting
    NodeStatsContextFieldResolver(Supplier<DiscoveryNode> localNode,
                                  MonitorService monitorService,
                                  Supplier<TransportAddress> boundHttpAddress,
                                  Supplier<HttpStats> httpStatsSupplier,
                                  ThreadPool threadPool,
                                  ExtendedNodeInfo extendedNodeInfo,
                                  Supplier<ConnectionStats> psqlStats,
                                  Supplier<TransportAddress> boundPostgresAddress,
                                  LongSupplier numOpenTransportConnections,
                                  LongSupplier clusterStateVersion) {
        this.localNode = localNode;
        processService = monitorService.processService();
        osService = monitorService.osService();
        jvmService = monitorService.jvmService();
        fsService = monitorService.fsService();
        this.httpStatsSupplier = httpStatsSupplier;
        this.boundHttpAddress = boundHttpAddress;
        this.threadPool = threadPool;
        this.extendedNodeInfo = extendedNodeInfo;
        this.psqlStats = psqlStats;
        this.boundPostgresAddress = boundPostgresAddress;
        this.numOpenTransportConnections = numOpenTransportConnections;
        this.clusterStateVersion = clusterStateVersion;
    }

    public NodeStatsContext forTopColumnIdents(Collection<ColumnIdent> topColumnIdents) {
        NodeStatsContext context = new NodeStatsContext(true);
        if (topColumnIdents.isEmpty()) {
            return context;
        }

        for (ColumnIdent column : topColumnIdents) {
            consumerForTopColumnIdent(column).accept(context);
        }
        return context;
    }

    private Consumer<NodeStatsContext> consumerForTopColumnIdent(ColumnIdent columnIdent) {
        Consumer<NodeStatsContext> consumer = columnIdentToContext.get(columnIdent);
        if (consumer == null) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "Cannot resolve NodeStatsContext field for \"%s\" column ident.", columnIdent)
            );
        }
        return consumer;
    }

    private final Map<ColumnIdent, Consumer<NodeStatsContext>> columnIdentToContext =
        ImmutableMap.<ColumnIdent, Consumer<NodeStatsContext>>builder()
            .put(SysNodesTableInfo.Columns.ID, new Consumer<NodeStatsContext>() {
                @Override
                public void accept(NodeStatsContext context) {
                    context.id(localNode.get().getId());
                }
            })
            .put(SysNodesTableInfo.Columns.NAME, new Consumer<NodeStatsContext>() {
                @Override
                public void accept(NodeStatsContext context) {
                    context.name(localNode.get().getName());
                }
            })
            .put(SysNodesTableInfo.Columns.HOSTNAME, context -> {
                try {
                    context.hostname(InetAddress.getLocalHost().getHostName());
                } catch (UnknownHostException e) {
                    LOGGER.warn("Cannot resolve the hostname.", e);
                }
            })
            .put(SysNodesTableInfo.Columns.REST_URL, new Consumer<NodeStatsContext>() {
                @Override
                public void accept(NodeStatsContext context) {
                    DiscoveryNode node = localNode.get();
                    if (node != null) {
                        String url = node.getAttributes().get("http_address");
                        context.restUrl(url);
                    }
                }
            })
            .put(SysNodesTableInfo.Columns.PORT, new Consumer<NodeStatsContext>() {
                @Override
                public void accept(NodeStatsContext context) {
                    Integer http = portFromAddress(boundHttpAddress.get());
                    Integer transport = portFromAddress(localNode.get().getAddress());
                    Integer pgsql = portFromAddress(boundPostgresAddress.get());
                    context.httpPort(http);
                    context.transportPort(transport);
                    context.pgPort(pgsql);
                }
            })
            .put(SysNodesTableInfo.Columns.CLUSTER_STATE_VERSION, new Consumer<NodeStatsContext>() {
                @Override
                public void accept(NodeStatsContext context) {
                    context.clusterStateVersion(clusterStateVersion.getAsLong());
                }
            })
            .put(SysNodesTableInfo.Columns.LOAD, new Consumer<NodeStatsContext>() {
                @Override
                public void accept(NodeStatsContext context) {
                    context.extendedOsStats(extendedNodeInfo.osStats());
                }
            })
            .put(SysNodesTableInfo.Columns.MEM, new Consumer<NodeStatsContext>() {
                @Override
                public void accept(NodeStatsContext context) {
                    context.osStats(osService.stats());
                }
            })
            .put(SysNodesTableInfo.Columns.HEAP, new Consumer<NodeStatsContext>() {
                @Override
                public void accept(NodeStatsContext context) {
                    context.jvmStats(jvmService.stats());
                }
            })
            .put(SysNodesTableInfo.Columns.VERSION, context -> {
                context.version(Version.CURRENT);
                context.build(Build.CURRENT);
            })
            .put(SysNodesTableInfo.Columns.THREAD_POOLS, new Consumer<NodeStatsContext>() {
                @Override
                public void accept(NodeStatsContext context) {
                    context.threadPools(threadPool.stats());
                }
            })
            .put(SysNodesTableInfo.Columns.NETWORK, context -> {
            })
            .put(SysNodesTableInfo.Columns.CONNECTIONS, new Consumer<NodeStatsContext>() {
                @Override
                public void accept(NodeStatsContext nodeStatsContext) {
                    nodeStatsContext.httpStats(httpStatsSupplier.get());
                    nodeStatsContext.psqlStats(psqlStats.get());
                    nodeStatsContext.openTransportConnections(numOpenTransportConnections.getAsLong());
                }
            })
            .put(SysNodesTableInfo.Columns.OS, new Consumer<NodeStatsContext>() {
                @Override
                public void accept(NodeStatsContext context) {
                    context.timestamp(System.currentTimeMillis());
                    context.extendedOsStats(extendedNodeInfo.osStats());
                }
            })
            .put(SysNodesTableInfo.Columns.OS_INFO, new Consumer<NodeStatsContext>() {
                @Override
                public void accept(NodeStatsContext context) {
                    context.osInfo(osService.info());
                }
            })
            .put(SysNodesTableInfo.Columns.PROCESS, new Consumer<NodeStatsContext>() {
                @Override
                public void accept(NodeStatsContext context) {
                    context.processStats(processService.stats());
                }
            })
            .put(SysNodesTableInfo.Columns.FS, new Consumer<NodeStatsContext>() {
                @Override
                public void accept(NodeStatsContext context) {
                    context.fsInfo(fsService.stats());
                }
            }).build();
}
