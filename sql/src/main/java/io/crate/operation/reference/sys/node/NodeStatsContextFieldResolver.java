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

import com.google.common.collect.ImmutableMap;
import io.crate.Build;
import io.crate.Version;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.monitor.ExtendedNodeInfo;
import io.crate.monitor.ThreadPools;
import io.crate.protocols.postgres.PostgresNetty;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.monitor.jvm.JvmService;
import org.elasticsearch.monitor.os.OsService;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;

@Singleton
public class NodeStatsContextFieldResolver {

    private final static Logger LOGGER = Loggers.getLogger(NodeStatsContextFieldResolver.class);

    private final OsService osService;
    private final JvmService jvmService;
    private final ClusterService clusterService;
    private final NodeService nodeService;
    private final ThreadPool threadPool;
    private final ExtendedNodeInfo extendedNodeInfo;
    private final PostgresNetty postgresNetty;

    @Inject
    public NodeStatsContextFieldResolver(ClusterService clusterService,
                                         OsService osService,
                                         NodeService nodeService,
                                         JvmService jvmService,
                                         ThreadPool threadPool,
                                         ExtendedNodeInfo extendedNodeInfo,
                                         PostgresNetty postgresNetty) {
        this.osService = osService;
        this.jvmService = jvmService;
        this.clusterService = clusterService;
        this.nodeService = nodeService;
        this.threadPool = threadPool;
        this.extendedNodeInfo = extendedNodeInfo;
        this.postgresNetty = postgresNetty;
    }

    public NodeStatsContext forColumns(Collection<ColumnIdent> columns) {
        NodeStatsContext context = NodeStatsContext.newInstance();
        if (columns.isEmpty()) {
            return context;
        }

        for (ColumnIdent column : columns) {
            consumerForColumnIdent(column).accept(context);
        }
        return context;
    }

    private Consumer<NodeStatsContext> consumerForColumnIdent(ColumnIdent columnIdent) {
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
                    context.id(BytesRefs.toBytesRef(clusterService.localNode().getId()));
                }
            })
            .put(SysNodesTableInfo.Columns.NAME, new Consumer<NodeStatsContext>() {
                @Override
                public void accept(NodeStatsContext context) {
                    context.name(BytesRefs.toBytesRef(clusterService.localNode().getName()));
                }
            })
            .put(SysNodesTableInfo.Columns.HOSTNAME, new Consumer<NodeStatsContext>() {
                @Override
                public void accept(NodeStatsContext context) {
                    try {
                        context.hostname(BytesRefs.toBytesRef(InetAddress.getLocalHost().getHostName()));
                    } catch (UnknownHostException e) {
                        LOGGER.warn("Cannot resolve the hostname.", e);
                    }
                }
            })
            .put(SysNodesTableInfo.Columns.REST_URL, new Consumer<NodeStatsContext>() {
                @Override
                public void accept(NodeStatsContext context) {
                    DiscoveryNode node = clusterService.localNode();
                    if (node != null) {
                        String url = node.getAttributes().get("http_address");
                        context.restUrl(BytesRefs.toBytesRef(url));
                    }
                }
            })
            .put(SysNodesTableInfo.Columns.PORT, new Consumer<NodeStatsContext>() {
                @Override
                public void accept(NodeStatsContext context) {
                    Integer http = null;
                    Integer pgsql = null;
                    Integer transport;
                    NodeInfo info = nodeService.info();
                    if (info.getHttp() != null) {
                        http = portFromAddress(info.getHttp().address().publishAddress());
                    }
                    try {
                        transport = portFromAddress(nodeService.stats().getNode().address());
                    } catch (IOException e) {
                        throw new ElasticsearchException("unable to get node transport statistics", e);
                    }
                    if (postgresNetty.boundAddress() != null) {
                        pgsql = portFromAddress(postgresNetty.boundAddress().publishAddress());
                    }
                    Map<String, Integer> port = new HashMap<>(2);
                    port.put("http", http);
                    port.put("transport", transport);
                    port.put("psql", pgsql);
                    context.port(port);
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
            .put(SysNodesTableInfo.Columns.VERSION, new Consumer<NodeStatsContext>() {
                @Override
                public void accept(NodeStatsContext context) {
                    context.version(Version.CURRENT);
                    context.build(Build.CURRENT);
                }
            })
            .put(SysNodesTableInfo.Columns.THREAD_POOLS, new Consumer<NodeStatsContext>() {
                @Override
                public void accept(NodeStatsContext context) {
                    context.threadPools(ThreadPools.newInstance(threadPool));
                }
            })
            .put(SysNodesTableInfo.Columns.NETWORK, new Consumer<NodeStatsContext>() {
                @Override
                public void accept(NodeStatsContext context) {
                    context.networkStats(extendedNodeInfo.networkStats());
                }
            })
            .put(SysNodesTableInfo.Columns.OS, new Consumer<NodeStatsContext>() {
                @Override
                public void accept(NodeStatsContext context) {
                    context.extendedOsStats(extendedNodeInfo.osStats());
                }
            })
            .put(SysNodesTableInfo.Columns.OS_INFO, new Consumer<NodeStatsContext>() {
                @Override
                public void accept(NodeStatsContext context) {
                    context.osInfo(nodeService.info().getOs());
                }
            })
            .put(SysNodesTableInfo.Columns.PROCESS, new Consumer<NodeStatsContext>() {
                @Override
                public void accept(NodeStatsContext context) {
                    context.extendedProcessCpuStats(extendedNodeInfo.processCpuStats());
                    try {
                        context.processStats(nodeService.stats().getProcess());
                    } catch (IOException e) {
                        throw new ElasticsearchException("unable to get node statistics", e);
                    }
                }
            })
            .put(SysNodesTableInfo.Columns.FS, new Consumer<NodeStatsContext>() {
                @Override
                public void accept(NodeStatsContext context) {
                    context.extendedFsStats(extendedNodeInfo.fsStats());
                }
            }).build();


    private static Integer portFromAddress(TransportAddress address) {
        Integer port = null;
        if (address instanceof InetSocketTransportAddress) {
            port = ((InetSocketTransportAddress) address).address().getPort();
        }
        return port;
    }
}
