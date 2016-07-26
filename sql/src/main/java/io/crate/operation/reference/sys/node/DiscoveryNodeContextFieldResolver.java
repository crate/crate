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
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.monitor.ExtendedNodeInfo;
import io.crate.monitor.ThreadPools;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.Consumer;
import org.elasticsearch.common.util.concurrent.XRejectedExecutionHandler;
import org.elasticsearch.monitor.jvm.JvmService;
import org.elasticsearch.monitor.os.OsService;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

@Singleton
public class DiscoveryNodeContextFieldResolver {

    private final OsService osService;
    private final JvmService jvmService;
    private final ClusterService clusterService;
    private final NodeService nodeService;
    private final ThreadPool threadPool;
    private final ExtendedNodeInfo extendedNodeInfo;

    @Inject
    public DiscoveryNodeContextFieldResolver(ClusterService clusterService,
                                             OsService osService,
                                             NodeService nodeService,
                                             JvmService jvmService,
                                             ThreadPool threadPool,
                                             ExtendedNodeInfo extendedNodeInfo) {
        this.osService = osService;
        this.jvmService = jvmService;
        this.clusterService = clusterService;
        this.nodeService = nodeService;
        this.threadPool = threadPool;
        this.extendedNodeInfo = extendedNodeInfo;
    }

    public DiscoveryNodeContext resolveForColumnIdents(List<ReferenceIdent> referenceIdents) {
        List<ColumnIdent> processedColumnReferenceIdents = new ArrayList<>();
        DiscoveryNodeContext context = new DiscoveryNodeContext();

        for (ReferenceIdent ident : referenceIdents) {
            ColumnIdent columnReferenceIdent = ident.columnReferenceIdent().columnIdent();

            if (SysNodesTableInfo.IDENT.equals(ident.tableIdent())
                    && !processedColumnReferenceIdents.contains(columnReferenceIdent)) {
                columnToInit.get(columnReferenceIdent).accept(context);
                processedColumnReferenceIdents.add(columnReferenceIdent);
            }
        }
        return context;
    }


    private Map<ColumnIdent, Consumer<DiscoveryNodeContext>> columnToInit =
            ImmutableMap.<ColumnIdent, Consumer<DiscoveryNodeContext>>builder()
                    .put(SysNodesTableInfo.Columns.ID, new Consumer<DiscoveryNodeContext>() {
                        @Override
                        public void accept(DiscoveryNodeContext context) {
                            context.id = clusterService.localNode().id();
                        }
                    })
                    .put(SysNodesTableInfo.Columns.NAME, new Consumer<DiscoveryNodeContext>() {
                        @Override
                        public void accept(DiscoveryNodeContext context) {
                            context.name = clusterService.localNode().name();
                        }
                    })
                    .put(SysNodesTableInfo.Columns.HOSTNAME, new Consumer<DiscoveryNodeContext>() {
                        @Override
                        public void accept(DiscoveryNodeContext context) {
                            try {
                                context.hostname = InetAddress.getLocalHost().getHostName();
                            } catch (UnknownHostException e) {
                            }
                        }
                    })
                    .put(SysNodesTableInfo.Columns.REST_URL, new Consumer<DiscoveryNodeContext>() {
                        @Override
                        public void accept(DiscoveryNodeContext context) {
                            context.restUrl = clusterService.localNode().attributes().get("http_address");
                        }
                    })
                    .put(SysNodesTableInfo.Columns.PORT, new Consumer<DiscoveryNodeContext>() {
                        @Override
                        public void accept(DiscoveryNodeContext context) {
                            int transport = 0;
                            try {
                                transport = portFromAddress(nodeService.stats().getNode().address());
                            } catch (IOException e) {
                                // This is a bug in ES method signature, IOException is never thrown
                            }
                            context.port = new HashMap<>();
                            context.port.put("HTTP", portFromAddress(nodeService.info().getHttp().address().publishAddress()));
                            context.port.put("TRANSPORT", transport);
                        }
                    })
                    .put(SysNodesTableInfo.Columns.LOAD, new Consumer<DiscoveryNodeContext>() {
                        @Override
                        public void accept(DiscoveryNodeContext context) {
                            context.extendedOsStats = extendedNodeInfo.osStats();
                        }
                    })
                    .put(SysNodesTableInfo.Columns.MEM, new Consumer<DiscoveryNodeContext>() {
                        @Override
                        public void accept(DiscoveryNodeContext context) {
                            context.osStats = osService.stats();
                        }
                    })
                    .put(SysNodesTableInfo.Columns.HEAP, new Consumer<DiscoveryNodeContext>() {
                        @Override
                        public void accept(DiscoveryNodeContext context) {
                            context.jvmStats = jvmService.stats();
                        }
                    })
                    .put(SysNodesTableInfo.Columns.VERSION, new Consumer<DiscoveryNodeContext>() {
                        @Override
                        public void accept(DiscoveryNodeContext context) {
                            context.version = Version.CURRENT;
                            context.build = Build.CURRENT;
                        }
                    })
                    .put(SysNodesTableInfo.Columns.THREAD_POOLS, new Consumer<DiscoveryNodeContext>() {
                        @Override
                        public void accept(DiscoveryNodeContext context) {
                            ThreadPools threadPools = new ThreadPools();
                            for (ThreadPool.Info info : threadPool.info()) {
                                String name = info.getName();

                                ThreadPoolExecutor executor = (ThreadPoolExecutor) threadPool.executor(name);
                                long rejectedCount = -1;
                                RejectedExecutionHandler rejectedExecutionHandler = executor.getRejectedExecutionHandler();
                                if (rejectedExecutionHandler instanceof XRejectedExecutionHandler) {
                                    rejectedCount = ((XRejectedExecutionHandler) rejectedExecutionHandler).rejected();
                                }

                                ThreadPools.ThreadPoolExecutorContext executorContext =
                                        new ThreadPools.ThreadPoolExecutorContext(
                                                executor.getActiveCount(),
                                                executor.getQueue().size(),
                                                executor.getLargestPoolSize(),
                                                executor.getPoolSize(),
                                                executor.getCompletedTaskCount(),
                                                rejectedCount);
                                threadPools.add(name, executorContext);
                            }
                            context.threadPools = threadPools;
                        }
                    })
                    .put(SysNodesTableInfo.Columns.NETWORK, new Consumer<DiscoveryNodeContext>() {
                        @Override
                        public void accept(DiscoveryNodeContext context) {
                            context.networkStats = extendedNodeInfo.networkStats();
                        }
                    })
                    .put(SysNodesTableInfo.Columns.OS, new Consumer<DiscoveryNodeContext>() {
                        @Override
                        public void accept(DiscoveryNodeContext context) {
                            context.extendedOsStats = extendedNodeInfo.osStats();
                        }
                    })
                    .put(SysNodesTableInfo.Columns.OS_INFO, new Consumer<DiscoveryNodeContext>() {
                        @Override
                        public void accept(DiscoveryNodeContext context) {
                            context.osInfo = nodeService.info().getOs();
                        }
                    })
                    .put(SysNodesTableInfo.Columns.PROCESS, new Consumer<DiscoveryNodeContext>() {
                        @Override
                        public void accept(DiscoveryNodeContext context) {
                            context.extendedProcessCpuStats = extendedNodeInfo.processCpuStats();
                            try {
                                context.processStats = nodeService.stats().getProcess();
                            } catch (IOException e) {
                                // This is a bug in ES method signature, IOException is never thrown
                            }
                        }
                    })
                    .put(SysNodesTableInfo.Columns.FS, new Consumer<DiscoveryNodeContext>() {
                        @Override
                        public void accept(DiscoveryNodeContext context) {
                            context.extendedFsStats = extendedNodeInfo.fsStats();
                        }
                    }).build();


    private static Integer portFromAddress(TransportAddress address) {
        Integer port = 0;
        if (address instanceof InetSocketTransportAddress) {
            port = ((InetSocketTransportAddress) address).address().getPort();
        }
        return port;
    }
}
