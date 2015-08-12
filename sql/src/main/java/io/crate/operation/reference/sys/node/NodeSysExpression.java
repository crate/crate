/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.reference.sys.node;

import io.crate.metadata.*;
import io.crate.operation.reference.NestedObjectExpression;
import io.crate.operation.reference.sys.node.fs.NodeFsExpression;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.monitor.jvm.JvmService;
import org.elasticsearch.monitor.network.NetworkService;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.monitor.os.OsService;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.monitor.sigar.SigarService;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Collection;

public class NodeSysExpression extends NestedObjectExpression {

    private final NodeService nodeService;
    private final OsService osService;
    private final JvmService jvmService;
    private final NetworkService networkService;

    private static final Collection EXPRESSIONS_WITH_OS_STATS = Arrays.asList(
            NodeMemoryExpression.NAME,
            NodeLoadExpression.NAME,
            NodeOsExpression.NAME
    );

    @Inject
    public NodeSysExpression(ClusterService clusterService,
                             SigarService sigarService,
                             OsService osService,
                             NodeService nodeService,
                             JvmService jvmService,
                             NetworkService networkService,
                             NodeEnvironment nodeEnvironment,
                             Discovery discovery,
                             ThreadPool threadPool) {
        this.nodeService = nodeService;
        this.osService = osService;
        this.jvmService = jvmService;
        this.networkService = networkService;
        childImplementations.put(NodeFsExpression.NAME,
                new NodeFsExpression(sigarService, nodeEnvironment));
        childImplementations.put(NodeHostnameExpression.NAME,
                new NodeHostnameExpression(clusterService));
        childImplementations.put(NodeRestUrlExpression.NAME,
                new NodeRestUrlExpression(clusterService));
        childImplementations.put(NodeIdExpression.NAME,
                new NodeIdExpression(clusterService));
        childImplementations.put(NodeNameExpression.NAME,
                new NodeNameExpression(discovery));
        childImplementations.put(NodePortExpression.NAME,
                new NodePortExpression(nodeService));
        childImplementations.put(NodeVersionExpression.NAME,
                new NodeVersionExpression());
        childImplementations.put(NodeThreadPoolsExpression.NAME,
                new NodeThreadPoolsExpression(threadPool));
        childImplementations.put(NodeOsInfoExpression.NAME,
                new NodeOsInfoExpression(osService.info()));
    }

    @Override
    public ReferenceImplementation getChildImplementation(String name) {
        if (EXPRESSIONS_WITH_OS_STATS.contains(name)) {
            OsStats osStats = osService.stats();
            if (NodeMemoryExpression.NAME.equals(name)) {
                return new NodeMemoryExpression(osStats);
            } else if (NodeLoadExpression.NAME.equals(name)) {
                return new NodeLoadExpression(osStats);
            } else if (NodeOsExpression.NAME.equals(name)) {
                return new NodeOsExpression(osStats);
            }
        } else if (NodeProcessExpression.NAME.equals(name)) {
            return new NodeProcessExpression(nodeService.info().getProcess(),
                    nodeService.stats().getProcess());
        } else if (NodeHeapExpression.NAME.equals(name)) {
            return new NodeHeapExpression(jvmService.stats());
        } else if (NodeNetworkExpression.NAME.equals(name)) {
            return new NodeNetworkExpression(networkService.stats());
        }
        return super.getChildImplementation(name);
    }

}
