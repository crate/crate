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

package io.crate.execution.jobs.transport;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.node.Node;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportConnectionListener;
import org.elasticsearch.transport.TransportService;

import org.jetbrains.annotations.VisibleForTesting;
import io.crate.execution.jobs.NodeLimits;
import io.crate.execution.jobs.TasksService;
import io.crate.execution.jobs.kill.KillJobsNodeAction;
import io.crate.execution.jobs.kill.KillJobsNodeRequest;
import io.crate.execution.jobs.kill.KillResponse;
import io.crate.execution.support.ActionExecutor;
import io.crate.role.Role;

/**
 * service that listens to node-disconnected-events and kills jobContexts that were started by the nodes that got disconnected
 */
@Singleton
public class NodeDisconnectJobMonitorService extends AbstractLifecycleComponent implements TransportConnectionListener {

    private final TasksService tasksService;
    private final NodeLimits nodeLimits;
    private final TransportService transportService;

    private final ActionExecutor<KillJobsNodeRequest, KillResponse> killNodeAction;
    private static final Logger LOGGER = LogManager.getLogger(NodeDisconnectJobMonitorService.class);

    @Inject
    public NodeDisconnectJobMonitorService(TasksService tasksService,
                                           NodeLimits nodeLimits,
                                           TransportService transportService,
                                           Node node) {
        this(tasksService, nodeLimits, transportService, req -> node.client().execute(KillJobsNodeAction.INSTANCE, req));
    }

    @VisibleForTesting
    NodeDisconnectJobMonitorService(TasksService tasksService,
                                    NodeLimits nodeLimits,
                                    TransportService transportService,
                                    ActionExecutor<KillJobsNodeRequest, KillResponse> killNodeAction) {
        this.tasksService = tasksService;
        this.nodeLimits = nodeLimits;
        this.transportService = transportService;
        this.killNodeAction = killNodeAction;
    }

    @Override
    protected void doStart() {
        transportService.addConnectionListener(this);
    }

    @Override
    protected void doStop() {
        transportService.removeConnectionListener(this);
    }

    @Override
    protected void doClose() {
    }

    @Override
    public void onNodeConnected(DiscoveryNode node, Transport.Connection connection) {
    }

    @Override
    public void onNodeDisconnected(DiscoveryNode node, Transport.Connection connection) {
        nodeLimits.nodeDisconnected(node.getId());
        killJobsCoordinatedBy(node);
        broadcastKillToParticipatingNodes(node);
    }

    /**
     * Broadcast the kill if *this* node is the coordinator and a participating node died
     * The information which nodes are participating is only available on the coordinator, so other nodes
     * can not kill the jobs on their own.
     *
     * <pre>
     *              n1                      n2                  n3
     *               |                      |                   |
     *           startJob 1 (n1,n2,n3)      |                   |
     *               |                      |                   |
     *               |                    *dies*                |
     *               |                                          |
     *           onNodeDisc(n2)                            onNodeDisc(n2)
     *            broadcast kill job1                   does not know which jobs involve n2
     *                  |
     *      kill job1 <-+---------------------------------->  kill job1
     *
     * </pre>
     */
    private void broadcastKillToParticipatingNodes(DiscoveryNode deadNode) {
        List<UUID> affectedJobs = tasksService
            .getJobIdsByParticipatingNodes(deadNode.getId()).collect(Collectors.toList());
        if (affectedJobs.isEmpty()) {
            return;
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "Broadcasting kill for {} jobs because they involved disconnected node={}",
                affectedJobs.size(),
                deadNode.getId());
        }
        List<String> excludedNodeIds = Collections.singletonList(deadNode.getId());
        KillJobsNodeRequest killRequest = new KillJobsNodeRequest(
            excludedNodeIds,
            affectedJobs,
            Role.CRATE_USER.name(),
            "Participating node=" + deadNode.getName() + " disconnected."
        );
        killNodeAction
            .execute(killRequest)
            .whenComplete(
                (resp, t) -> {
                    if (t != null) {
                        LOGGER.warn("failed to send kill request to nodes");
                    }
                }
            );
    }

    /**
     * Immediately kills all jobs that were initiated by the disconnected node.
     * It is not possible to send results to the disconnected node.
     * <pre>
     *
     *              n1                      n2                  n3
     *               |                      |                   |
     *           startJob 1 (n1,n2,n3)      |                   |
     *               |                      |                   |
     *              *dies*                  |                   |
     *                                   onNodeDisc(n1)      onNodeDisc(n1)
     *                                    killJob 1            killJob1
     * </pre>
     */
    private void killJobsCoordinatedBy(DiscoveryNode deadNode) {
        List<UUID> jobsStartedByDeadNode = tasksService
            .getJobIdsByCoordinatorNode(deadNode.getId()).collect(Collectors.toList());
        if (jobsStartedByDeadNode.isEmpty()) {
            return;
        }
        tasksService.killJobs(
            jobsStartedByDeadNode,
            Role.CRATE_USER.name(),
            "Participating node=" + deadNode.getName() + " disconnected."
        );
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Killed {} jobs started by disconnected node={}", jobsStartedByDeadNode.size(), deadNode.getId());
        }
    }
}
