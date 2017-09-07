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

package io.crate.jobs.transport;

import io.crate.executor.transport.kill.KillJobsRequest;
import io.crate.executor.transport.kill.KillResponse;
import io.crate.executor.transport.kill.TransportKillJobsNodeAction;
import io.crate.jobs.JobContextService;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportConnectionListener;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * service that listens to node-disconnected-events and kills jobContexts that were started by the nodes that got disconnected
 */
@Singleton
public class NodeDisconnectJobMonitorService extends AbstractLifecycleComponent implements TransportConnectionListener {

    private final ThreadPool threadPool;
    private final JobContextService jobContextService;
    private final TransportService transportService;

    private static final TimeValue DELAY = TimeValue.timeValueMinutes(1);
    private final TransportKillJobsNodeAction killJobsNodeAction;
    private static final Logger LOGGER = Loggers.getLogger(NodeDisconnectJobMonitorService.class);

    @Inject
    public NodeDisconnectJobMonitorService(Settings settings,
                                           ThreadPool threadPool,
                                           JobContextService jobContextService,
                                           TransportService transportService,
                                           TransportKillJobsNodeAction killJobsNodeAction) {
        super(settings);
        this.threadPool = threadPool;
        this.jobContextService = jobContextService;
        this.transportService = transportService;
        this.killJobsNodeAction = killJobsNodeAction;
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
    public void onNodeConnected(DiscoveryNode node) {
    }

    @Override
    public void onNodeDisconnected(final DiscoveryNode node) {
        final Collection<UUID> contexts = jobContextService.getJobIdsByCoordinatorNode(node.getId()).collect(Collectors.toList());
        if (contexts.isEmpty()) {
            // Disconnected node is not a handler node --> kill jobs on all participated nodes
            contexts.addAll(jobContextService.getJobIdsByParticipatingNodes(node.getId()).collect(Collectors.toList()));
            KillJobsRequest killJobsRequest = new KillJobsRequest(contexts);
            if (!contexts.isEmpty()) {
                killJobsNodeAction.broadcast(killJobsRequest, new ActionListener<KillResponse>() {
                    @Override
                    public void onResponse(KillResponse killResponse) {
                    }

                    @Override
                    public void onFailure(Exception e) {
                        LOGGER.warn("failed to send kill request to nodes");
                    }
                }, Arrays.asList(node.getId()));
            } else {
                return;
            }
        }

        threadPool.schedule(DELAY, ThreadPool.Names.GENERIC, () -> jobContextService.killJobs(contexts));
    }
}
