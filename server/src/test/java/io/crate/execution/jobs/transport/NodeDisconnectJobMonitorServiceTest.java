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

import static io.crate.testing.DiscoveryNodes.newNode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import io.crate.exceptions.TaskMissing;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.execution.jobs.DummyTask;
import io.crate.execution.jobs.NodeLimits;
import io.crate.execution.jobs.RootTask;
import io.crate.execution.jobs.TasksService;
import io.crate.execution.jobs.kill.KillJobsNodeRequest;
import io.crate.execution.jobs.kill.KillResponse;
import io.crate.execution.jobs.kill.TransportKillJobsNodeAction;
import io.crate.execution.support.ActionExecutor;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class NodeDisconnectJobMonitorServiceTest extends CrateDummyClusterServiceUnitTest {

    private TasksService tasksInstance() throws Exception {
        return new TasksService(clusterService, new JobsLogs(() -> true));
    }

    @Test
    public void testOnNodeDisconnectedKillsJobOriginatingFromThatNode() throws Exception {
        TasksService tasksService = tasksInstance();
        RootTask.Builder builder = tasksService.newBuilder(UUID.randomUUID());
        builder.addTask(new DummyTask());
        RootTask context = tasksService.createTask(builder);

        NodeDisconnectJobMonitorService monitorService = new NodeDisconnectJobMonitorService(
            tasksService,
            new NodeLimits(new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            mock(TransportService.class),
            mock(ActionExecutor.class));

        monitorService.onNodeDisconnected(
            new DiscoveryNode(NODE_ID, buildNewFakeTransportAddress(), Version.CURRENT),
            mock(Transport.Connection.class)
        );
        assertThatThrownBy(() -> tasksService.getTask(context.jobId()))
            .isExactlyInstanceOf(TaskMissing.class);
        monitorService.close();
    }

    @Test
    public void testOnParticipatingNodeDisconnectedKillsJob() throws Exception {
        TasksService tasksService = tasksInstance();

        DiscoveryNode coordinator = newNode("coordinator");
        DiscoveryNode dataNode = newNode("dataNode");

        RootTask.Builder builder = tasksService.newBuilder(UUID.randomUUID(), "dummy-user", coordinator.getId(), Arrays.asList(coordinator.getId(), dataNode.getId()));
        builder.addTask(new DummyTask());
        tasksService.createTask(builder);

        // add a second job that is coordinated by the other node to make sure the the broadcast logic is run
        // even though there are jobs coordinated by the disconnected node
        builder = tasksService.newBuilder(UUID.randomUUID(), "dummy-user", dataNode.getId(), Collections.emptySet());
        builder.addTask(new DummyTask());
        tasksService.createTask(builder);

        AtomicInteger broadcasts = new AtomicInteger(0);
        TransportKillJobsNodeAction killAction = new TransportKillJobsNodeAction(
            tasksService,
            clusterService,
            mock(TransportService.class)
        ) {
            @Override
            public void doExecute(KillJobsNodeRequest request, ActionListener<KillResponse> listener) {
                broadcasts.incrementAndGet();
            }
        };
        NodeDisconnectJobMonitorService monitorService = new NodeDisconnectJobMonitorService(
            tasksService,
            new NodeLimits(new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            mock(TransportService.class),
            req -> {
                return killAction.execute(req);
            });

        monitorService.onNodeDisconnected(dataNode, mock(Transport.Connection.class));

        assertThat(broadcasts.get()).isEqualTo(1);
        monitorService.close();
    }
}
