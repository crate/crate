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

package io.crate.execution.jobs.transport;

import io.crate.exceptions.ContextMissingException;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.execution.jobs.DummySubContext;
import io.crate.execution.jobs.JobContextService;
import io.crate.execution.jobs.JobExecutionContext;
import io.crate.execution.jobs.kill.KillJobsRequest;
import io.crate.execution.jobs.kill.TransportKillJobsNodeAction;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import java.util.Arrays;
import java.util.UUID;

import static io.crate.testing.DiscoveryNodes.newNode;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class NodeDisconnectJobMonitorServiceTest extends CrateDummyClusterServiceUnitTest {

    private JobContextService jobContextService() throws Exception {
        return new JobContextService(Settings.EMPTY, clusterService, new JobsLogs(() -> true));
    }

    @Test
    public void testOnNodeDisconnectedKillsJobOriginatingFromThatNode() throws Exception {
        JobContextService jobContextService = jobContextService();
        JobExecutionContext.Builder builder = jobContextService.newBuilder(UUID.randomUUID());
        builder.addSubContext(new DummySubContext());
        JobExecutionContext context = jobContextService.createContext(builder);

        NodeDisconnectJobMonitorService monitorService = new NodeDisconnectJobMonitorService(
            Settings.EMPTY,
            jobContextService,
            mock(TransportService.class),
            mock(TransportKillJobsNodeAction.class));

        monitorService.onNodeDisconnected(new DiscoveryNode(
            NODE_ID,
            buildNewFakeTransportAddress(),
            Version.CURRENT));

        expectedException.expect(ContextMissingException.class);
        jobContextService.getContext(context.jobId());
    }

    @Test
    public void testOnParticipatingNodeDisconnectedKillsJob() throws Exception {
        JobContextService jobContextService = jobContextService();

        DiscoveryNode coordinator = newNode("coordinator");
        DiscoveryNode dataNode = newNode("dataNode");

        JobExecutionContext.Builder builder = jobContextService.newBuilder(
            UUID.randomUUID(),
            coordinator.getId(),
            Arrays.asList(coordinator.getId(), dataNode.getId())
        );
        builder.addSubContext(new DummySubContext());
        jobContextService.createContext(builder);

        // add a second job that is coordinated by the other node to make sure the the broadcast logic is run
        // even though there are jobs coordinated by the disconnected node
        builder = jobContextService.newBuilder(UUID.randomUUID(), dataNode.getId());
        builder.addSubContext(new DummySubContext());
        jobContextService.createContext(builder);

        TransportKillJobsNodeAction killAction = mock(TransportKillJobsNodeAction.class);
        NodeDisconnectJobMonitorService monitorService = new NodeDisconnectJobMonitorService(
            Settings.EMPTY,
            jobContextService,
            mock(TransportService.class),
            killAction);

        monitorService.onNodeDisconnected(dataNode);

        verify(killAction, times(1)).broadcast(
            any(KillJobsRequest.class),
            any(ActionListener.class),
            eq(Arrays.asList(dataNode.getId())));
    }
}
