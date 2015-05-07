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

package io.crate.executor.transport.task;

import io.crate.executor.transport.kill.KillAllRequest;
import io.crate.executor.transport.kill.TransportKillAllNodeAction;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.junit.Test;

import java.util.UUID;

import static org.mockito.Mockito.*;

public class KillTaskTest extends CrateUnitTest {


    @SuppressWarnings("unchecked")
    @Test
    public void testExecuteIsCalledOnTransportForEachNode() throws Exception {
        ClusterService clusterService = mock(ClusterService.class);
        TransportKillAllNodeAction killNodeAction = mock(TransportKillAllNodeAction.class);

        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .put(new DiscoveryNode("n1", DummyTransportAddress.INSTANCE, Version.CURRENT))
                .put(new DiscoveryNode("n2", DummyTransportAddress.INSTANCE, Version.CURRENT))
                .put(new DiscoveryNode("n3", DummyTransportAddress.INSTANCE, Version.CURRENT)).build();

        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);
        when(state.nodes()).thenReturn(nodes);

        KillTask killTask = new KillTask(clusterService, killNodeAction, UUID.randomUUID());
        killTask.start();

        verify(killNodeAction, times(1)).execute(eq("n1"), any(KillAllRequest.class), any(ActionListener.class));
        verify(killNodeAction, times(1)).execute(eq("n2"), any(KillAllRequest.class), any(ActionListener.class));
        verify(killNodeAction, times(1)).execute(eq("n3"), any(KillAllRequest.class), any(ActionListener.class));
    }
}