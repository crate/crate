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

package io.crate.executor.transport;

import com.google.common.util.concurrent.ListenableFuture;
import io.crate.executor.Job;
import io.crate.executor.transport.task.RemoteCollectTask;
import io.crate.operator.reference.sys.NodeLoadExpression;
import io.crate.planner.plan.CollectNode;
import io.crate.planner.symbol.Reference;
import io.crate.metadata.Routing;
import io.crate.planner.symbol.Symbol;
import org.cratedb.SQLTransportIntegrationTest;
import org.cratedb.test.integration.CrateIntegrationTest;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.core.Is.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class TransportExecutorTest extends SQLTransportIntegrationTest {

    private TransportCollectNodeAction transportCollectNodeAction;
    private ClusterService clusterService;

    @Before
    public void transportSetUp() {
        transportCollectNodeAction = cluster().getInstance(TransportCollectNodeAction.class);
        clusterService = cluster().getInstance(ClusterService.class);
    }

    @Test
    public void testRemoteCollectTask() throws Exception {
        TransportExecutor executor = new TransportExecutor();


        Map<String, Map<String, Integer>> locations = new HashMap<>(2);

        for (DiscoveryNode discoveryNode : clusterService.state().nodes()) {
            locations.put(discoveryNode.id(), null);
        }

        Routing routing = new Routing(locations);
        Symbol reference = new Reference(NodeLoadExpression.INFO_LOAD_1);

        CollectNode collectNode = new CollectNode("collect", routing);
        collectNode.outputs(reference);

        // later created inside executor.newJob
        RemoteCollectTask task = new RemoteCollectTask(collectNode, transportCollectNodeAction);
        Job job = new Job();
        job.addTask(task);

        List<ListenableFuture<Object[][]>> result = executor.execute(job);

        assertThat(result.size(), is(2));
        for (ListenableFuture<Object[][]> nodeResult : result) {
            assertEquals(1, nodeResult.get().length);
            assertEquals(0.4, nodeResult.get()[0][0]);
        }
   }
}
