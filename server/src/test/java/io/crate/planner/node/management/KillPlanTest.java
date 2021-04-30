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

package io.crate.planner.node.management;

import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.execution.jobs.TasksService;
import io.crate.execution.jobs.kill.KillAllRequest;
import io.crate.execution.jobs.kill.KillResponse;
import io.crate.execution.jobs.kill.TransportKillAllNodeAction;
import io.crate.execution.jobs.kill.TransportKillJobsNodeAction;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.TestingRowConsumer;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class KillPlanTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testKillTaskCallsBroadcastOnTransportKillAllNodeAction() {
        AtomicInteger broadcastCalls = new AtomicInteger(0);
        AtomicInteger nodeOperationCalls = new AtomicInteger(0);
        TransportKillAllNodeAction killAllNodeAction = new TransportKillAllNodeAction(
            new TasksService(clusterService, new JobsLogs(() -> false)),
            clusterService,
            mock(TransportService.class)
        ) {
            @Override
            public void broadcast(KillAllRequest request, ActionListener<Long> listener) {
                broadcastCalls.incrementAndGet();
            }

            @Override
            public CompletableFuture<KillResponse> nodeOperation(KillAllRequest request) {
                nodeOperationCalls.incrementAndGet();
                return super.nodeOperation(request);
            }
        };
        KillPlan killPlan = new KillPlan(null);
        killPlan.execute(
            null,
            "dummy-user",
            killAllNodeAction,
            mock(TransportKillJobsNodeAction.class),
            new TestingRowConsumer());
        assertThat(broadcastCalls.get(), is(1));
        assertThat(nodeOperationCalls.get(), is(0));
    }
}
