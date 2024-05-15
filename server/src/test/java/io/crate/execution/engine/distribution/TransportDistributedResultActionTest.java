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

package io.crate.execution.engine.distribution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;
import org.junit.Test;

import io.crate.Streamer;
import io.crate.common.unit.TimeValue;
import io.crate.data.breaker.RamAccounting;
import io.crate.exceptions.TaskMissing;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.execution.jobs.TasksService;
import io.crate.execution.jobs.kill.KillJobsNodeRequest;
import io.crate.execution.jobs.kill.KillResponse;
import io.crate.execution.jobs.kill.TransportKillJobsNodeAction;
import io.crate.execution.support.Transports;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class TransportDistributedResultActionTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testKillIsInvokedIfContextIsNotFound() throws Exception {
        TasksService tasksService = new TasksService(clusterService, new JobsLogs(() -> false));
        AtomicInteger numBroadcasts = new AtomicInteger(0);
        TransportKillJobsNodeAction killJobsAction = new TransportKillJobsNodeAction(
            tasksService,
            clusterService,
            mock(TransportService.class)
        ) {
            @Override
            public void doExecute(KillJobsNodeRequest request, ActionListener<KillResponse> listener) {
                numBroadcasts.incrementAndGet();
            }
        };
        TransportDistributedResultAction transportDistributedResultAction = new TransportDistributedResultAction(
            mock(Transports.class),
            tasksService,
            THREAD_POOL,
            mock(TransportService.class),
            clusterService,
            req -> {
                return killJobsAction.execute(req);
            },
            BackoffPolicy.exponentialBackoff(TimeValue.ZERO, 0)
        );

        StreamBucket.Builder builder = new StreamBucket.Builder(
            new Streamer[0], RamAccounting.NO_ACCOUNTING);
        try {
            transportDistributedResultAction.nodeOperation(
                DistributedResultRequest.of(
                    "dummyNodeId", UUID.randomUUID(), 0, (byte) 0, 0, builder.build(), true
                ).innerRequest()
            ).get(5, TimeUnit.SECONDS);
            fail("nodeOperation call should fail with TaskMissing");
        } catch (ExecutionException e) {
            assertThat(e.getCause(), Matchers.instanceOf(TaskMissing.class));
        }

        assertThat(numBroadcasts.get()).isEqualTo(1);
    }
}
