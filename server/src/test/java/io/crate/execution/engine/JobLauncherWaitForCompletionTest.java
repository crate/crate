/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.engine;

import static org.hamcrest.core.Is.is;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.elasticsearch.indices.IndicesService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import io.crate.data.RowConsumer;
import io.crate.execution.jobs.JobSetup;
import io.crate.execution.jobs.TasksService;
import io.crate.execution.jobs.kill.TransportKillJobsNodeAction;
import io.crate.execution.jobs.transport.TransportJobAction;
import io.crate.metadata.TransactionContext;
import io.crate.planner.PlannerContext;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.TestingRowConsumer;

public class JobLauncherWaitForCompletionTest extends CrateDummyClusterServiceUnitTest {

    private PlannerContext plannerContext;
    private JobLauncher jobLauncher;

    @Before
    public void setupExecutor() throws IOException {
        SQLExecutor e = SQLExecutor.builder(clusterService).build();
        plannerContext = e.getPlannerContext(clusterService.state());
        jobLauncher = new JobLauncher(
            UUID.randomUUID(),
            clusterService,
            Mockito.mock(JobSetup.class),
            Mockito.mock(TasksService.class),
            Mockito.mock(IndicesService.class),
            Mockito.mock(TransportJobAction.class),
            Mockito.mock(TransportKillJobsNodeAction.class),
            List.of(),
            false,
            THREAD_POOL.generic()
        ) {
            @Override
            public void execute(RowConsumer consumer, TransactionContext txnCtx){
            }
        };
    }

    @Test
    public void testCopyPlanNoWaitForCompletion() throws Exception {
        TestingRowConsumer consumer = new TestingRowConsumer();
        jobLauncher.execute(consumer, plannerContext.transactionContext(), false);
        assertThat((Long)consumer.getResult(50).get(0)[0], is(-1L));
    }

    public void testCopyPlanWaitForCompletion() throws Exception {
        TestingRowConsumer consumer = new TestingRowConsumer();
        jobLauncher.execute(consumer, plannerContext.transactionContext(), true);
        assertThrows(TimeoutException.class, ()-> consumer.getResult(100));
    }
}
