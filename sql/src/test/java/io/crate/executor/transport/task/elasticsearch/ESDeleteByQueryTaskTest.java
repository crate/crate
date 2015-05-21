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

package io.crate.executor.transport.task.elasticsearch;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.WhereClause;
import io.crate.jobs.ESJobContext;
import io.crate.jobs.ExecutionSubContext;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.operation.collect.StatsTables;
import io.crate.planner.node.dml.ESDeleteByQueryNode;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingHelpers;
import org.elasticsearch.action.deletebyquery.TransportDeleteByQueryAction;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;

public class ESDeleteByQueryTaskTest extends CrateUnitTest {

    private static ESDeleteByQueryNode node = new ESDeleteByQueryNode(
                1,
                ImmutableList.of(new String[]{"dummy"}),
                ImmutableList.of(WhereClause.MATCH_ALL));

    private final ThreadPool testThreadPool = TestingHelpers.newMockedThreadPool();
    private final JobContextService jobContextService = new JobContextService(
            ImmutableSettings.EMPTY, testThreadPool, mock(StatsTables.class));

    @After
    public void cleanUp() throws Exception {
        testThreadPool.shutdown();
    }

    @Test
    public void testContextCreation() throws Exception {
        UUID jobId = UUID.randomUUID();
        ESDeleteByQueryTask task = new ESDeleteByQueryTask(
                jobId,
                node,
                mock(TransportDeleteByQueryAction.class),
                jobContextService);

        JobExecutionContext jobExecutionContext = jobContextService.getContext(jobId);
        ExecutionSubContext subContext = jobExecutionContext.getSubContext(node.executionNodeId());
        assertThat(subContext, notNullValue());
        assertThat(subContext, instanceOf(ESJobContext.class));
    }

    @Test
    public void testContextKill() throws Exception {
        UUID jobId = UUID.randomUUID();
        ESDeleteByQueryTask task = new ESDeleteByQueryTask(
                jobId,
                node,
                mock(TransportDeleteByQueryAction.class),
                jobContextService);

        JobExecutionContext jobExecutionContext = jobContextService.getContext(jobId);
        ExecutionSubContext subContext = jobExecutionContext.getSubContext(node.executionNodeId());
        subContext.kill();

        assertThat(task.result().size(), is(1));
        assertThat(task.result().get(0).isCancelled(), is(true));
        assertNull(jobExecutionContext.getSubContextOrNull(node.executionNodeId()));
    }
}
