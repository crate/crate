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
import io.crate.executor.transport.BaseTransportExecutorTest;
import io.crate.jobs.ESGetContext;
import io.crate.jobs.ExecutionSubContext;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.metadata.Functions;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.planner.node.dql.ESGetNode;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingHelpers;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.get.TransportMultiGetAction;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ESGetTaskTest extends CrateUnitTest {

    private final ThreadPool testThreadPool = TestingHelpers.newMockedThreadPool();
    private final Settings settings = ImmutableSettings.EMPTY;
    private final JobContextService jobContextService = new JobContextService(
            settings, testThreadPool);

    @After
    public void cleanUp() throws Exception {
        testThreadPool.shutdown();
    }

    @Test
    public void testContextCreation() throws Exception {
        TableIdent tableIdent = new TableIdent("doc", "dummy");
        TableInfo tableInfo = mock(DocTableInfo.class);
        when(tableInfo.ident()).thenReturn(tableIdent);
        ESGetNode node = BaseTransportExecutorTest.newGetNode(
                tableInfo,
                ImmutableList.<Symbol>of(),
                ImmutableList.of("1"),
                1
        );

        UUID jobId = UUID.randomUUID();
        ESGetTask task = new ESGetTask(
                jobId,
                mock(Functions.class),
                mock(ProjectionToProjectorVisitor.class),
                mock(TransportMultiGetAction.class),
                mock(TransportGetAction.class),
                node,
                jobContextService);

        JobExecutionContext jobExecutionContext = jobContextService.getContext(jobId);
        ExecutionSubContext subContext = jobExecutionContext.getSubContext(node.executionNodeId());
        assertThat(subContext, notNullValue());
        assertThat(subContext, instanceOf(ESGetContext.class));
    }
}
