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

package io.crate.jobs;

import io.crate.Streamer;
import io.crate.action.job.SharedShardContexts;
import io.crate.breaker.RamAccountingContext;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.MapSideDataCollectOperation;
import io.crate.operation.collect.stats.JobsLogs;
import io.crate.operation.merge.PassThroughPagingIterator;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingBatchConsumer;
import io.crate.types.IntegerType;
import org.elasticsearch.common.logging.Loggers;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.*;

public class JobExecutionContextTest extends CrateUnitTest {

    private String coordinatorNode = "dummyNode";

    @Test
    public void testAddTheSameContextTwiceThrowsAnError() throws Exception {
        JobExecutionContext.Builder builder =
            new JobExecutionContext.Builder(UUID.randomUUID(), coordinatorNode, Collections.emptyList(), mock(JobsLogs.class));
        builder.addSubContext(new AbstractExecutionSubContextTest.TestingExecutionSubContext());
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("ExecutionSubContext for 0 already added");
        builder.addSubContext(new AbstractExecutionSubContextTest.TestingExecutionSubContext());
        builder.build();
    }

    @Test
    public void testKillPropagatesToSubContexts() throws Exception {
        JobExecutionContext.Builder builder =
            new JobExecutionContext.Builder(UUID.randomUUID(), coordinatorNode, Collections.emptyList(), mock(JobsLogs.class));


        AbstractExecutionSubContextTest.TestingExecutionSubContext ctx1 = new AbstractExecutionSubContextTest.TestingExecutionSubContext(1);
        AbstractExecutionSubContextTest.TestingExecutionSubContext ctx2 = new AbstractExecutionSubContextTest.TestingExecutionSubContext(2);

        builder.addSubContext(ctx1);
        builder.addSubContext(ctx2);
        JobExecutionContext jobExecutionContext = builder.build();

        assertThat(jobExecutionContext.kill(), is(2L));
        assertThat(jobExecutionContext.kill(), is(0L)); // second call is ignored, only killed once

        assertThat(ctx1.numKill.get(), is(1));
        assertThat(ctx2.numKill.get(), is(1));
    }

    @Test
    public void testErrorMessageIsIncludedInStatsTableOnFailure() throws Exception {
        JobsLogs jobsLogs = mock(JobsLogs.class);
        JobExecutionContext.Builder builder =
            new JobExecutionContext.Builder(UUID.randomUUID(), coordinatorNode, Collections.emptyList(), jobsLogs);

        ExecutionSubContext executionSubContext = new AbstractExecutionSubContext(0, logger) {
            @Override
            public String name() {
                return "dummy";
            }
        };
        builder.addSubContext(executionSubContext);
        builder.build();

        executionSubContext.kill(new IllegalStateException("dummy"));
        verify(jobsLogs).operationFinished(anyInt(), any(UUID.class), eq("dummy"), anyLong());
    }

    @Test
    public void testFailureClosesAllSubContexts() throws Exception {
        String localNodeId = "localNodeId";
        RoutedCollectPhase collectPhase = Mockito.mock(RoutedCollectPhase.class);
        Routing routing = Mockito.mock(Routing.class);
        when(routing.containsShards(localNodeId)).thenReturn(false);
        when(collectPhase.routing()).thenReturn(routing);
        when(collectPhase.maxRowGranularity()).thenReturn(RowGranularity.DOC);

        JobExecutionContext.Builder builder =
            new JobExecutionContext.Builder(UUID.randomUUID(), coordinatorNode, Collections.emptyList(), mock(JobsLogs.class));

        JobCollectContext jobCollectContext = new JobCollectContext(
            collectPhase,
            mock(MapSideDataCollectOperation.class),
            localNodeId,
            mock(RamAccountingContext.class),
            new TestingBatchConsumer(),
            mock(SharedShardContexts.class));
        TestingBatchConsumer batchConsumer = new TestingBatchConsumer();
        PageDownstreamContext pageDownstreamContext = spy(new PageDownstreamContext(
            Loggers.getLogger(PageDownstreamContext.class),
            "n1",
            2, "dummy",
            batchConsumer,
            PassThroughPagingIterator.oneShot(),
            new Streamer[]{IntegerType.INSTANCE.streamer()},
            mock(RamAccountingContext.class),
            1));

        builder.addSubContext(jobCollectContext);
        builder.addSubContext(pageDownstreamContext);
        JobExecutionContext jobExecutionContext = builder.build();

        Exception failure = new Exception("failure!");
        jobCollectContext.close(failure);
        // other contexts must be killed with same failure
        verify(pageDownstreamContext, times(1)).innerKill(failure);

        final Field subContexts = JobExecutionContext.class.getDeclaredField("subContexts");
        subContexts.setAccessible(true);
        int size = ((ConcurrentMap<Integer, ExecutionSubContext>) subContexts.get(jobExecutionContext)).size();

        assertThat(size, is(0));
    }
}
