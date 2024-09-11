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

package io.crate.execution.jobs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;
import org.mockito.Mockito;

import io.crate.Streamer;
import io.crate.data.breaker.RamAccounting;
import io.crate.data.testing.TestingRowConsumer;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.engine.collect.CollectTask;
import io.crate.execution.engine.collect.MapSideDataCollectOperation;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.execution.engine.distribution.merge.PassThroughPagingIterator;
import io.crate.memory.OnHeapMemoryManager;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.profile.ProfilingContext;
import io.crate.types.IntegerType;

public class RootTaskTest extends ESTestCase {

    private Logger logger = LogManager.getLogger(RootTaskTest.class);

    private String coordinatorNode = "dummyNode";

    @Test
    public void testKillPropagatesToSubContexts() throws Exception {
        RootTask.Builder builder =
            new RootTask.Builder(logger, UUID.randomUUID(), "dummy-user", coordinatorNode, Collections.emptySet(), mock(JobsLogs.class));


        AbstractTaskTest.TestingTask ctx1 = new AbstractTaskTest.TestingTask(1);
        AbstractTaskTest.TestingTask ctx2 = new AbstractTaskTest.TestingTask(2);

        builder.addTask(ctx1);
        builder.addTask(ctx2);
        RootTask rootTask = builder.build();

        assertThat(rootTask.kill(null)).isGreaterThanOrEqualTo(1);
        assertThat(rootTask.kill(null)).isZero(); // Everything is killed already

        assertThat(ctx1.numKill.get()).isEqualTo(1);
        assertThat(ctx2.numKill.get()).isEqualTo(1);
    }

    @Test
    public void testErrorMessageIsIncludedInStatsTableOnFailure() throws Throwable {
        JobsLogs jobsLogs = mock(JobsLogs.class);
        RootTask.Builder builder =
            new RootTask.Builder(logger, UUID.randomUUID(), "dummy-user", coordinatorNode, Collections.emptySet(), jobsLogs);

        Task task = new AbstractTask(0) {
            @Override
            public String name() {
                return "dummy";
            }

            @Override
            public long bytesUsed() {
                return -1;
            }
        };
        builder.addTask(task);
        RootTask rootTask = builder.build();

        rootTask.start();
        task.kill(new IllegalStateException("dummy"));

        verify(jobsLogs).operationFinished(anyInt(), any(UUID.class), eq("dummy"));
    }

    @Test
    public void testFailureClosesAllSubContexts() throws Throwable {
        String localNodeId = "localNodeId";
        RoutedCollectPhase collectPhase = Mockito.mock(RoutedCollectPhase.class);
        Routing routing = Mockito.mock(Routing.class);
        when(routing.containsShards(localNodeId)).thenReturn(false);
        when(collectPhase.phaseId()).thenReturn(1);
        when(collectPhase.routing()).thenReturn(routing);
        when(collectPhase.maxRowGranularity()).thenReturn(RowGranularity.DOC);

        RootTask.Builder builder =
            new RootTask.Builder(logger, UUID.randomUUID(), "dummy-user", coordinatorNode, Collections.emptySet(), mock(JobsLogs.class));

        CollectTask collectChildTask = new CollectTask(
            collectPhase,
            CoordinatorTxnCtx.systemTransactionContext(),
            mock(MapSideDataCollectOperation.class),
            RamAccounting.NO_ACCOUNTING,
            ramAccounting -> new OnHeapMemoryManager(ramAccounting::addBytes),
            new TestingRowConsumer(),
            mock(SharedShardContexts.class),
            Version.CURRENT,
            4096
        );
        TestingRowConsumer batchConsumer = new TestingRowConsumer();

        PageBucketReceiver pageBucketReceiver = new CumulativePageBucketReceiver(
            "n1",
            2,
            Runnable::run,
            new Streamer[]{IntegerType.INSTANCE.streamer()},
            batchConsumer,
            PassThroughPagingIterator.oneShot(),
            1);
        DistResultRXTask distResultRXTask = spy(new DistResultRXTask(
            2,
            "dummy",
            pageBucketReceiver,
            RamAccounting.NO_ACCOUNTING,
            1));

        builder.addTask(collectChildTask);
        builder.addTask(distResultRXTask);
        RootTask rootTask = builder.build();

        Exception failure = new Exception("failure!");
        collectChildTask.kill(failure);
        // other contexts must be killed with same failure
        verify(distResultRXTask, times(1)).kill(failure);

        assertThat(rootTask.getTask(1).completionFuture().isDone()).isTrue();
        assertThat(rootTask.getTask(2).completionFuture().isDone()).isTrue();
    }

    @Test
    public void testEnablingProfilingGathersExecutionTimes() throws Throwable {
        RootTask.Builder builder =
            new RootTask.Builder(logger, UUID.randomUUID(), "dummy-user", coordinatorNode, Collections.emptySet(), mock(JobsLogs.class));
        ProfilingContext profilingContext = new ProfilingContext(Map.of());
        builder.profilingContext(profilingContext);

        AbstractTaskTest.TestingTask ctx1 = new AbstractTaskTest.TestingTask(1);
        builder.addTask(ctx1);
        AbstractTaskTest.TestingTask ctx2 = new AbstractTaskTest.TestingTask(2);
        builder.addTask(ctx2);
        RootTask rootTask = builder.build();

        rootTask.start();
        // fake execution time so we can sure the measurement is > 0
        Thread.sleep(5L);
        // kill because the testing subcontexts would run infinitely
        rootTask.kill(null);
        assertThat(rootTask.executionTimes()).containsKeys("1-TestingTask", "2-TestingTask");
        assertThat(((double) rootTask.executionTimes().get("1-TestingTask"))).isGreaterThan(0);
        assertThat(((double) rootTask.executionTimes().get("2-TestingTask"))).isGreaterThan(0);
        assertThat(rootTask.completionFuture()).isCompletedExceptionally();
    }
}
