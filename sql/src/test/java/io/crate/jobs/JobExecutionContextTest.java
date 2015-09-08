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

import com.google.common.base.Throwables;
import io.crate.Streamer;
import io.crate.breaker.RamAccountingContext;
import io.crate.operation.PageDownstream;
import io.crate.operation.RowDownstream;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.MapSideDataCollectOperation;
import io.crate.operation.collect.StatsTables;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.IntegerType;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.ElasticsearchTestCase.assertBusy;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.*;

public class JobExecutionContextTest extends CrateUnitTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private ThreadPool threadPool;


    @Before
    public void before() throws Exception {
        threadPool = new org.elasticsearch.threadpool.ThreadPool("dummy");
    }

    @After
    public void after() throws Exception {
        threadPool.shutdown();
        threadPool.awaitTermination(500, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testAddTheSameContextTwiceThrowsAnError() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("ExecutionSubContext for 1 already added");

        JobExecutionContext.Builder builder =
                new JobExecutionContext.Builder(UUID.randomUUID(), threadPool, mock(StatsTables.class));

        builder.addSubContext(1, mock(PageDownstreamContext.class));
        builder.addSubContext(1, mock(PageDownstreamContext.class));
    }


    @Test
    public void testKillPropagatesToSubContexts() throws Exception {
        JobExecutionContext.Builder builder =
                new JobExecutionContext.Builder(UUID.randomUUID(), threadPool, mock(StatsTables.class));

        PageDownstreamContext pageDownstreamContext = mock(PageDownstreamContext.class);
        builder.addSubContext(1, pageDownstreamContext);
        builder.addSubContext(2, mock(PageDownstreamContext.class));
        JobExecutionContext jobExecutionContext = builder.build();

        assertThat(jobExecutionContext.kill(), is(2L));
        assertThat(jobExecutionContext.kill(), is(0L)); // second call is ignored, only killed once

        verify(pageDownstreamContext, times(1)).kill(null);
    }

    @Test
    public void testFailureClosesAllSubContexts() throws Exception {
        UUID jobId = UUID.randomUUID();
        JobExecutionContext.Builder builder =
                new JobExecutionContext.Builder(UUID.randomUUID(), threadPool, mock(StatsTables.class));

        JobCollectContext jobCollectContext = new JobCollectContext(
                jobId,
                mock(CollectPhase.class),
                mock(MapSideDataCollectOperation.class),
                mock(RamAccountingContext.class),
                mock(RowDownstream.class));
        PageDownstreamContext pageDownstreamContext = spy(new PageDownstreamContext(
                "dummy",
                mock(PageDownstream.class),
                new Streamer[]{IntegerType.INSTANCE.streamer()},
                mock(RamAccountingContext.class),
                1,
                null));

        builder.addSubContext(1, jobCollectContext);
        builder.addSubContext(2, pageDownstreamContext);
        JobExecutionContext jobExecutionContext = builder.build();

        Exception failure = new Exception("failure!");
        jobCollectContext.closeDueToFailure(failure);
        // other contexts must be killed with same failure
        verify(pageDownstreamContext, times(1)).kill(failure);

        final Field subContexts = JobExecutionContext.class.getDeclaredField("subContexts");
        subContexts.setAccessible(true);
        int size = ((ConcurrentMap<Integer, ExecutionSubContext>) subContexts.get(jobExecutionContext)).size();

        assertThat(size, is(0));
    }

    @Test
    public void testParallelKill() throws Exception {
        final Field subContexts = JobExecutionContext.class.getDeclaredField("subContexts");
        subContexts.setAccessible(true);

        JobExecutionContext.Builder builder =
                new JobExecutionContext.Builder(UUID.randomUUID(), threadPool, mock(StatsTables.class));
        SlowKillExecutionSubContext slowKillExecutionSubContext = new SlowKillExecutionSubContext();
        builder.addSubContext(1, slowKillExecutionSubContext);
        final JobExecutionContext jobExecutionContext = builder.build();
        jobExecutionContext.start();
        Thread killThread = new Thread(new Runnable() {
            @Override
            public void run() {
                jobExecutionContext.kill();
            }
        });
        killThread.start();

        // wait until kill is started
        final Field closed = JobExecutionContext.class.getDeclaredField("closed");
        closed.setAccessible(true);
        assertBusy(new Runnable() {
            @Override
            public void run() {
                try {
                    assertThat(((AtomicBoolean) closed.get(jobExecutionContext)).get(), is(true));
                } catch (Throwable t) {
                    throw Throwables.propagate(t);
                }
            }
        }, 100, TimeUnit.MILLISECONDS);


        // call kill again, because the first kill is still in progress nothing is done here, but this kill should
        // not return before every subContext is killed
        jobExecutionContext.kill();
        int size = ((ConcurrentMap<Integer, ExecutionSubContext>) subContexts.get(jobExecutionContext)).size();
        killThread.join();

        assertThat(size, is(0));
    }

    private static class SlowKillExecutionSubContext implements ExecutionSubContext {

        private ContextCallback contextCallback;

        @Override
        public void addCallback(ContextCallback contextCallback) {
            this.contextCallback = contextCallback;
        }

        @Override
        public void prepare() {}

        @Override
        public void start() {}

        @Override
        public void close() {}

        @Override
        public void kill(@Nullable Throwable throwable) {
            try {
                Thread.sleep(200);
                if (throwable == null) {
                    throwable = new CancellationException();
                }
                contextCallback.onClose(throwable, -1L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public String name() {
            return "SlowKilLExecutionSubContext";
        }
    }
}