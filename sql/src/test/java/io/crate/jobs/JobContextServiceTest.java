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

import com.google.common.collect.ImmutableList;
import io.crate.Streamer;
import io.crate.breaker.RamAccountingContext;
import io.crate.operation.PageDownstream;
import io.crate.operation.collect.StatsTables;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Test;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.*;

public class JobContextServiceTest extends CrateUnitTest {

    private static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
            new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.Name.FIELDDATA));

    private final ThreadPool testThreadPool = new ThreadPool(getClass().getSimpleName());
    private final Settings settings = ImmutableSettings.EMPTY;
    private final JobContextService jobContextService = new JobContextService(
            settings, testThreadPool, mock(StatsTables.class));

    @After
    public void cleanUp() throws Exception {
        jobContextService.close();
        testThreadPool.shutdown();
    }

    @Test
    public void testAcquireContext() throws Exception {
        // create new context
        UUID jobId = UUID.randomUUID();
        JobExecutionContext.Builder builder1 = jobContextService.newBuilder(jobId);
        ExecutionSubContext subContext = mock(PageDownstreamContext.class);
        builder1.addSubContext(1, subContext);
        JobExecutionContext ctx1 = jobContextService.createContext(builder1);
        assertThat(ctx1.getSubContext(1), is(subContext));
    }

    @Test
    public void testAcquireContextSameJobId() throws Exception {
        UUID jobId = UUID.randomUUID();

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(String.format(Locale.ENGLISH,
                "context for job %s already exists", jobId));

        // create new context
        JobExecutionContext.Builder builder1 = jobContextService.newBuilder(jobId);
        builder1.addSubContext(1, mock(PageDownstreamContext.class));
        jobContextService.createContext(builder1);

        // creating a context with the same jobId will fail
        JobExecutionContext.Builder builder2 = jobContextService.newBuilder(jobId);
        builder2.addSubContext(2, mock(PageDownstreamContext.class));

        jobContextService.createContext(builder2);
    }

    @Test
    public void testCreateCallWithEmptyBuilderThrowsAnError() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("JobExecutionContext.Builder must at least contain 1 SubExecutionContext");

        JobExecutionContext.Builder builder = jobContextService.newBuilder(UUID.randomUUID());
        jobContextService.createContext(builder);
    }

    @Test
    public void testAccessContext() throws Exception {
        JobExecutionContext ctx1 = getJobExecutionContextWithOneActiveSubContext(jobContextService);
        long firstAccessTime = ctx1.lastAccessTime();
        Thread.sleep(205); // sleep for > 200 ms to ensure that the estimatedTime in ThreadPool is updated
        ctx1.getSubContextOrNull(1);
        assertThat(ctx1.lastAccessTime(), greaterThan(firstAccessTime));
    }

    @Test
    public void testKillAllCallsKillOnSubContext() throws Exception {
        final AtomicBoolean killCalled = new AtomicBoolean(false);
        ExecutionSubContext dummyContext = new DummySubContext() {

            @Override
            public void kill(@Nullable Throwable throwable) {
                super.kill(throwable);
                killCalled.set(true);
            }
        };

        JobExecutionContext.Builder builder = jobContextService.newBuilder(UUID.randomUUID());
        builder.addSubContext(1, dummyContext);
        jobContextService.createContext(builder);

        Field activeContextsField = JobContextService.class.getDeclaredField("activeContexts");
        activeContextsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<UUID, JobExecutionContext> activeContexts = (Map<UUID, JobExecutionContext>) activeContextsField.get(jobContextService);
        assertThat(activeContexts.size(), is(1));
        assertThat(jobContextService.killAll(), is(1L));

        assertThat(killCalled.get(), is(true));
        assertThat(activeContexts.size(), is(0));
    }

    @Test
    public void testKillJobsCallsKillOnSubContext() throws Exception {
        final AtomicBoolean killCalled = new AtomicBoolean(false);
        final AtomicBoolean kill2Called = new AtomicBoolean(false);
        ExecutionSubContext dummyContext = new DummySubContext() {

            @Override
            public void kill(@Nullable Throwable throwable) {
                super.kill(throwable);
                killCalled.set(true);
            }
        };

        UUID jobId = UUID.randomUUID();
        JobExecutionContext.Builder builder = jobContextService.newBuilder(jobId);
        builder.addSubContext(1, dummyContext);
        jobContextService.createContext(builder);

        builder = jobContextService.newBuilder(UUID.randomUUID());
        builder.addSubContext(1, new DummySubContext() {
            @Override
            public void kill(@Nullable Throwable throwable) {
                super.kill(throwable);
                kill2Called.set(true);
            }
        });
        jobContextService.createContext(builder);

        Field activeContextsField = JobContextService.class.getDeclaredField("activeContexts");
        activeContextsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<UUID, JobExecutionContext> activeContexts = (Map<UUID, JobExecutionContext>) activeContextsField.get(jobContextService);
        assertThat(activeContexts.size(), is(2));
        assertThat(jobContextService.killJobs(ImmutableList.of(jobId)), is(1L));

        assertThat(killCalled.get(), is(true));
        assertThat(kill2Called.get(), is(false));
        assertThat(activeContexts.size(), is(1)); //only one job is killed

    }

    @Test
    public void testJobExecutionContextIsSelfClosing() throws Exception {
        JobExecutionContext.Builder builder1 = jobContextService.newBuilder(UUID.randomUUID());
        PageDownstreamContext pageDownstreamContext =
                new PageDownstreamContext("dummy", mock(PageDownstream.class), new Streamer[0], RAM_ACCOUNTING_CONTEXT, 1, mock(FlatProjectorChain.class));
        builder1.addSubContext(1, pageDownstreamContext);
        JobExecutionContext ctx1 = jobContextService.createContext(builder1);

        Field activeSubContexts = JobExecutionContext.class.getDeclaredField("activeSubContexts");
        activeSubContexts.setAccessible(true);
        assertThat(((AtomicInteger) activeSubContexts.get(ctx1)).get(), is(1));

        pageDownstreamContext.finish();

        assertThat(((AtomicInteger) activeSubContexts.get(ctx1)).get(), is(0));
    }

    @Test
    public void testKillReturnsNumberOfJobsKilled() throws Exception {
        JobExecutionContext.Builder builder = jobContextService.newBuilder(UUID.randomUUID());
        builder.addSubContext(1, new DummySubContext());
        builder.addSubContext(2, new DummySubContext());
        builder.addSubContext(3, new DummySubContext());
        builder.addSubContext(4, new DummySubContext());
        jobContextService.createContext(builder);
        builder = jobContextService.newBuilder(UUID.randomUUID());
        builder.addSubContext(1, new DummySubContext());
        jobContextService.createContext(builder);

        assertThat(jobContextService.killAll(), is(2L));
    }

    @Test
    public void testKillSingleJob() {
        ImmutableList<UUID> jobsToKill = ImmutableList.<UUID>of(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        JobExecutionContext.Builder builder = jobContextService.newBuilder(jobsToKill.get(0));
        builder.addSubContext(1, new DummySubContext());
        jobContextService.createContext(builder);

        builder = jobContextService.newBuilder(UUID.randomUUID());
        builder.addSubContext(1, new DummySubContext());
        jobContextService.createContext(builder);

        builder = jobContextService.newBuilder(UUID.randomUUID());
        builder.addSubContext(1, new DummySubContext());
        jobContextService.createContext(builder);
        assertThat(jobContextService.killJobs(jobsToKill), is(1L));
    }

    @Test
    public void testCloseContext() throws Exception {
        JobExecutionContext ctx1 = getJobExecutionContextWithOneActiveSubContext(jobContextService);

        Field activeSubContexts = JobExecutionContext.class.getDeclaredField("activeSubContexts");
        activeSubContexts.setAccessible(true);
        assertThat(((AtomicInteger) activeSubContexts.get(ctx1)).get(), is(1));

        ctx1.close();

        assertThat(((AtomicInteger) activeSubContexts.get(ctx1)).get(), is(0));
    }

    private JobExecutionContext getJobExecutionContextWithOneActiveSubContext(JobContextService jobContextService) {
        JobExecutionContext.Builder builder1 = jobContextService.newBuilder(UUID.randomUUID());
        PageDownstreamContext pageDownstreamContext =
                new PageDownstreamContext("dummy", mock(PageDownstream.class), new Streamer[0], RAM_ACCOUNTING_CONTEXT, 1, mock(FlatProjectorChain.class));
        builder1.addSubContext(1, pageDownstreamContext);
        return jobContextService.createContext(builder1);
    }

    @Test
    public void testKeepAliveExpiration() throws Exception {
        JobContextService.DEFAULT_KEEP_ALIVE_INTERVAL = timeValueMillis(1);
        JobContextService.KEEP_ALIVE = timeValueMillis(0).millis();
        JobContextService jobContextService1 = new JobContextService(settings, testThreadPool, mock(StatsTables.class));

        final ExecutionSubContext executionSubContext = mock(ExecutionSubContext.class);

        JobExecutionContext.Builder builder = jobContextService1.newBuilder(UUID.randomUUID());
        builder.addSubContext(0, executionSubContext);
        jobContextService1.createContext(builder);

        Field activeContexts = JobContextService.class.getDeclaredField("activeContexts");
        activeContexts.setAccessible(true);
        assertThat(((Map) activeContexts.get(jobContextService1)).size(), is(1));

        Thread.sleep(300);

        verify(executionSubContext, times(1)).kill(null);

        // close service, stop reaper thread
        jobContextService1.close();

        // set back original values
        JobContextService.DEFAULT_KEEP_ALIVE_INTERVAL = timeValueMinutes(1);
        JobContextService.KEEP_ALIVE = timeValueMinutes(5).millis();
    }

    protected static class DummySubContext implements ExecutionSubContext {

        private List<ContextCallback> callbacks = new ArrayList<>();

        @Override
        public void addCallback(ContextCallback contextCallback) {
            callbacks.add(contextCallback);
        }

        @Override
        public void prepare() {}

        @Override
        public void start() {}

        @Override
        public void close() {
            for (ContextCallback callback : callbacks) {
                callback.onClose(null, -1L);
            }
        }

        @Override
        public void kill(@Nullable Throwable throwable) {
            for (ContextCallback callback : callbacks) {
                callback.onKill();
            }
        }

        @Override
        public String name() {
            return "dummy";
        }
    }
}
