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
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class JobContextServiceTest extends CrateUnitTest {

    private static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
            new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.Name.FIELDDATA));

    private JobContextService jobContextService;


    @Before
    public void prepare() throws Exception {
        jobContextService  = new JobContextService(ImmutableSettings.EMPTY, mock(StatsTables.class));
    }

    @After
    public void cleanUp() throws Exception {
        jobContextService.close();
    }

    @Test
    public void testAcquireContext() throws Exception {
        // create new context
        UUID jobId = UUID.randomUUID();
        JobExecutionContext.Builder builder1 = jobContextService.newBuilder(jobId);
        ExecutionSubContext subContext = new DummySubContext();
        builder1.addSubContext(subContext);
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
        builder1.addSubContext(new DummySubContext(1));
        jobContextService.createContext(builder1);

        // creating a context with the same jobId will fail
        JobExecutionContext.Builder builder2 = jobContextService.newBuilder(jobId);
        builder2.addSubContext(new DummySubContext(2));

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
    public void testKillAllCallsKillOnSubContext() throws Exception {
        final AtomicBoolean killCalled = new AtomicBoolean(false);
        ExecutionSubContext dummyContext = new DummySubContext() {

            @Override
            public void innerKill(@Nonnull Throwable throwable) {
                killCalled.set(true);
            }
        };

        JobExecutionContext.Builder builder = jobContextService.newBuilder(UUID.randomUUID());
        builder.addSubContext(dummyContext);
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
            public void innerKill(@Nonnull Throwable throwable) {
                killCalled.set(true);
            }
        };

        UUID jobId = UUID.randomUUID();
        JobExecutionContext.Builder builder = jobContextService.newBuilder(jobId);
        builder.addSubContext(dummyContext);
        jobContextService.createContext(builder);

        builder = jobContextService.newBuilder(UUID.randomUUID());
        builder.addSubContext(new DummySubContext() {
            @Override
            public void innerKill(@Nonnull Throwable throwable) {
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


    private int numContexts(JobExecutionContext ctx) throws Exception {
        Field subContexts = JobExecutionContext.class.getDeclaredField("subContexts");
        subContexts.setAccessible(true);
        return ((Map) subContexts.get(ctx)).size();
    }

    @Test
    public void testJobExecutionContextIsSelfClosing() throws Exception {
        JobExecutionContext.Builder builder1 = jobContextService.newBuilder(UUID.randomUUID());
        DummySubContext subContext = new DummySubContext();

        builder1.addSubContext(subContext);
        JobExecutionContext ctx1 = jobContextService.createContext(builder1);

        assertThat(numContexts(ctx1), is(1));
        subContext.close();
        assertThat(numContexts(ctx1), is(0));
    }

    @Test
    public void testKillReturnsNumberOfJobsKilled() throws Exception {
        JobExecutionContext.Builder builder = jobContextService.newBuilder(UUID.randomUUID());
        builder.addSubContext(new DummySubContext(1));
        builder.addSubContext(new DummySubContext(2));
        builder.addSubContext(new DummySubContext(3));
        builder.addSubContext(new DummySubContext(4));
        jobContextService.createContext(builder);
        builder = jobContextService.newBuilder(UUID.randomUUID());
        builder.addSubContext(new DummySubContext(1));
        jobContextService.createContext(builder);

        assertThat(jobContextService.killAll(), is(2L));
    }

    @Test
    public void testKillSingleJob() {
        ImmutableList<UUID> jobsToKill = ImmutableList.<UUID>of(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        JobExecutionContext.Builder builder = jobContextService.newBuilder(jobsToKill.get(0));
        builder.addSubContext(new DummySubContext());
        jobContextService.createContext(builder);

        builder = jobContextService.newBuilder(UUID.randomUUID());
        builder.addSubContext(new DummySubContext());
        jobContextService.createContext(builder);

        builder = jobContextService.newBuilder(UUID.randomUUID());
        builder.addSubContext(new DummySubContext());
        jobContextService.createContext(builder);
        assertThat(jobContextService.killJobs(jobsToKill), is(1L));
    }

    @Test
    public void testCloseContextRemovesSubContext() throws Exception {
        JobExecutionContext ctx1 = getJobExecutionContextWithOneActiveSubContext(jobContextService);
        assertThat(numContexts(ctx1), is(1));
        ctx1.close();
        assertThat(numContexts(ctx1), is(0));
    }

    private JobExecutionContext getJobExecutionContextWithOneActiveSubContext(JobContextService jobContextService) {
        JobExecutionContext.Builder builder1 = jobContextService.newBuilder(UUID.randomUUID());
        PageDownstreamContext pageDownstreamContext =
                new PageDownstreamContext(1, "dummy", mock(PageDownstream.class), new Streamer[0], RAM_ACCOUNTING_CONTEXT, 1, mock(FlatProjectorChain.class));
        builder1.addSubContext(pageDownstreamContext);
        return jobContextService.createContext(builder1);
    }

    protected static class DummySubContext extends AbstractExecutionSubContext {

        private static final ESLogger LOGGER = Loggers.getLogger(DummySubContext.class);

        public DummySubContext() {
            this(1);
        }

        public DummySubContext(int id) {
            super(id, LOGGER);
        }

        @Override
        public String name() {
            return "dummy " + id();
        }
    }
}
