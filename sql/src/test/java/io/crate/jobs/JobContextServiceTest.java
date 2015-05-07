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
import io.crate.operation.PageDownstream;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;

public class JobContextServiceTest extends CrateUnitTest {

    private final ThreadPool testThreadPool = new ThreadPool(getClass().getSimpleName());
    private final Settings settings = ImmutableSettings.EMPTY;
    private final JobContextService jobContextService = new JobContextService(
            settings, testThreadPool);

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
        builder1.addSubContext(1, mock(PageDownstreamContext.class));
        JobExecutionContext ctx1 = jobContextService.createContext(builder1);
        assertThat(ctx1.lastAccessTime(), is(-1L));
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
        JobExecutionContext ctx1 = jobContextService.createContext(builder1);
        assertThat(ctx1.lastAccessTime(), is(-1L));

        // creating a context with the same jobId will fail
        JobExecutionContext.Builder builder2 = jobContextService.newBuilder(jobId);
        builder2.addSubContext(2, mock(PageDownstreamContext.class));

        jobContextService.createContext(builder2);
    }

    @Test
    public void testCreateCallWithEmptyBuilderReturnsNull() throws Exception {
        JobExecutionContext.Builder builder = jobContextService.newBuilder(UUID.randomUUID());
        assertThat(jobContextService.createContext(builder), nullValue());
    }

    @Test
    public void testAccessContext() throws Exception {
        JobExecutionContext ctx1 = getJobExecutionContextWithOneActiveSubContext(jobContextService);
        assertThat(ctx1.lastAccessTime(), is(-1L));
        ctx1.getSubContextOrNull(1);
        assertThat(ctx1.lastAccessTime(), greaterThan(-1L));
    }

    @Test
    public void testKillAllCallsKillOnSubContext() throws Exception {
        final AtomicBoolean killCalled = new AtomicBoolean(false);
        ExecutionSubContext dummyContext = new ExecutionSubContext() {
            List<ContextCallback> callbacks = new ArrayList<>();

            @Override
            public void addCallback(ContextCallback contextCallback) {
                callbacks.add(contextCallback);
            }

            @Override
            public void close() {
                for (ContextCallback callback : callbacks) {
                    callback.onClose();
                }
            }

            @Override
            public void kill() {
                killCalled.set(true);
                for (ContextCallback callback : callbacks) {
                    callback.onClose();
                }
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
        jobContextService.killAll();

        assertThat(killCalled.get(), is(true));
        assertThat(activeContexts.size(), is(0));
    }

    @Test
    public void testJobExecutionContextIsSelfClosing() throws Exception {
        JobExecutionContext.Builder builder1 = jobContextService.newBuilder(UUID.randomUUID());
        PageDownstreamContext pageDownstreamContext =
                new PageDownstreamContext(mock(PageDownstream.class), new Streamer[0], 1);
        builder1.addSubContext(1, pageDownstreamContext);
        JobExecutionContext ctx1 = jobContextService.createContext(builder1);

        Field activeSubContexts = JobExecutionContext.class.getDeclaredField("activeSubContexts");
        activeSubContexts.setAccessible(true);
        assertThat(((AtomicInteger) activeSubContexts.get(ctx1)).get(), is(1));

        pageDownstreamContext.finish();

        assertThat(((AtomicInteger) activeSubContexts.get(ctx1)).get(), is(0));
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
                new PageDownstreamContext(mock(PageDownstream.class), new Streamer[0], 1);
        builder1.addSubContext(1, pageDownstreamContext);
        return jobContextService.createContext(builder1);
    }

    @Test
    public void testKeepAliveExpiration() throws Exception {
        JobContextService.DEFAULT_KEEP_ALIVE_INTERVAL = timeValueMillis(1);
        JobContextService.DEFAULT_KEEP_ALIVE = timeValueMillis(0).millis();
        JobContextService jobContextService1 = new JobContextService(settings, testThreadPool);

        JobExecutionContext jobExecutionContext = getJobExecutionContextWithOneActiveSubContext(jobContextService1);
        jobExecutionContext.getSubContextOrNull(1);
        Field activeContexts = JobContextService.class.getDeclaredField("activeContexts");
        activeContexts.setAccessible(true);

        Thread.sleep(300);

        assertThat(((Map) activeContexts.get(jobContextService1)).size(), is(0));

        // close service, stop reaper thread
        jobContextService1.close();

        // set back original values
        JobContextService.DEFAULT_KEEP_ALIVE_INTERVAL = timeValueMinutes(1);
        JobContextService.DEFAULT_KEEP_ALIVE = timeValueMinutes(5).millis();
    }
}
