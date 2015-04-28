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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.crate.Streamer;
import io.crate.operation.PageDownstream;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
        builder1.addPageDownstreamContext(1, mock(PageDownstreamContext.class));
        JobExecutionContext ctx1 = jobContextService.createOrMergeContext(builder1);
        assertThat(ctx1.lastAccessTime(), is(-1L));

        // builder with the same jobId returns the same JobExecutionContext but the subContexts will be merged
        JobExecutionContext.Builder builder2 = jobContextService.newBuilder(jobId);
        builder2.addPageDownstreamContext(2, mock(PageDownstreamContext.class));
        JobExecutionContext ctx2 = jobContextService.createOrMergeContext(builder2);
        assertThat(ctx2, is(ctx1));

        assertThat(ctx1.getPageDownstreamContext(1), notNullValue());
        assertThat(ctx1.getPageDownstreamContext(2), notNullValue());
    }

    @Test
    public void testCreateCallWithEmptyBuilderReturnsNull() throws Exception {
        JobExecutionContext.Builder builder = jobContextService.newBuilder(UUID.randomUUID());
        assertThat(jobContextService.createOrMergeContext(builder), nullValue());
    }

    @Test
    public void testAccessContext() throws Exception {
        JobExecutionContext ctx1 = getJobExecutionContextWithOneActiveSubContext(jobContextService);
        assertThat(ctx1.lastAccessTime(), is(-1L));
        ctx1.getCollectContextOrNull(1);
        assertThat(ctx1.lastAccessTime(), greaterThan(-1L));
    }

    @Test
    public void testJobExecutionContextIsSelfClosing() throws Exception {
        JobExecutionContext.Builder builder1 = jobContextService.newBuilder(UUID.randomUUID());
        PageDownstreamContext pageDownstreamContext =
                new PageDownstreamContext(mock(PageDownstream.class), new Streamer[0], 1);
        builder1.addPageDownstreamContext(1, pageDownstreamContext);
        JobExecutionContext ctx1 = jobContextService.createOrMergeContext(builder1);

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
        builder1.addPageDownstreamContext(1, pageDownstreamContext);
        return jobContextService.createOrMergeContext(builder1);
    }

    @Test
    public void testKeepAliveExpiration() throws Exception {
        JobContextService.DEFAULT_KEEP_ALIVE_INTERVAL = timeValueMillis(1);
        JobContextService.DEFAULT_KEEP_ALIVE = timeValueMillis(0).millis();
        JobContextService jobContextService1 = new JobContextService(settings, testThreadPool);

        JobExecutionContext jobExecutionContext = getJobExecutionContextWithOneActiveSubContext(jobContextService1);
        jobExecutionContext.getCollectContextOrNull(1);
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

    @Test
    public void testCreateOrMergeThreaded() throws Exception {
        final UUID jobId = UUID.randomUUID();
        ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(20));

        for (int i = 0; i < 50; i++) {
            final int currentSubContextId = i;
            final PageDownstreamContext pageDownstreamContext =
                    new PageDownstreamContext(mock(PageDownstream.class), new Streamer[0], 1);
            ListenableFuture<?> future = executorService.submit(new Runnable() {
                @Override
                public void run() {
                    JobExecutionContext.Builder builder = jobContextService.newBuilder(jobId);
                    builder.addPageDownstreamContext(currentSubContextId, pageDownstreamContext);
                    jobContextService.createOrMergeContext(builder);
                }
            });
            if (currentSubContextId < 49) {
                future.addListener(new Runnable() {
                    @Override
                    public void run() {
                        pageDownstreamContext.finish();
                    }
                }, MoreExecutors.directExecutor());
            }
        }
        executorService.shutdown();
        executorService.awaitTermination(500, TimeUnit.MILLISECONDS);

        JobExecutionContext context = jobContextService.getContext(jobId);
        for (int i = 1; i < 49; i++) {
            assertThat(context.getPageDownstreamContext(i), nullValue());
        }
        assertThat(context.getPageDownstreamContext(49), notNullValue());
    }
}
