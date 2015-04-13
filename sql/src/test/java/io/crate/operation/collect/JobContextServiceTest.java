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

package io.crate.operation.collect;

import io.crate.jobs.JobContextService;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;
import static org.hamcrest.Matchers.*;

public class JobContextServiceTest extends CrateUnitTest {

    private final ThreadPool testThreadPool = new ThreadPool(getClass().getSimpleName());
    private final Settings settings = ImmutableSettings.EMPTY;

    private final JobContextService jobContextService = new JobContextService(settings, testThreadPool);

    @After
    public void cleanUp() throws Exception {
        jobContextService.close();
        testThreadPool.shutdown();
    }

    @Test
    public void testAcquireContext() throws Exception {
        // create new context
        JobCollectContext ctx1 = jobContextService.acquireContext(UUID.randomUUID());
        assertThat(ctx1, instanceOf(JobCollectContext.class));
        assertThat(ctx1.lastAccessTime(), is(-1L));

        // using same UUID must return existing context
        JobCollectContext ctx2 = jobContextService.acquireContext(ctx1.id());
        assertThat(ctx2, is(ctx1));
    }

    @Test
    public void testReleaseContext() throws Exception {
        JobCollectContext ctx1 = jobContextService.acquireContext(UUID.randomUUID());
        jobContextService.releaseContext(ctx1.id());
        assertThat(ctx1.lastAccessTime(), greaterThan(-1L));
    }

    @Test
    public void testCloseContext() throws Exception {
        JobCollectContext ctx1 = jobContextService.acquireContext(UUID.randomUUID());
        jobContextService.closeContext(ctx1.id());

        // context must be closed
        Field closed = JobCollectContext.class.getDeclaredField("closed");
        closed.setAccessible(true);
        assertThat(((AtomicBoolean)closed.get(ctx1)).get(), is(true));

        Field activeContexts = JobContextService.class.getDeclaredField("activeContexts");
        activeContexts.setAccessible(true);
        assertThat(((Map) activeContexts.get(jobContextService)).size(), is(0));
    }

    @Test
    public void testKeepAliveExpiration() throws Exception {
        JobContextService.DEFAULT_KEEP_ALIVE_INTERVAL = timeValueMillis(1);
        JobContextService.DEFAULT_KEEP_ALIVE = timeValueMillis(0).millis();
        JobContextService jobContextService1 = new JobContextService(settings, testThreadPool);
        JobCollectContext ctx1 = jobContextService1.acquireContext(UUID.randomUUID());
        jobContextService1.releaseContext(ctx1.id());
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
