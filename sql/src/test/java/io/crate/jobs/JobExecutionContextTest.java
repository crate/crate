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

import io.crate.operation.collect.StatsTables;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

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

        verify(pageDownstreamContext, times(1)).kill();
    }
}