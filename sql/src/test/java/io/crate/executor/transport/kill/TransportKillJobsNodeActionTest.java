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

package io.crate.executor.transport.kill;

import com.google.common.collect.ImmutableList;
import io.crate.executor.transport.Transports;
import io.crate.jobs.JobContextService;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

public class TransportKillJobsNodeActionTest {

    private ThreadPool threadPool;

    @Before
    public void setUp() throws Exception {
        threadPool = new ThreadPool("dummy");
    }

    @After
    public void tearDown() throws Exception {
        threadPool.shutdown();
        threadPool.awaitTermination(100, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testKillIsCalledOnJobContextService() throws Exception {
        TransportService transportService = mock(TransportService.class);
        JobContextService jobContextService = mock(JobContextService.class);
        NoopClusterService noopClusterService = new NoopClusterService();

        TransportKillJobsNodeAction transportKillJobsNodeAction = new TransportKillJobsNodeAction(
                jobContextService,
                new Transports(noopClusterService, transportService, threadPool),
                transportService
        );

        final CountDownLatch latch = new CountDownLatch(1);
        List<UUID> toKill = ImmutableList.of(UUID.randomUUID(), UUID.randomUUID());
        transportKillJobsNodeAction.execute("noop_id", new KillJobsRequest(toKill), new ActionListener<KillResponse>() {
            @Override
            public void onResponse(KillResponse killAllResponse) {
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable throwable) {
                latch.countDown();
            }
        });

        latch.await();
        verify(jobContextService, times(1)).killJobs(toKill);
    }

}
