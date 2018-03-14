/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.engine.distribution;

import io.crate.Streamer;
import io.crate.data.Bucket;
import io.crate.exceptions.ContextMissingException;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.execution.jobs.JobContextService;
import io.crate.execution.jobs.kill.TransportKillJobsNodeAction;
import io.crate.execution.support.Transports;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TransportDistributedResultActionTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testKillIsInvokedIfContextIsNotFound() throws InterruptedException, TimeoutException {
        JobContextService jobContextService = new JobContextService(
            Settings.EMPTY, clusterService, new JobsLogs(() -> false));
        TransportKillJobsNodeAction killJobsAction = mock(TransportKillJobsNodeAction.class);
        TransportDistributedResultAction transportDistributedResultAction = new TransportDistributedResultAction(
            mock(Transports.class),
            jobContextService,
            THREAD_POOL,
            mock(TransportService.class),
            clusterService,
            killJobsAction,
            Settings.EMPTY,
            BackoffPolicy.exponentialBackoff(TimeValue.ZERO, 0)
        );

        try {
            transportDistributedResultAction.nodeOperation(
                new DistributedResultRequest(UUID.randomUUID(), 0, (byte) 0, 0, new Streamer[0], Bucket.EMPTY, true)
            ).get(5, TimeUnit.SECONDS);
            fail("nodeOperation call should fail with ContextMissingException");
        } catch (ExecutionException e) {
            assertThat(e.getCause(), Matchers.instanceOf(ContextMissingException.class));
        }

        verify(killJobsAction, times(1)).broadcast(any(), any(), any());
    }
}
