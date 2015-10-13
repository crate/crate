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

package io.crate.jobs;

import com.google.common.collect.ImmutableList;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.executor.transport.kill.KillJobsRequest;
import io.crate.executor.transport.kill.KillResponse;
import io.crate.executor.transport.kill.TransportKillJobsNodeAction;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;

public class KillJobsReaperTest extends CrateUnitTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private ThreadPool threadPool;
    private TransportActionProvider transportActionProvider;
    private Set<UUID> killed;

    @Before
    public void prepare() throws Exception {
        this.killed = new HashSet<>();
        threadPool = mock(ThreadPool.class);
        when(threadPool.estimatedTimeInMillis()).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                return System.currentTimeMillis();
            }
        });
        transportActionProvider = mock(TransportActionProvider.class);
        TransportKillJobsNodeAction transportKillJobsNodeAction = mock(TransportKillJobsNodeAction.class);
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                KillJobsRequest request = (KillJobsRequest)invocation.getArguments()[0];
                killed.addAll(request.toKill());
                ActionListener<KillResponse> listener = (ActionListener<KillResponse>)invocation.getArguments()[1];
                listener.onResponse(new KillResponse(request.toKill().size()));
                return null;
            }
        }).when(transportKillJobsNodeAction).executeKillOnAllNodes(Mockito.any(KillJobsRequest.class), Mockito.<ActionListener<KillResponse>>any());
        when(transportActionProvider.transportKillJobsNodeAction()).thenReturn(transportKillJobsNodeAction);
    }

    private JobExecutionContext mockedContext(UUID jobId, TimeValue notAccessed) {
        long now = System.currentTimeMillis();
        JobExecutionContext jobExecutionContext = mock(JobExecutionContext.class);
        when(jobExecutionContext.jobId()).thenReturn(jobId);
        when(jobExecutionContext.lastAccessTime()).thenReturn(now - notAccessed.getMillis());
        return jobExecutionContext;
    }

    @Test
    public void testHangingJobsAreKilled() throws Exception {
        UUID notKilledId = UUID.randomUUID();
        UUID killed1 = UUID.randomUUID();
        UUID killed2 = UUID.randomUUID();
        KillJobsReaper reaper = new KillJobsReaper(threadPool, transportActionProvider);
        List<JobExecutionContext> contexts = ImmutableList.of(
                mockedContext(notKilledId, TimeValue.timeValueMillis(100)),
                mockedContext(killed1, TimeValue.timeValueMillis(1000)),
                mockedContext(killed2, TimeValue.timeValueMillis(2000))
        );
        reaper.killHangingJobs(TimeValue.timeValueMillis(999), contexts);
        assertThat(this.killed, containsInAnyOrder(killed1, killed2));
        assertThat(this.killed, not(hasItem(notKilledId)));
    }
}
