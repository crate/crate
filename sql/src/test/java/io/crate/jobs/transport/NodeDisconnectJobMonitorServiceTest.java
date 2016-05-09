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

package io.crate.jobs.transport;

import io.crate.exceptions.ContextMissingException;
import io.crate.jobs.DummySubContext;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.operation.collect.StatsTables;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.UUID;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NodeDisconnectJobMonitorServiceTest extends CrateUnitTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testOnNodeDisconnectedKillsJobOriginatingFromThatNode() throws Exception {
        JobContextService jobContextService = new JobContextService(
            Settings.EMPTY, new NoopClusterService(), mock(StatsTables.class));

        JobExecutionContext.Builder builder = jobContextService.newBuilder(UUID.randomUUID());
        builder.addSubContext(new DummySubContext());
        JobExecutionContext context = jobContextService.createContext(builder);

        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.schedule(any(TimeValue.class), anyString(), any(Runnable.class))).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                ((Runnable) invocation.getArguments()[2]).run();
                return null;
            }
        });

        NodeDisconnectJobMonitorService monitorService = new NodeDisconnectJobMonitorService(
            Settings.EMPTY,
            threadPool,
            jobContextService,
            mock(TransportService.class));

        monitorService.onNodeDisconnected(new DiscoveryNode("noop_id", DummyTransportAddress.INSTANCE, Version.CURRENT));

        expectedException.expect(ContextMissingException.class);
        jobContextService.getContext(context.jobId());
    }
}
