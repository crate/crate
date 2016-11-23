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

package io.crate.operation.collect.collectors;

import com.google.common.collect.ImmutableMap;
import io.crate.action.job.JobRequest;
import io.crate.action.job.JobResponse;
import io.crate.action.job.TransportJobAction;
import io.crate.analyze.WhereClause;
import io.crate.analyze.symbol.Symbol;
import io.crate.breaker.RamAccountingContext;
import io.crate.executor.transport.kill.KillJobsRequest;
import io.crate.executor.transport.kill.TransportKillJobsNodeAction;
import io.crate.jobs.JobContextService;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.operation.collect.StatsTables;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.Projection;
import io.crate.testing.CollectingRowReceiver;
import io.crate.types.DataTypes;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.crate.testing.TestingHelpers.createReference;
import static org.hamcrest.Matchers.isA;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class RemoteCollectorTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private TransportJobAction transportJobAction;
    private TransportKillJobsNodeAction transportKillJobsNodeAction;
    private CollectingRowReceiver rowReceiver;
    private RemoteCollector remoteCollector;

    @Captor
    public ArgumentCaptor<ActionListener<JobResponse>> listenerCaptor;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        UUID jobId = UUID.randomUUID();
        RoutedCollectPhase collectPhase = new RoutedCollectPhase(
            jobId,
            0,
            "remoteCollect",
            new Routing(ImmutableMap.<String, Map<String, List<Integer>>>of("remoteNode", ImmutableMap.of("dummyTable", Collections.singletonList(1)))),
            RowGranularity.DOC,
            Collections.<Symbol>singletonList(createReference("name", DataTypes.STRING)),
            Collections.<Projection>emptyList(),
            WhereClause.MATCH_ALL,
            DistributionInfo.DEFAULT_BROADCAST,
            (byte) 0);
        transportJobAction = mock(TransportJobAction.class);
        transportKillJobsNodeAction = mock(TransportKillJobsNodeAction.class);
        rowReceiver = new CollectingRowReceiver();

        JobContextService jobContextService = new JobContextService(Settings.EMPTY, new NoopClusterService(), mock(StatsTables.class));
        remoteCollector = new RemoteCollector(
            jobId,
            "localNode",
            "remoteNode",
            transportJobAction,
            transportKillJobsNodeAction,
            jobContextService,
            mock(RamAccountingContext.class),
            rowReceiver,
            collectPhase
        );
    }

    @Test
    public void testKillBeforeContextCreation() throws Exception {
        remoteCollector.kill(new InterruptedException("KILLED"));
        remoteCollector.doCollect();

        verify(transportJobAction, times(0)).execute(eq("remoteNode"), any(JobRequest.class), any(ActionListener.class));

        expectedException.expectCause(isA(InterruptedException.class));
        rowReceiver.result();
    }


    @Test
    public void testRemoteContextIsNotCreatedIfKillHappensBeforeCreateRemoteContext() throws Exception {
        remoteCollector.createLocalContext();
        remoteCollector.kill(new InterruptedException());
        remoteCollector.createRemoteContext();

        verify(transportJobAction, times(0)).execute(eq("remoteNode"), any(JobRequest.class), any(ActionListener.class));
        expectedException.expectCause(isA(InterruptedException.class));
        rowReceiver.result();
    }

    @Test
    public void testKillRequestsAreMadeIfCollectorIsKilledAfterRemoteContextCreation() throws Exception {
        remoteCollector.doCollect();
        verify(transportJobAction, times(1)).execute(eq("remoteNode"), any(JobRequest.class), listenerCaptor.capture());

        remoteCollector.kill(new InterruptedException());

        ActionListener<JobResponse> listener = listenerCaptor.getValue();
        listener.onResponse(new JobResponse());

        verify(transportKillJobsNodeAction, times(1)).broadcast(any(KillJobsRequest.class), any(ActionListener.class));
    }
}
