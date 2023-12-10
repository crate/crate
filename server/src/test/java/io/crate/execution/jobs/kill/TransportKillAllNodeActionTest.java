/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.jobs.kill;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.TimeUnit;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import io.crate.execution.jobs.TasksService;
import io.crate.netty.NettyBootstrap;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class TransportKillAllNodeActionTest extends CrateDummyClusterServiceUnitTest {

    @Rule
    public MockitoRule initRule = MockitoJUnit.rule();

    private NettyBootstrap nettyBootstrap;

    @Before
    public void setupNetty() {
        nettyBootstrap = new NettyBootstrap(Settings.EMPTY);
        nettyBootstrap.start();
    }

    @After
    public void teardownNetty() {
        nettyBootstrap.close();
    }


    @Test
    public void testKillIsCalledOnTasks() throws Exception {
        TasksService tasksService = mock(TasksService.class, Answers.RETURNS_MOCKS);

        TransportKillAllNodeAction transportKillAllNodeAction = new TransportKillAllNodeAction(
            tasksService,
            clusterService,
            MockTransportService.createNewService(
                Settings.EMPTY, Version.CURRENT, THREAD_POOL, nettyBootstrap, clusterService.getClusterSettings())
        );

        transportKillAllNodeAction.nodeOperation(new KillAllRequest("dummy-user")).get(5, TimeUnit.SECONDS);
        verify(tasksService, times(1)).killAll("dummy-user");
    }

}
