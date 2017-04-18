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

import io.crate.jobs.JobContextService;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.Test;
import org.mockito.Answers;

import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

public class TransportKillAllNodeActionTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testKillIsCalledOnJobContextService() throws Exception {
        JobContextService jobContextService = mock(JobContextService.class, Answers.RETURNS_MOCKS.get());

        TransportKillAllNodeAction transportKillAllNodeAction = new TransportKillAllNodeAction(
            Settings.EMPTY,
            jobContextService,
            clusterService,
            MockTransportService.local(Settings.EMPTY, Version.CURRENT, THREAD_POOL, null)
        );

        transportKillAllNodeAction.nodeOperation(new KillAllRequest()).get(5, TimeUnit.SECONDS);
        verify(jobContextService, times(1)).killAll();
    }

}
