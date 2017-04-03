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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;
import org.mockito.Answers;

import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

public class TransportKillAllNodeActionTest {

    @Test
    public void testKillIsCalledOnJobContextService() throws Exception {
        TransportService transportService = mock(TransportService.class);
        JobContextService jobContextService = mock(JobContextService.class, Answers.RETURNS_MOCKS.get());
        NoopClusterService noopClusterService = new NoopClusterService();

        TransportKillAllNodeAction transportKillAllNodeAction = new TransportKillAllNodeAction(
            Settings.EMPTY,
            jobContextService,
            noopClusterService,
            transportService
        );

        transportKillAllNodeAction.nodeOperation(new KillAllRequest()).get(5, TimeUnit.SECONDS);
        verify(jobContextService, times(1)).killAll();
    }

}
