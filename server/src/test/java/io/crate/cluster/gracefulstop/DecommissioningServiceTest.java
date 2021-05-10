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

package io.crate.cluster.gracefulstop;

import io.crate.action.sql.SQLOperations;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.plugin.SQLPlugin;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import io.crate.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.Mockito;

import javax.annotation.Nullable;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DecommissioningServiceTest extends CrateDummyClusterServiceUnitTest {

    private JobsLogs jobsLogs;
    private TestableDecommissioningService decommissioningService;
    private ScheduledExecutorService executorService;
    private SQLOperations sqlOperations;
    private AtomicBoolean exited = new AtomicBoolean(false);

    @Override
    protected Set<Setting<?>> additionalClusterSettings() {
        SQLPlugin sqlPlugin = new SQLPlugin(Settings.EMPTY);
        return Sets.newHashSet(sqlPlugin.getSettings());
    }

    @Before
    public void init() throws Exception {
        executorService = mock(ScheduledExecutorService.class, Answers.RETURNS_MOCKS);
        jobsLogs = new JobsLogs(() -> true);
        sqlOperations = mock(SQLOperations.class, Answers.RETURNS_MOCKS);
        decommissioningService = new TestableDecommissioningService(
            Settings.EMPTY,
            clusterService,
            jobsLogs,
            executorService,
            sqlOperations,
            () -> exited.set(true),
            mock(TransportClusterHealthAction.class),
            mock(TransportClusterUpdateSettingsAction.class)
        );
    }

    @Test
    public void testExitIfNoActiveRequests() throws Exception {
        decommissioningService.exitIfNoActiveRequests(0);
        assertThat(exited.get(), is(true));
        assertThat(decommissioningService.forceStopOrAbortCalled, is(false));
    }

    @Test
    public void testNoExitIfRequestAreActive() throws Exception {
        jobsLogs.logExecutionEnd(UUID.randomUUID(), null);
        decommissioningService.exitIfNoActiveRequests(System.nanoTime());
        assertThat(exited.get(), is(false));
        assertThat(decommissioningService.forceStopOrAbortCalled, is(false));
        verify(executorService, times(1)).schedule(
            Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any(TimeUnit.class));
    }

    @Test
    public void testAbortOrForceStopIsCalledOnTimeout() throws Exception {
        jobsLogs.logExecutionEnd(UUID.randomUUID(), null);
        decommissioningService.exitIfNoActiveRequests(System.nanoTime() - TimeValue.timeValueHours(3).nanos());
        assertThat(decommissioningService.forceStopOrAbortCalled, is(true));
        verify(sqlOperations, times(1)).enable();
    }

    private static class TestableDecommissioningService extends DecommissioningService {

        private boolean forceStopOrAbortCalled = false;

        TestableDecommissioningService(Settings settings,
                                       ClusterService clusterService,
                                       JobsLogs jobsLogs,
                                       ScheduledExecutorService executorService,
                                       SQLOperations sqlOperations,
                                       Runnable safeExitAction,
                                       TransportClusterHealthAction healthAction,
                                       TransportClusterUpdateSettingsAction updateSettingsAction) {
            super(
                settings,
                clusterService,
                jobsLogs,
                executorService,
                sqlOperations,
                () -> 0,
                safeExitAction,
                healthAction,
                updateSettingsAction);
        }

        @Override
        void forceStopOrAbort(@Nullable Throwable e) {
            forceStopOrAbortCalled = true;
            super.forceStopOrAbort(e);
        }

        @Override
        protected void removeDecommissioningSetting() {
        }
    }
}
