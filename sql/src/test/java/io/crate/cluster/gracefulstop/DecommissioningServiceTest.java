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

package io.crate.cluster.gracefulstop;

import io.crate.action.sql.SQLOperations;
import io.crate.operation.collect.stats.JobsLogs;
import io.crate.plugin.SQLPlugin;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;

import javax.annotation.Nullable;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.*;

public class DecommissioningServiceTest extends CrateDummyClusterServiceUnitTest {

    private JobsLogs jobsLogs;
    private TestableDecommissioningService decommissioningService;
    private ThreadPool threadPool;
    private SQLOperations sqlOperations;

    @Override
    protected Set<Setting<?>> additionalClusterSettings() {
        SQLPlugin sqlPlugin = new SQLPlugin(Settings.EMPTY);
        return Sets.newHashSet(sqlPlugin.getSettings());
    }

    @Before
    public void init() throws Exception {
        threadPool = mock(ThreadPool.class, Answers.RETURNS_MOCKS.get());
        jobsLogs = new JobsLogs(() -> true);
        sqlOperations = mock(SQLOperations.class, Answers.RETURNS_MOCKS.get());
        decommissioningService = new TestableDecommissioningService(
            Settings.EMPTY,
            clusterService,
            jobsLogs,
            threadPool,
            sqlOperations,
            mock(TransportClusterHealthAction.class),
            mock(TransportClusterUpdateSettingsAction.class)
        );
    }

    @Test
    public void testExitIfNoActiveRequests() throws Exception {
        decommissioningService.exitIfNoActiveRequests(0);
        assertThat(decommissioningService.exited, is(true));
        assertThat(decommissioningService.forceStopOrAbortCalled, is(false));
    }

    @Test
    public void testNoExitIfRequestAreActive() throws Exception {
        jobsLogs.logExecutionEnd(UUID.randomUUID(), null);
        decommissioningService.exitIfNoActiveRequests(System.nanoTime());
        assertThat(decommissioningService.exited, is(false));
        assertThat(decommissioningService.forceStopOrAbortCalled, is(false));
        verify(threadPool, times(1)).scheduler();
    }

    @Test
    public void testAbortOrForceStopIsCalledOnTimeout() throws Exception {
        jobsLogs.logExecutionEnd(UUID.randomUUID(), null);
        decommissioningService.exitIfNoActiveRequests(System.nanoTime() - TimeValue.timeValueHours(3).nanos());
        assertThat(decommissioningService.forceStopOrAbortCalled, is(true));
        verify(sqlOperations, times(1)).enable();
    }

    private static class TestableDecommissioningService extends DecommissioningService {

        private boolean exited = false;
        private boolean forceStopOrAbortCalled = false;

        TestableDecommissioningService(Settings settings,
                                       ClusterService clusterService,
                                       JobsLogs jobsLogs,
                                       ThreadPool threadPool,
                                       SQLOperations sqlOperations,
                                       TransportClusterHealthAction healthAction,
                                       TransportClusterUpdateSettingsAction updateSettingsAction) {
            super(settings, clusterService, jobsLogs, threadPool,
                sqlOperations, healthAction, updateSettingsAction);

        }

        @Override
        void exit() {
            exited = true;
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
