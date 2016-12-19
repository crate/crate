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

package io.crate.operation.collect;

import io.crate.core.collections.BlockingEvictingQueue;
import io.crate.core.collections.NoopQueue;
import io.crate.metadata.settings.CrateSettings;
import io.crate.operation.reference.sys.job.JobContext;
import io.crate.operation.reference.sys.job.JobContextLog;
import io.crate.operation.reference.sys.operation.OperationContext;
import io.crate.operation.reference.sys.operation.OperationContextLog;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.elasticsearch.common.settings.Settings;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;

import static org.hamcrest.core.Is.is;

public class StatsTablesTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void testSettingsChanges() {
        StatsTables stats = new StatsTables(Settings.EMPTY, clusterService);

        assertThat(stats.isEnabled(), is(false));
        assertThat(stats.lastJobsLogSize, is(CrateSettings.STATS_JOBS_LOG_SIZE.defaultValue()));
        assertThat(stats.lastOperationsLogSize, is(CrateSettings.STATS_OPERATIONS_LOG_SIZE.defaultValue()));

        // even though logSizes are > 0 it must be a NoopQueue because the stats are disabled
        assertThat(stats.jobsLog.get(), Matchers.instanceOf(NoopQueue.class));

        stats = new StatsTables(Settings.builder()
            .put(CrateSettings.STATS_ENABLED.settingName(), true)
            .put(CrateSettings.STATS_JOBS_LOG_SIZE.settingName(), 100)
            .put(CrateSettings.STATS_OPERATIONS_LOG_SIZE.settingName(), 100).build(), clusterService);

        fail("test update required - change to test setterMethods directly?");
        /*
        stats.listener.onRefreshSettings(Settings.builder()
            .put(CrateSettings.STATS_OPERATIONS_LOG_SIZE.settingName(), 200).build());
            */

        assertThat(stats.isEnabled(), is(true));
        assertThat(stats.lastJobsLogSize, is(100));
        assertThat(stats.lastOperationsLogSize, is(200));

        assertThat(stats.jobsLog.get(), Matchers.instanceOf(BlockingEvictingQueue.class));


        /*
        stats.listener.onRefreshSettings(Settings.builder()
            .put(CrateSettings.STATS_ENABLED.settingName(), false).build());
            */

        // logs got wiped:
        assertThat(stats.jobsLog.get(), Matchers.instanceOf(NoopQueue.class));
        assertThat(stats.isEnabled(), is(false));
    }

    @Test
    public void testLogsArentWipedOnSizeChange() {
        Settings settings = Settings.builder()
            .put(CrateSettings.STATS_ENABLED.settingName(), true).build();
        StatsTables stats = new StatsTables(settings, clusterService);

        stats.jobsLog.get().add(new JobContextLog(new JobContext(UUID.randomUUID(), "select 1", 1L), null));

        fail("update tests");
        /*
        stats.listener.onRefreshSettings(Settings.builder()
            .put(CrateSettings.STATS_ENABLED.settingName(), true)
            .put(CrateSettings.STATS_JOBS_LOG_SIZE.settingName(), 200).build());
            */

        assertThat(stats.jobsLog.get().size(), is(1));


        stats.operationsLog.get().add(new OperationContextLog(
            new OperationContext(1, UUID.randomUUID(), "foo", 2L), null));
        stats.operationsLog.get().add(new OperationContextLog(
            new OperationContext(1, UUID.randomUUID(), "foo", 3L), null));

        /*
        stats.listener.onRefreshSettings(Settings.builder()
            .put(CrateSettings.STATS_ENABLED.settingName(), true)
            .put(CrateSettings.STATS_OPERATIONS_LOG_SIZE.settingName(), 1).build());
            */

        assertThat(stats.operationsLog.get().size(), is(1));
    }

    @Test
    public void testUniqueOperationIdsInOperationsTable() throws Exception {
        Settings settings = Settings.builder()
            .put(CrateSettings.STATS_ENABLED.settingName(), true).build();
        StatsTables stats = new StatsTables(settings, clusterService);

        OperationContext ctxA = new OperationContext(0, UUID.randomUUID(), "dummyOperation", 1L);
        stats.operationStarted(ctxA.id, ctxA.jobId, ctxA.name);

        OperationContext ctxB = new OperationContext(0, UUID.randomUUID(), "dummyOperation", 1L);
        stats.operationStarted(ctxB.id, ctxB.jobId, ctxB.name);

        stats.operationFinished(ctxB.id, ctxB.jobId, null, -1);

        BlockingQueue<OperationContextLog> queue = stats.operationsLog.get();
        assertTrue(queue.contains(new OperationContextLog(ctxB, null)));
        assertFalse(queue.contains(new OperationContextLog(ctxA, null)));

        stats.operationFinished(ctxA.id, ctxA.jobId, null, -1);
        assertTrue(queue.contains(new OperationContextLog(ctxA, null)));

    }
}
