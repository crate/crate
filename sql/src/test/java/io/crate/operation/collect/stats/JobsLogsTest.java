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

package io.crate.operation.collect.stats;

import com.google.common.collect.ImmutableList;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.BlockingEvictingQueue;
import io.crate.metadata.settings.CrateSettings;
import io.crate.operation.reference.sys.job.JobContext;
import io.crate.operation.reference.sys.job.JobContextLog;
import io.crate.operation.reference.sys.operation.OperationContext;
import io.crate.operation.reference.sys.operation.OperationContextLog;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.hamcrest.core.Is.is;

public class JobsLogsTest extends CrateUnitTest {

    private ScheduledExecutorService scheduler;
    private static CrateCircuitBreakerService breakerService;
    private RamAccountingContext ramAccountingContext;
    private static NodeSettingsService nodeSettingsService = new NodeSettingsService(Settings.EMPTY);

    @Before
    public void createScheduler() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        ramAccountingContext = new RamAccountingContext("testRamAccountingContext",
            breakerService.getBreaker(CrateCircuitBreakerService.JOBS_LOG));
    }

    @After
    public void terminateScheduler() throws InterruptedException {
        terminate(scheduler);
    }

    @BeforeClass
    public static void beforeClass() {
        CircuitBreakerService esBreakerService = new HierarchyCircuitBreakerService(Settings.EMPTY, nodeSettingsService);
        breakerService = new CrateCircuitBreakerService(Settings.EMPTY, nodeSettingsService, esBreakerService);
    }

    @Test
    public void testDefaultSettings() {
        JobsLogService stats = new JobsLogService(Settings.EMPTY, nodeSettingsService, scheduler, breakerService);

        assertThat(stats.isEnabled(), is(false));
        assertThat(stats.lastJobsLogSize, is(CrateSettings.STATS_JOBS_LOG_SIZE.defaultValue()));
        assertThat(stats.lastOperationsLogSize, is(CrateSettings.STATS_OPERATIONS_LOG_SIZE.defaultValue()));
        assertThat(stats.lastJobsLogExpiration, is(CrateSettings.STATS_JOBS_LOG_EXPIRATION.defaultValue()));
        assertThat(stats.lastOperationsLogExpiration, is(CrateSettings.STATS_OPERATIONS_LOG_EXPIRATION.defaultValue()));

        // even though jobsLog size and opertionsLog size are > 0 it must be a NoopQueue because the stats are disabled
        assertThat(stats.jobsLogSink, Matchers.instanceOf(NoopLogSink.class));
        assertThat(stats.operationsLogSink, Matchers.instanceOf(NoopLogSink.class));
    }

    @Test
    public void testEnableStats() throws Exception {
        Settings settings = Settings.builder()
            .put(CrateSettings.STATS_ENABLED.settingName(), false)
            .put(CrateSettings.STATS_JOBS_LOG_SIZE.settingName(), 100)
            .put(CrateSettings.STATS_OPERATIONS_LOG_SIZE.settingName(), 100)
            .build();

        JobsLogService stats = new JobsLogService(settings, nodeSettingsService, scheduler, breakerService);
        stats.listener.onRefreshSettings(Settings.builder()
            .put(CrateSettings.STATS_ENABLED.settingName(), true)
            .build());

        assertThat(stats.isEnabled(), is(true));
        assertThat(stats.lastJobsLogSize, is(100));
        assertThat(stats.jobsLogSink, Matchers.instanceOf(QueueSink.class));
        assertThat(stats.lastOperationsLogSize, is(100));
        assertThat(stats.operationsLogSink, Matchers.instanceOf(QueueSink.class));
    }

    @Test
    public void testSettingsChanges() throws Exception {
        Settings settings = Settings.builder()
            .put(CrateSettings.STATS_ENABLED.settingName(), true)
            .put(CrateSettings.STATS_JOBS_LOG_SIZE.settingName(), 100)
            .put(CrateSettings.STATS_OPERATIONS_LOG_SIZE.settingName(), 100)
            .build();

        JobsLogService stats = new JobsLogService(settings, nodeSettingsService, scheduler, breakerService);

        // sinks are still of type QueueSink
        assertThat(stats.jobsLogSink, Matchers.instanceOf(QueueSink.class));
        assertThat(stats.operationsLogSink, Matchers.instanceOf(QueueSink.class));

        assertThat(inspectRamAccountingQueue((QueueSink) stats.jobsLogSink),
            Matchers.instanceOf(BlockingEvictingQueue.class));
        assertThat(inspectRamAccountingQueue((QueueSink) stats.operationsLogSink),
            Matchers.instanceOf(BlockingEvictingQueue.class));

        stats.listener.onRefreshSettings(Settings.builder()
            .put(CrateSettings.STATS_JOBS_LOG_EXPIRATION.settingName(), "10s")
            .put(CrateSettings.STATS_OPERATIONS_LOG_EXPIRATION.settingName(), "10s")
            .build());

        assertThat(inspectRamAccountingQueue((QueueSink) stats.jobsLogSink),
            Matchers.instanceOf(ConcurrentLinkedDeque.class));
        assertThat(inspectRamAccountingQueue((QueueSink) stats.operationsLogSink),
            Matchers.instanceOf(ConcurrentLinkedDeque.class));

        // set all to 0 but don't disable stats
        stats.listener.onRefreshSettings(Settings.builder()
            .put(CrateSettings.STATS_JOBS_LOG_SIZE.settingName(), 0)
            .put(CrateSettings.STATS_JOBS_LOG_EXPIRATION.settingName(), "0s")
            .put(CrateSettings.STATS_OPERATIONS_LOG_SIZE.settingName(), 0)
            .put(CrateSettings.STATS_OPERATIONS_LOG_EXPIRATION.settingName(), "0s")
            .build());
        assertThat(stats.jobsLogSink, Matchers.instanceOf(NoopLogSink.class));
        assertThat(stats.operationsLogSink, Matchers.instanceOf(NoopLogSink.class));
        assertThat(stats.isEnabled(), is(true));

        stats.listener.onRefreshSettings(Settings.builder()
            .put(CrateSettings.STATS_JOBS_LOG_SIZE.settingName(), 200)
            .put(CrateSettings.STATS_OPERATIONS_LOG_SIZE.settingName(), 200)
            .build());
        assertThat(stats.jobsLogSink, Matchers.instanceOf(QueueSink.class));
        assertThat(inspectRamAccountingQueue((QueueSink) stats.jobsLogSink),
            Matchers.instanceOf(BlockingEvictingQueue.class));
        assertThat(stats.operationsLogSink, Matchers.instanceOf(QueueSink.class));
        assertThat(inspectRamAccountingQueue((QueueSink) stats.operationsLogSink),
            Matchers.instanceOf(BlockingEvictingQueue.class));

        // disable stats
        stats.listener.onRefreshSettings(Settings.builder()
            .put(CrateSettings.STATS_ENABLED.settingName(), false)
            .build());
        assertThat(stats.jobsLogSink, Matchers.instanceOf(NoopLogSink.class));
        assertThat(stats.operationsLogSink, Matchers.instanceOf(NoopLogSink.class));
        assertThat(stats.isEnabled(), is(false));
    }

    private static Queue inspectRamAccountingQueue(QueueSink sink) throws Exception {
        Field field = sink.getClass().getDeclaredField("queue");
        field.setAccessible(true);
        RamAccountingQueue q = (RamAccountingQueue) field.get(sink);
        field = field.get(sink).getClass().getDeclaredField("delegate");
        field.setAccessible(true);
        return (Queue) field.get(q);
    }

    @Test
    public void testLogsArentWipedOnSizeChange() {
        Settings settings = Settings.builder()
            .put(CrateSettings.STATS_ENABLED.settingName(), true).build();
        JobsLogService stats = new JobsLogService(settings, nodeSettingsService, scheduler, breakerService);

        stats.jobsLogSink.add(new JobContextLog(new JobContext(UUID.randomUUID(), "select 1", 1L), null));

        stats.listener.onRefreshSettings(Settings.builder()
            .put(CrateSettings.STATS_ENABLED.settingName(), true)
            .put(CrateSettings.STATS_JOBS_LOG_SIZE.settingName(), 200).build());

        assertThat(ImmutableList.copyOf(stats.jobsLogSink.iterator()).size(), is(1));

        stats.operationsLogSink.add(new OperationContextLog(
            new OperationContext(1, UUID.randomUUID(), "foo", 2L), null));
        stats.operationsLogSink.add(new OperationContextLog(
            new OperationContext(1, UUID.randomUUID(), "foo", 3L), null));

        stats.listener.onRefreshSettings(Settings.builder()
            .put(CrateSettings.STATS_ENABLED.settingName(), true)
            .put(CrateSettings.STATS_OPERATIONS_LOG_SIZE.settingName(), 1).build());

        assertThat(ImmutableList.copyOf(stats.jobsLogSink.iterator()).size(), is(1));
    }

    @Test
    public void testUniqueOperationIdsInOperationsTable() {
        JobsLogs jobsLogs = new JobsLogs(() -> true);
        Queue<OperationContextLog> q = new BlockingEvictingQueue(10);
        jobsLogs.updateOperationsLog(new QueueSink<>(q, ramAccountingContext::close));

        OperationContext ctxA = new OperationContext(0, UUID.randomUUID(), "dummyOperation", 1L);
        jobsLogs.operationStarted(ctxA.id, ctxA.jobId, ctxA.name);

        OperationContext ctxB = new OperationContext(0, UUID.randomUUID(), "dummyOperation", 1L);
        jobsLogs.operationStarted(ctxB.id, ctxB.jobId, ctxB.name);

        jobsLogs.operationFinished(ctxB.id, ctxB.jobId, null, -1);
        List<OperationContextLog> entries = ImmutableList.copyOf(jobsLogs.operationsLog.get().iterator());

        assertTrue(entries.contains(new OperationContextLog(ctxB, null)));
        assertFalse(entries.contains(new OperationContextLog(ctxA, null)));

        jobsLogs.operationFinished(ctxA.id, ctxA.jobId, null, -1);
        entries = ImmutableList.copyOf(jobsLogs.operationsLog.get());
        assertTrue(entries.contains(new OperationContextLog(ctxA, null)));
    }

    @Test
    public void testLowerBoundScheduler() throws NoSuchMethodException {
        JobsLogService stats = new JobsLogService(Settings.EMPTY, nodeSettingsService, scheduler, breakerService);
        assertThat(stats.clearInterval(TimeValue.timeValueMillis(1L)), is(1000L));
        assertThat(stats.clearInterval(TimeValue.timeValueSeconds(8L)), is(1000L));
        assertThat(stats.clearInterval(TimeValue.timeValueSeconds(10L)), is(1000L));
        assertThat(stats.clearInterval(TimeValue.timeValueSeconds(20L)), is(2000L));
        assertThat(stats.clearInterval(TimeValue.timeValueHours(720L)), is(86_400_000L));  // 30 days
    }
}
