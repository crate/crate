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
import com.google.common.collect.ImmutableSet;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.BlockingEvictingQueue;
import io.crate.operation.reference.sys.job.JobContext;
import io.crate.operation.reference.sys.job.JobContextLog;
import io.crate.operation.reference.sys.operation.OperationContext;
import io.crate.operation.reference.sys.operation.OperationContextLog;
import io.crate.operation.user.User;
import io.crate.plugin.SQLPlugin;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.hamcrest.Matchers.is;

public class JobsLogsTest extends CrateDummyClusterServiceUnitTest {

    private ScheduledExecutorService scheduler;
    private CrateCircuitBreakerService breakerService;
    private RamAccountingContext ramAccountingContext;
    private ClusterSettings clusterSettings;

    @Before
    public void createScheduler() {
        clusterSettings = clusterService.getClusterSettings();
        CircuitBreakerService esBreakerService = new HierarchyCircuitBreakerService(Settings.EMPTY, clusterSettings);
        breakerService = new CrateCircuitBreakerService(Settings.EMPTY, clusterSettings, esBreakerService);
        scheduler = Executors.newSingleThreadScheduledExecutor();
        ramAccountingContext = new RamAccountingContext("testRamAccountingContext",
            breakerService.getBreaker(CrateCircuitBreakerService.JOBS_LOG));
    }

    @After
    public void terminateScheduler() throws InterruptedException {
        terminate(scheduler);
    }

    @Override
    protected Set<Setting<?>> additionalClusterSettings() {
        SQLPlugin sqlPlugin = new SQLPlugin(Settings.EMPTY);
        return Sets.newHashSet(sqlPlugin.getSettings());
    }

    @Test
    public void testDefaultSettings() {
        JobsLogService stats = new JobsLogService(Settings.EMPTY, clusterSettings, scheduler, breakerService);
        assertThat(stats.isEnabled(), is(false));
        assertThat(stats.jobsLogSize, is(JobsLogService.STATS_JOBS_LOG_SIZE_SETTING.getDefault()));
        assertThat(stats.operationsLogSize, is(JobsLogService.STATS_OPERATIONS_LOG_SIZE_SETTING.getDefault()));
        assertThat(stats.jobsLogExpiration, is(JobsLogService.STATS_JOBS_LOG_EXPIRATION_SETTING.getDefault()));
        assertThat(stats.operationsLogExpiration, is(JobsLogService.STATS_OPERATIONS_LOG_EXPIRATION_SETTING.getDefault()));

        // even though jobsLog size and opertionsLog size are > 0 it must be a NoopQueue because the stats are disabled
        assertThat(stats.jobsLogSink, Matchers.instanceOf(NoopLogSink.class));
        assertThat(stats.operationsLogSink, Matchers.instanceOf(NoopLogSink.class));
    }

    @Test
    public void testEnableStats() throws Exception {
        Settings settings = Settings.builder()
            .put(JobsLogService.STATS_ENABLED_SETTING.getKey(), false)
            .put(JobsLogService.STATS_JOBS_LOG_SIZE_SETTING.getKey(), 100)
            .put(JobsLogService.STATS_OPERATIONS_LOG_SIZE_SETTING.getKey(), 100)
            .build();
        JobsLogService stats = new JobsLogService(settings, clusterSettings, scheduler, breakerService);

        clusterService.getClusterSettings().applySettings(Settings.builder()
            .put(JobsLogService.STATS_ENABLED_SETTING.getKey(), true)
            .build());

        assertThat(stats.isEnabled(), is(true));
        assertThat(stats.jobsLogSize, is(100));
        assertThat(stats.jobsLogSink, Matchers.instanceOf(QueueSink.class));
        assertThat(stats.operationsLogSize, is(100));
        assertThat(stats.operationsLogSink, Matchers.instanceOf(QueueSink.class));
    }

    @Test
    public void testSettingsChanges() throws Exception {
        Settings settings = Settings.builder()
            .put(JobsLogService.STATS_ENABLED_SETTING.getKey(), true)
            .put(JobsLogService.STATS_JOBS_LOG_SIZE_SETTING.getKey(), 100)
            .put(JobsLogService.STATS_OPERATIONS_LOG_SIZE_SETTING.getKey(), 100)
            .build();
        JobsLogService stats = new JobsLogService(settings, clusterSettings, scheduler, breakerService);

        // sinks are still of type QueueSink
        assertThat(stats.jobsLogSink, Matchers.instanceOf(QueueSink.class));
        assertThat(stats.operationsLogSink, Matchers.instanceOf(QueueSink.class));

        assertThat(inspectRamAccountingQueue((QueueSink) stats.jobsLogSink),
            Matchers.instanceOf(BlockingEvictingQueue.class));
        assertThat(inspectRamAccountingQueue((QueueSink) stats.operationsLogSink),
            Matchers.instanceOf(BlockingEvictingQueue.class));

        clusterSettings.applySettings(Settings.builder()
            .put(JobsLogService.STATS_JOBS_LOG_EXPIRATION_SETTING.getKey(), "10s")
            .put(JobsLogService.STATS_OPERATIONS_LOG_EXPIRATION_SETTING.getKey(), "10s")
            .build());

        assertThat(inspectRamAccountingQueue((QueueSink) stats.jobsLogSink),
            Matchers.instanceOf(ConcurrentLinkedDeque.class));
        assertThat(inspectRamAccountingQueue((QueueSink) stats.operationsLogSink),
            Matchers.instanceOf(ConcurrentLinkedDeque.class));

        // set all to 0 but don't disable stats
        clusterSettings.applySettings(Settings.builder()
            .put(JobsLogService.STATS_JOBS_LOG_SIZE_SETTING.getKey(), 0)
            .put(JobsLogService.STATS_JOBS_LOG_EXPIRATION_SETTING.getKey(), "0s")
            .put(JobsLogService.STATS_OPERATIONS_LOG_SIZE_SETTING.getKey(), 0)
            .put(JobsLogService.STATS_OPERATIONS_LOG_EXPIRATION_SETTING.getKey(), "0s")
            .build());
        assertThat(stats.jobsLogSink, Matchers.instanceOf(NoopLogSink.class));
        assertThat(stats.operationsLogSink, Matchers.instanceOf(NoopLogSink.class));
        assertThat(stats.isEnabled(), is(true));

        clusterSettings.applySettings(Settings.builder()
            .put(JobsLogService.STATS_JOBS_LOG_SIZE_SETTING.getKey(), 200)
            .put(JobsLogService.STATS_OPERATIONS_LOG_SIZE_SETTING.getKey(), 200)
            .put(JobsLogService.STATS_ENABLED_SETTING.getKey(), true)
            .build());
        assertThat(stats.jobsLogSink, Matchers.instanceOf(QueueSink.class));
        assertThat(inspectRamAccountingQueue((QueueSink) stats.jobsLogSink),
            Matchers.instanceOf(BlockingEvictingQueue.class));
        assertThat(stats.operationsLogSink, Matchers.instanceOf(QueueSink.class));
        assertThat(inspectRamAccountingQueue((QueueSink) stats.operationsLogSink),
            Matchers.instanceOf(BlockingEvictingQueue.class));

        // disable stats
        clusterSettings.applySettings(Settings.builder()
            .put(JobsLogService.STATS_ENABLED_SETTING.getKey(), false)
            .build());
        assertThat(stats.isEnabled(), is(false));
        assertThat(stats.jobsLogSink, Matchers.instanceOf(NoopLogSink.class));
        assertThat(stats.operationsLogSink, Matchers.instanceOf(NoopLogSink.class));
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
            .put(JobsLogService.STATS_ENABLED_SETTING.getKey(), true).build();
        JobsLogService stats = new JobsLogService(settings, clusterSettings, scheduler, breakerService);

        stats.jobsLogSink.add(new JobContextLog(
            new JobContext(UUID.randomUUID(), "select 1", 1L, null), null));

        clusterSettings.applySettings(Settings.builder()
            .put(JobsLogService.STATS_ENABLED_SETTING.getKey(), true)
            .put(JobsLogService.STATS_JOBS_LOG_SIZE_SETTING.getKey(), 200).build());

        assertThat(ImmutableList.copyOf(stats.jobsLogSink.iterator()).size(), is(1));

        stats.operationsLogSink.add(new OperationContextLog(
            new OperationContext(1, UUID.randomUUID(), "foo", 2L), null));
        stats.operationsLogSink.add(new OperationContextLog(
            new OperationContext(1, UUID.randomUUID(), "foo", 3L), null));

        clusterSettings.applySettings(Settings.builder()
            .put(JobsLogService.STATS_ENABLED_SETTING.getKey(), true)
            .put(JobsLogService.STATS_OPERATIONS_LOG_SIZE_SETTING.getKey(), 1).build());

        assertThat(ImmutableList.copyOf(stats.jobsLogSink.iterator()).size(), is(1));
    }

    @Test
    public void testExecutionStart() {
        JobsLogs jobsLogs = new JobsLogs(() -> true);
        User user = new User("arthur", ImmutableSet.of(), ImmutableSet.of());

        JobContext jobContext = new JobContext(UUID.randomUUID(), "select 1", 1L, user);
        jobsLogs.logExecutionStart(jobContext.id, jobContext.stmt, user);
        List<JobContext> jobsEntries = ImmutableList.copyOf(jobsLogs.activeJobs().iterator());

        assertThat(jobsEntries.size(), is(1));
        assertThat(jobsEntries.get(0).username(), is(user.name()));
        assertThat(jobsEntries.get(0).stmt(), is("select 1"));
    }

    @Test
    public void testExecutionFailure() {
        JobsLogs jobsLogs = new JobsLogs(() -> true);
        User user = new User("arthur", ImmutableSet.of(), ImmutableSet.of());
        Queue<JobContextLog> q = new BlockingEvictingQueue<>(1);

        jobsLogs.updateJobsLog(new QueueSink<>(q, ramAccountingContext::close));
        jobsLogs.logPreExecutionFailure(UUID.randomUUID(), "select foo", "stmt error", user);

        List<JobContextLog> jobsLogEntries = ImmutableList.copyOf(jobsLogs.jobsLog().iterator());
        assertThat(jobsLogEntries.size(), is(1));
        assertThat(jobsLogEntries.get(0).username(), is(user.name()));
        assertThat(jobsLogEntries.get(0).statement(), is("select foo"));
        assertThat(jobsLogEntries.get(0).errorMessage(), is("stmt error"));
    }

    @Test
    public void testUniqueOperationIdsInOperationsTable() {
        JobsLogs jobsLogs = new JobsLogs(() -> true);
        Queue<OperationContextLog> q = new BlockingEvictingQueue<>(10);
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
        assertThat(JobsLogService.clearInterval(TimeValue.timeValueMillis(1L)), is(1000L));
        assertThat(JobsLogService.clearInterval(TimeValue.timeValueSeconds(8L)), is(1000L));
        assertThat(JobsLogService.clearInterval(TimeValue.timeValueSeconds(10L)), is(1000L));
        assertThat(JobsLogService.clearInterval(TimeValue.timeValueSeconds(20L)), is(2000L));
        assertThat(JobsLogService.clearInterval(TimeValue.timeValueHours(720L)), is(86_400_000L));  // 30 days
    }
}
