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

package io.crate.execution.engine.collect.stats;

import static io.crate.planner.Plan.StatementType.SELECT;
import static io.crate.planner.Plan.StatementType.UNDEFINED;
import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertThat;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import io.crate.common.collections.BlockingEvictingQueue;
import io.crate.common.unit.TimeValue;
import io.crate.expression.reference.sys.job.JobContext;
import io.crate.expression.reference.sys.job.JobContextLog;
import io.crate.expression.reference.sys.operation.OperationContext;
import io.crate.expression.reference.sys.operation.OperationContextLog;
import io.crate.metadata.NodeContext;
import io.crate.metadata.sys.MetricsView;
import io.crate.planner.operators.StatementClassifier.Classification;
import io.crate.role.Role;
import io.crate.role.metadata.RolesHelper;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class JobsLogsTest extends CrateDummyClusterServiceUnitTest {

    private HierarchyCircuitBreakerService breakerService;
    private ClusterSettings clusterSettings;
    private NodeContext nodeCtx;

    @Before
    public void createJobsLogDependencies() {
        clusterSettings = clusterService.getClusterSettings();
        breakerService = new HierarchyCircuitBreakerService(Settings.EMPTY, clusterSettings);
        nodeCtx = createNodeContext();
    }

    @Test
    public void testDefaultSettings() {
        try (JobsLogService stats = new JobsLogService(Settings.EMPTY, clusterService, nodeCtx, breakerService)) {
            assertThat(stats.isEnabled()).isTrue();
            assertThat(stats.jobsLogSize).isEqualTo(JobsLogService.STATS_JOBS_LOG_SIZE_SETTING.getDefault(Settings.EMPTY));
            assertThat(stats.operationsLogSize).isEqualTo(JobsLogService.STATS_OPERATIONS_LOG_SIZE_SETTING.getDefault(Settings.EMPTY));
            assertThat(stats.jobsLogExpiration).isEqualTo(JobsLogService.STATS_JOBS_LOG_EXPIRATION_SETTING.getDefault(Settings.EMPTY));
            assertThat(stats.operationsLogExpiration).isEqualTo(JobsLogService.STATS_OPERATIONS_LOG_EXPIRATION_SETTING.getDefault(Settings.EMPTY));
            assertThat(stats.get().jobsLog(), Matchers.instanceOf(FilteredLogSink.class));
            assertThat(stats.get().operationsLog(), Matchers.instanceOf(QueueSink.class));
        }
    }

    @Test
    public void testEntriesCanBeRejectedWithFilter() {
        Settings settings = Settings.builder()
            .put(JobsLogService.STATS_JOBS_LOG_FILTER.getKey(), "stmt like 'select%'")
            .build();
        try (JobsLogService stats = new JobsLogService(settings, clusterService, nodeCtx, breakerService)) {
            LogSink<JobContextLog> jobsLogSink = (LogSink<JobContextLog>) stats.get().jobsLog();

            jobsLogSink.add(new JobContextLog(
                new JobContext(UUID.randomUUID(), "insert into", 10L, Role.CRATE_USER, null), null, 20L));
            jobsLogSink.add(new JobContextLog(
                new JobContext(UUID.randomUUID(), "select * from t1", 10L, Role.CRATE_USER, null), null, 20L));
            assertThat(StreamSupport.stream(jobsLogSink.spliterator(), false).count()).isEqualTo(1L);
        }
    }

    @Test
    public void testFilterIsValidatedOnUpdate() {
        // creating the service registers the update listener
        try (var jobsLogService = new JobsLogService(Settings.EMPTY, clusterService, nodeCtx, breakerService)) {
            var settings = Settings.builder()
                .put(JobsLogService.STATS_JOBS_LOG_FILTER.getKey(), "statement = 'x'")
                .build();
            assertThatThrownBy(() -> clusterSettings.applySettings(settings))
                .hasMessage("illegal value can't update [stats.jobs_log_filter] from [true] to [statement = 'x']");
        }
    }

    @Test
    public void testErrorIsRaisedInitiallyOnInvalidFilterExpression() {
        Settings settings = Settings.builder()
                .put(JobsLogService.STATS_JOBS_LOG_FILTER.getKey(), "invalid_column = 10")
                .build();

        assertThatThrownBy(() -> new JobsLogService(settings, clusterService, nodeCtx, breakerService).close())
            .hasMessage("Invalid filter expression: invalid_column = 10: Column invalid_column unknown");
    }

    @Test
    public void testReEnableStats() {
        clusterService.getClusterSettings().applySettings(Settings.builder()
            .put(JobsLogService.STATS_ENABLED_SETTING.getKey(), false)
            .build());

        Settings settings = Settings.builder()
            .put(JobsLogService.STATS_ENABLED_SETTING.getKey(), false)
            .put(JobsLogService.STATS_JOBS_LOG_SIZE_SETTING.getKey(), 100)
            .put(JobsLogService.STATS_OPERATIONS_LOG_SIZE_SETTING.getKey(), 100)
            .build();
        try (JobsLogService stats = new JobsLogService(settings, clusterService, nodeCtx, breakerService)) {
            LogSink<JobContextLog> jobsLogSink = (LogSink<JobContextLog>) stats.get().jobsLog();
            LogSink<OperationContextLog> operationsLogSink = (LogSink<OperationContextLog>) stats.get().operationsLog();

            assertThat(stats.isEnabled()).isFalse();
            assertThat(stats.jobsLogSize).isEqualTo(100);
            assertThat(jobsLogSink, Matchers.instanceOf(NoopLogSink.class));
            assertThat(stats.operationsLogSize).isEqualTo(100);
            assertThat(operationsLogSink, Matchers.instanceOf(NoopLogSink.class));

            clusterService.getClusterSettings().applySettings(Settings.builder()
                .put(JobsLogService.STATS_ENABLED_SETTING.getKey(), true)
                .build());

            assertThat(stats.isEnabled()).isTrue();
            assertThat(stats.jobsLogSize).isEqualTo(100);
            assertThat(stats.get().jobsLog(), Matchers.instanceOf(FilteredLogSink.class));
            assertThat(stats.operationsLogSize).isEqualTo(100);
            assertThat(stats.get().operationsLog(), Matchers.instanceOf(QueueSink.class));
        }
    }

    @Test
    public void testSettingsChanges() throws Exception {
        Settings settings = Settings.builder()
            .put(JobsLogService.STATS_ENABLED_SETTING.getKey(), true)
            .put(JobsLogService.STATS_JOBS_LOG_SIZE_SETTING.getKey(), 100)
            .put(JobsLogService.STATS_OPERATIONS_LOG_SIZE_SETTING.getKey(), 100)
            .build();
        try (var stats = new JobsLogService(settings, clusterService, nodeCtx, breakerService)) {
            Supplier<LogSink<JobContextLog>> jobsLogSink = () -> (LogSink<JobContextLog>) stats.get().jobsLog();
            Supplier<LogSink<OperationContextLog>> operationsLogSink = () -> (LogSink<OperationContextLog>) stats.get().operationsLog();

            // sinks are still of type QueueSink
            assertThat(jobsLogSink.get(), Matchers.instanceOf(FilteredLogSink.class));
            assertThat(operationsLogSink.get(), Matchers.instanceOf(QueueSink.class));

            assertThat(inspectRamAccountingQueue((QueueSink) ((FilteredLogSink) jobsLogSink.get()).delegate),
                Matchers.instanceOf(BlockingEvictingQueue.class));
            assertThat(inspectRamAccountingQueue((QueueSink) operationsLogSink.get()),
                Matchers.instanceOf(BlockingEvictingQueue.class));

            clusterSettings.applySettings(Settings.builder()
                .put(JobsLogService.STATS_JOBS_LOG_EXPIRATION_SETTING.getKey(), "10s")
                .put(JobsLogService.STATS_OPERATIONS_LOG_EXPIRATION_SETTING.getKey(), "10s")
                .build());

            assertThat(inspectRamAccountingQueue((QueueSink) ((FilteredLogSink<JobContextLog>) jobsLogSink.get()).delegate),
                Matchers.instanceOf(ConcurrentLinkedDeque.class));
            assertThat(inspectRamAccountingQueue((QueueSink) operationsLogSink.get()),
                Matchers.instanceOf(ConcurrentLinkedDeque.class));

            // set all to 0 but don't disable stats
            clusterSettings.applySettings(Settings.builder()
                .put(JobsLogService.STATS_JOBS_LOG_SIZE_SETTING.getKey(), 0)
                .put(JobsLogService.STATS_JOBS_LOG_EXPIRATION_SETTING.getKey(), "0s")
                .put(JobsLogService.STATS_OPERATIONS_LOG_SIZE_SETTING.getKey(), 0)
                .put(JobsLogService.STATS_OPERATIONS_LOG_EXPIRATION_SETTING.getKey(), "0s")
                .build());
            assertThat(jobsLogSink.get(), Matchers.instanceOf(NoopLogSink.class));
            assertThat(operationsLogSink.get(), Matchers.instanceOf(NoopLogSink.class));
            assertThat(stats.isEnabled()).isTrue();

            clusterSettings.applySettings(Settings.builder()
                .put(JobsLogService.STATS_JOBS_LOG_SIZE_SETTING.getKey(), 200)
                .put(JobsLogService.STATS_OPERATIONS_LOG_SIZE_SETTING.getKey(), 200)
                .put(JobsLogService.STATS_ENABLED_SETTING.getKey(), true)
                .build());
            assertThat(jobsLogSink.get(), Matchers.instanceOf(FilteredLogSink.class));
            assertThat(inspectRamAccountingQueue((QueueSink) ((FilteredLogSink<JobContextLog>) jobsLogSink.get()).delegate),
                Matchers.instanceOf(BlockingEvictingQueue.class));
            assertThat(operationsLogSink.get(), Matchers.instanceOf(QueueSink.class));
            assertThat(inspectRamAccountingQueue((QueueSink) operationsLogSink.get()),
                Matchers.instanceOf(BlockingEvictingQueue.class));

            // disable stats
            clusterSettings.applySettings(Settings.builder()
                .put(JobsLogService.STATS_ENABLED_SETTING.getKey(), false)
                .build());
            assertThat(stats.isEnabled()).isFalse();
            assertThat(jobsLogSink.get(), Matchers.instanceOf(NoopLogSink.class));
            assertThat(operationsLogSink.get(), Matchers.instanceOf(NoopLogSink.class));
        }
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
        try (var stats = new JobsLogService(settings, clusterService, nodeCtx, breakerService)) {
            LogSink<JobContextLog> jobsLogSink = (LogSink<JobContextLog>) stats.get().jobsLog();
            LogSink<OperationContextLog> operationsLogSink = (LogSink<OperationContextLog>) stats.get().operationsLog();

            Classification classification =
                new Classification(SELECT, Collections.singleton("Collect"));

            jobsLogSink.add(new JobContextLog(
                new JobContext(UUID.randomUUID(), "select 1", 1L, Role.CRATE_USER, classification), null));

            clusterSettings.applySettings(Settings.builder()
                .put(JobsLogService.STATS_ENABLED_SETTING.getKey(), true)
                .put(JobsLogService.STATS_JOBS_LOG_SIZE_SETTING.getKey(), 200).build());

            assertThat(StreamSupport.stream(stats.get().jobsLog().spliterator(), false).count()).isEqualTo(1L);

            operationsLogSink.add(new OperationContextLog(
                new OperationContext(1, UUID.randomUUID(), "foo", 2L, () -> -1), null));
            operationsLogSink.add(new OperationContextLog(
                new OperationContext(1, UUID.randomUUID(), "foo", 3L, () -> 1), null));

            clusterSettings.applySettings(Settings.builder()
                .put(JobsLogService.STATS_ENABLED_SETTING.getKey(), true)
                .put(JobsLogService.STATS_OPERATIONS_LOG_SIZE_SETTING.getKey(), 1).build());

            assertThat(StreamSupport.stream(stats.get().operationsLog().spliterator(), false).count()).isEqualTo(1L);
        }
    }

    @Test
    public void testRunningJobsAreNotLostOnSettingsChange() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);

        Settings settings = Settings.builder()
            .put(JobsLogService.STATS_ENABLED_SETTING.getKey(), true).build();
        try (var jobsLogService = new JobsLogService(settings, clusterService, nodeCtx, breakerService)) {
            JobsLogs jobsLogs = jobsLogService.get();

            Classification classification =
                new Classification(SELECT, Collections.singleton("Collect"));

            CountDownLatch latch = new CountDownLatch(2);
            AtomicBoolean doInsertJobs = new AtomicBoolean(true);
            AtomicInteger numJobs = new AtomicInteger();
            int maxQueueSize = JobsLogService.STATS_JOBS_LOG_SIZE_SETTING.getDefault(Settings.EMPTY);

            executor.submit(() -> {
                while (doInsertJobs.get() && numJobs.get() < maxQueueSize) {
                    UUID uuid = UUID.randomUUID();
                    int i = numJobs.getAndIncrement();
                    jobsLogs.logExecutionStart(uuid, "select 1", Role.CRATE_USER, classification);
                    if (i % 2 == 0) {
                        jobsLogs.logExecutionEnd(uuid, null);
                    } else {
                        jobsLogs.logPreExecutionFailure(uuid, "select 1", "failure", Role.CRATE_USER);
                    }
                }
                latch.countDown();
            });
            executor.submit(() -> {
                jobsLogService.updateJobSink(maxQueueSize + 10, JobsLogService.STATS_JOBS_LOG_EXPIRATION_SETTING.getDefault(Settings.EMPTY));
                doInsertJobs.set(false);
                latch.countDown();
            });

            latch.await(10, TimeUnit.SECONDS);
            assertThat(
                StreamSupport.stream(jobsLogs.jobsLog().spliterator(), false).count()).isEqualTo((long) numJobs.get());
        } finally {
            executor.shutdown();
            executor.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testRunningOperationsAreNotLostOnSettingsChange() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);

        Settings settings = Settings.builder()
            .put(JobsLogService.STATS_ENABLED_SETTING.getKey(), true).build();
        try (var jobsLogService = new JobsLogService(settings, clusterService, nodeCtx, breakerService)) {
            JobsLogs jobsLogs = jobsLogService.get();

            CountDownLatch latch = new CountDownLatch(2);
            AtomicBoolean doInsertJobs = new AtomicBoolean(true);
            AtomicInteger numJobs = new AtomicInteger();
            int maxQueueSize = JobsLogService.STATS_OPERATIONS_LOG_SIZE_SETTING.getDefault(Settings.EMPTY);

            executor.submit(() -> {
                while (doInsertJobs.get() && numJobs.get() < maxQueueSize) {
                    UUID uuid = UUID.randomUUID();
                    jobsLogs.operationStarted(1, uuid, "dummy", () -> -1);
                    jobsLogs.operationFinished(1, uuid, null);
                    numJobs.incrementAndGet();
                }
                latch.countDown();
            });
            executor.submit(() -> {
                jobsLogService.updateOperationSink(maxQueueSize + 10, JobsLogService.STATS_OPERATIONS_LOG_EXPIRATION_SETTING.getDefault(Settings.EMPTY));
                doInsertJobs.set(false);
                latch.countDown();
            });

            latch.await(10, TimeUnit.SECONDS);
            assertThat(
                StreamSupport.stream(jobsLogs.operationsLog().spliterator(), false).count()).isEqualTo((long) numJobs.get());
        } finally {
            executor.shutdown();
            executor.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testExecutionStart() {
        JobsLogs jobsLogs = new JobsLogs(() -> true);
        Role user = RolesHelper.userOf("arthur");

        Classification classification =
            new Classification(SELECT, Collections.singleton("Collect"));

        JobContext jobContext = new JobContext(UUID.randomUUID(), "select 1", 1L, user, classification);
        jobsLogs.logExecutionStart(jobContext.id(), jobContext.stmt(), user, classification);
        List<JobContext> jobsEntries = StreamSupport.stream(jobsLogs.activeJobs().spliterator(), false)
            .collect(Collectors.toList());

        assertThat(jobsEntries).hasSize(1);
        assertThat(jobsEntries.get(0).username()).isEqualTo(user.name());
        assertThat(jobsEntries.get(0).stmt()).isEqualTo("select 1");
        assertThat(jobsEntries.get(0).classification()).isEqualTo(classification);
    }

    @Test
    public void testExecutionFailure() {
        JobsLogs jobsLogs = new JobsLogs(() -> true);
        Role user = RolesHelper.userOf("arthur");
        Queue<JobContextLog> q = new BlockingEvictingQueue<>(1);

        jobsLogs.updateJobsLog(new QueueSink<>(q, () -> {}));
        jobsLogs.logPreExecutionFailure(UUID.randomUUID(), "select foo", "stmt error", user);

        List<JobContextLog> jobsLogEntries = StreamSupport.stream(jobsLogs.jobsLog().spliterator(), false)
            .collect(Collectors.toList());
        assertThat(jobsLogEntries).hasSize(1);
        assertThat(jobsLogEntries.get(0).username()).isEqualTo(user.name());
        assertThat(jobsLogEntries.get(0).statement()).isEqualTo("select foo");
        assertThat(jobsLogEntries.get(0).errorMessage()).isEqualTo("stmt error");
        assertThat(jobsLogEntries.get(0).classification()).isEqualTo(new Classification(UNDEFINED));
    }

    @Test
    public void testExecutionFailureIsRecordedInMetrics() {
        JobsLogs jobsLogs = new JobsLogs(() -> true);
        Role user = RolesHelper.userOf("arthur");
        Queue<JobContextLog> q = new BlockingEvictingQueue<>(1);

        jobsLogs.updateJobsLog(new QueueSink<>(q, () -> {}));
        jobsLogs.logPreExecutionFailure(UUID.randomUUID(), "select foo", "stmt error", user);

        List<MetricsView> metrics = StreamSupport.stream(jobsLogs.metrics().spliterator(), false)
            .collect(Collectors.toList());
        assertThat(metrics).hasSize(1);
        assertThat(metrics.get(0).failedCount()).isEqualTo(1L);
        assertThat(metrics.get(0).totalCount()).isEqualTo(1L);
        assertThat(metrics.get(0).classification()).isEqualTo(new Classification(UNDEFINED));
    }

    @Test
    public void testUniqueOperationIdsInOperationsTable() {
        JobsLogs jobsLogs = new JobsLogs(() -> true);
        Queue<OperationContextLog> q = new BlockingEvictingQueue<>(10);
        jobsLogs.updateOperationsLog(new QueueSink<>(q, () -> {}));

        OperationContext ctxA = new OperationContext(0, UUID.randomUUID(), "dummyOperation", 1L, () -> -1);
        jobsLogs.operationStarted(ctxA.id, ctxA.jobId, ctxA.name, () -> -1);

        OperationContext ctxB = new OperationContext(0, UUID.randomUUID(), "dummyOperation", 1L, () -> -1);
        jobsLogs.operationStarted(ctxB.id, ctxB.jobId, ctxB.name, () -> 1);

        jobsLogs.operationFinished(ctxB.id, ctxB.jobId, null);
        List<OperationContextLog> entries = StreamSupport.stream(jobsLogs.operationsLog().spliterator(), false)
            .collect(Collectors.toList());

        assertThat(entries).containsExactly(new OperationContextLog(ctxB, null));
        assertThat(entries.contains(new OperationContextLog(ctxA, null))).isFalse();

        jobsLogs.operationFinished(ctxA.id, ctxA.jobId, null);
        entries = StreamSupport.stream(jobsLogs.operationsLog().spliterator(), false)
            .collect(Collectors.toList());
        assertThat(entries.contains(new OperationContextLog(ctxA, null))).isTrue();
    }

    @Test
    public void testLowerBoundScheduler() {
        assertThat(JobsLogService.clearInterval(TimeValue.timeValueMillis(1L))).isEqualTo(1000L);
        assertThat(JobsLogService.clearInterval(TimeValue.timeValueSeconds(8L))).isEqualTo(1000L);
        assertThat(JobsLogService.clearInterval(TimeValue.timeValueSeconds(10L))).isEqualTo(1000L);
        assertThat(JobsLogService.clearInterval(TimeValue.timeValueSeconds(20L))).isEqualTo(2000L);
        assertThat(JobsLogService.clearInterval(TimeValue.timeValueHours(720L))).isEqualTo(86_400_000L);  // 30 days
    }
}
