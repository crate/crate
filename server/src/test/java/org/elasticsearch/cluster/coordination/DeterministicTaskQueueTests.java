/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.threadpool.ThreadPool.Names.GENERIC;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import io.crate.common.unit.TimeValue;

public class DeterministicTaskQueueTests extends ESTestCase {

    public void testRunRandomTask() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        final List<String> strings = new ArrayList<>(2);

        taskQueue.scheduleNow(() -> strings.add("foo"));
        taskQueue.scheduleNow(() -> strings.add("bar"));

        assertThat(strings, empty());

        assertThat(taskQueue.hasRunnableTasks()).isTrue();
        taskQueue.runRandomTask();
        assertThat(strings).hasSize(1).containsAnyOf("foo", "bar");

        assertThat(taskQueue.hasRunnableTasks()).isTrue();
        taskQueue.runRandomTask();
        assertThat(strings, containsInAnyOrder("foo", "bar"));

        assertThat(taskQueue.hasRunnableTasks()).isFalse();
    }

    public void testRunRandomTaskVariesOrder() {
        final List<String> strings1 = getResultsOfRunningRandomly(new Random(4520795446362137264L));
        final List<String> strings2 = getResultsOfRunningRandomly(new Random(266504691902226821L));
        assertThat(strings1).isNotEqualTo(strings2);
    }

    private List<String> getResultsOfRunningRandomly(Random random) {
        final DeterministicTaskQueue taskQueue = newTaskQueue(random);
        final List<String> strings = new ArrayList<>(4);

        taskQueue.scheduleNow(() -> strings.add("foo"));
        taskQueue.scheduleNow(() -> strings.add("bar"));
        taskQueue.scheduleNow(() -> strings.add("baz"));
        taskQueue.scheduleNow(() -> strings.add("quux"));

        assertThat(strings, empty());

        while (taskQueue.hasRunnableTasks()) {
            taskQueue.runRandomTask();
        }

        assertThat(strings, containsInAnyOrder("foo", "bar", "baz", "quux"));
        return strings;
    }

    public void testStartsAtTimeZero() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        assertThat(taskQueue.getCurrentTimeMillis()).isEqualTo(0L);
    }

    private void advanceToRandomTime(DeterministicTaskQueue taskQueue) {
        taskQueue.scheduleAt(randomLongBetween(1, 100), () -> {
        });
        taskQueue.advanceTime();
        taskQueue.runRandomTask();
        assertThat(taskQueue.hasRunnableTasks()).isFalse();
        assertThat(taskQueue.hasDeferredTasks()).isFalse();
    }

    public void testDoesNotDeferTasksForImmediateExecution() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        advanceToRandomTime(taskQueue);

        final List<String> strings = new ArrayList<>(1);

        taskQueue.scheduleAt(taskQueue.getCurrentTimeMillis(), () -> strings.add("foo"));
        assertThat(taskQueue.hasRunnableTasks()).isTrue();
        assertThat(taskQueue.hasDeferredTasks()).isFalse();
        taskQueue.runRandomTask();
        assertThat(strings).containsExactly("foo");

        assertThat(taskQueue.hasRunnableTasks()).isFalse();
    }

    public void testDoesNotDeferTasksScheduledInThePast() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        advanceToRandomTime(taskQueue);

        final List<String> strings = new ArrayList<>(1);

        taskQueue.scheduleAt(taskQueue.getCurrentTimeMillis() - randomInt(200), () -> strings.add("foo"));
        assertThat(taskQueue.hasRunnableTasks()).isTrue();
        assertThat(taskQueue.hasDeferredTasks()).isFalse();
        taskQueue.runRandomTask();
        assertThat(strings).containsExactly("foo");

        assertThat(taskQueue.hasRunnableTasks()).isFalse();
    }

    public void testDefersTasksWithPositiveDelays() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        final List<String> strings = new ArrayList<>(1);

        final long executionTimeMillis = randomLongBetween(1, 100);
        taskQueue.scheduleAt(executionTimeMillis, () -> strings.add("foo"));
        assertThat(taskQueue.getCurrentTimeMillis()).isEqualTo(0L);
        assertThat(taskQueue.hasRunnableTasks()).isFalse();
        assertThat(taskQueue.hasDeferredTasks()).isTrue();

        taskQueue.advanceTime();
        assertThat(taskQueue.getCurrentTimeMillis()).isEqualTo(executionTimeMillis);
        assertThat(taskQueue.hasRunnableTasks()).isTrue();
        assertThat(taskQueue.hasDeferredTasks()).isFalse();

        taskQueue.runRandomTask();
        assertThat(strings).containsExactly("foo");

        assertThat(taskQueue.hasRunnableTasks()).isFalse();
        assertThat(taskQueue.hasDeferredTasks()).isFalse();
    }

    public void testKeepsFutureTasksDeferred() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        final List<String> strings = new ArrayList<>(2);

        final long executionTimeMillis1 = randomLongBetween(1, 100);
        final long executionTimeMillis2 = randomLongBetween(executionTimeMillis1 + 1, 200);

        taskQueue.scheduleAt(executionTimeMillis1, () -> strings.add("foo"));
        taskQueue.scheduleAt(executionTimeMillis2, () -> strings.add("bar"));

        assertThat(taskQueue.getCurrentTimeMillis()).isEqualTo(0L);
        assertThat(taskQueue.hasRunnableTasks()).isFalse();
        assertThat(taskQueue.hasDeferredTasks()).isTrue();

        taskQueue.advanceTime();
        assertThat(taskQueue.getCurrentTimeMillis()).isEqualTo(executionTimeMillis1);
        assertThat(taskQueue.hasRunnableTasks()).isTrue();
        assertThat(taskQueue.hasDeferredTasks()).isTrue();

        taskQueue.runRandomTask();
        assertThat(strings).containsExactly("foo");
        assertThat(taskQueue.hasRunnableTasks()).isFalse();
        assertThat(taskQueue.hasDeferredTasks()).isTrue();

        taskQueue.advanceTime();
        assertThat(taskQueue.getCurrentTimeMillis()).isEqualTo(executionTimeMillis2);
        assertThat(taskQueue.hasRunnableTasks()).isTrue();
        assertThat(taskQueue.hasDeferredTasks()).isFalse();

        taskQueue.runRandomTask();
        assertThat(strings).containsExactly("foo", "bar");
    }

    public void testExecutesTasksInTimeOrder() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        final List<String> strings = new ArrayList<>(3);

        final long executionTimeMillis1 = randomLongBetween(1, 100);
        final long executionTimeMillis2 = randomLongBetween(executionTimeMillis1 + 100, 300);

        taskQueue.scheduleAt(executionTimeMillis1, () -> strings.add("foo"));
        taskQueue.scheduleAt(executionTimeMillis2, () -> strings.add("bar"));

        assertThat(taskQueue.getCurrentTimeMillis()).isEqualTo(0L);
        assertThat(taskQueue.hasRunnableTasks()).isFalse();
        assertThat(taskQueue.hasDeferredTasks()).isTrue();

        taskQueue.advanceTime();
        assertThat(taskQueue.getCurrentTimeMillis()).isEqualTo(executionTimeMillis1);
        assertThat(taskQueue.hasRunnableTasks()).isTrue();
        assertThat(taskQueue.hasDeferredTasks()).isTrue();

        taskQueue.runRandomTask();
        assertThat(strings).containsExactly("foo");
        assertThat(taskQueue.hasRunnableTasks()).isFalse();
        assertThat(taskQueue.hasDeferredTasks()).isTrue();

        final long executionTimeMillis3 = randomLongBetween(executionTimeMillis1 + 1, executionTimeMillis2 - 1);
        taskQueue.scheduleAt(executionTimeMillis3, () -> strings.add("baz"));

        taskQueue.advanceTime();
        assertThat(taskQueue.getCurrentTimeMillis()).isEqualTo(executionTimeMillis3);
        assertThat(taskQueue.hasRunnableTasks()).isTrue();
        assertThat(taskQueue.hasDeferredTasks()).isTrue();

        taskQueue.runRandomTask();
        taskQueue.advanceTime();
        taskQueue.runRandomTask();
        assertThat(strings).containsExactly("foo", "baz", "bar");
        assertThat(taskQueue.getCurrentTimeMillis()).isEqualTo(executionTimeMillis2);
        assertThat(taskQueue.hasRunnableTasks()).isFalse();
        assertThat(taskQueue.hasDeferredTasks()).isFalse();
    }

    public void testRunInTimeOrder() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        final List<String> strings = new ArrayList<>(2);

        final long executionTimeMillis1 = randomLongBetween(1, 100);
        final long executionTimeMillis2 = randomLongBetween(executionTimeMillis1 + 1, 200);

        taskQueue.scheduleAt(executionTimeMillis1, () -> strings.add("foo"));
        taskQueue.scheduleAt(executionTimeMillis2, () -> strings.add("bar"));

        taskQueue.runAllTasksInTimeOrder();
        assertThat(strings).containsExactly("foo", "bar");
    }

    public void testExecutorServiceEnqueuesTasks() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        final List<String> strings = new ArrayList<>(2);

        final ExecutorService executorService = taskQueue.getExecutorService();
        assertThat(taskQueue.hasRunnableTasks()).isFalse();
        executorService.execute(() -> strings.add("foo"));
        assertThat(taskQueue.hasRunnableTasks()).isTrue();
        executorService.execute(() -> strings.add("bar"));

        assertThat(strings, empty());

        while (taskQueue.hasRunnableTasks()) {
            taskQueue.runRandomTask();
        }

        assertThat(strings, containsInAnyOrder("foo", "bar"));
    }

    public void testThreadPoolEnqueuesTasks() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        final List<String> strings = new ArrayList<>(2);

        final ThreadPool threadPool = taskQueue.getThreadPool();
        assertThat(taskQueue.hasRunnableTasks()).isFalse();
        threadPool.generic().execute(() -> strings.add("foo"));
        assertThat(taskQueue.hasRunnableTasks()).isTrue();
        threadPool.executor("anything").execute(() -> strings.add("bar"));

        assertThat(strings, empty());

        while (taskQueue.hasRunnableTasks()) {
            taskQueue.runRandomTask();
        }

        assertThat(strings, containsInAnyOrder("foo", "bar"));
    }

    public void testThreadPoolWrapsRunnable() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        final AtomicBoolean called = new AtomicBoolean();
        final ThreadPool threadPool = taskQueue.getThreadPool(runnable -> () -> {
            assertThat(called.get()).isFalse();
            called.set(true);
            runnable.run();
        });
        threadPool.generic().execute(() -> logger.info("runnable executed"));
        assertThat(called.get()).isFalse();
        taskQueue.runAllRunnableTasks();
        assertThat(called.get()).isTrue();
    }

    public void testExecutorServiceWrapsRunnable() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        final AtomicBoolean called = new AtomicBoolean();
        final ExecutorService executorService = taskQueue.getExecutorService(runnable -> () -> {
            assertThat(called.get()).isFalse();
            called.set(true);
            runnable.run();
        });
        executorService.execute(() -> logger.info("runnable executed"));
        assertThat(called.get()).isFalse();
        taskQueue.runAllRunnableTasks();
        assertThat(called.get()).isTrue();
    }

    public void testThreadPoolSchedulesFutureTasks() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        advanceToRandomTime(taskQueue);
        final long startTime = taskQueue.getCurrentTimeMillis();

        final List<String> strings = new ArrayList<>(5);

        final ThreadPool threadPool = taskQueue.getThreadPool();
        final long delayMillis = randomLongBetween(1, 100);

        threadPool.schedule(() -> strings.add("deferred"), TimeValue.timeValueMillis(delayMillis), GENERIC);
        assertThat(taskQueue.hasRunnableTasks()).isFalse();
        assertThat(taskQueue.hasDeferredTasks()).isTrue();

        threadPool.schedule(() -> strings.add("runnable"), TimeValue.ZERO, GENERIC);
        assertThat(taskQueue.hasRunnableTasks()).isTrue();

        threadPool.schedule(() -> strings.add("also runnable"), TimeValue.MINUS_ONE, GENERIC);

        taskQueue.runAllTasks();

        assertThat(taskQueue.getCurrentTimeMillis()).isEqualTo(startTime + delayMillis);
        assertThat(strings, containsInAnyOrder("runnable", "also runnable", "deferred"));

        final long delayMillis1 = randomLongBetween(2, 100);
        final long delayMillis2 = randomLongBetween(1, delayMillis1 - 1);

        threadPool.schedule(() -> strings.add("further deferred"), TimeValue.timeValueMillis(delayMillis1), GENERIC);
        threadPool.schedule(() -> strings.add("not quite so deferred"), TimeValue.timeValueMillis(delayMillis2), GENERIC);

        assertThat(taskQueue.hasRunnableTasks()).isFalse();
        assertThat(taskQueue.hasDeferredTasks()).isTrue();

        taskQueue.runAllTasks();
        assertThat(taskQueue.getCurrentTimeMillis()).isEqualTo(startTime + delayMillis + delayMillis1);

        final TimeValue cancelledDelay = TimeValue.timeValueMillis(randomLongBetween(1, 100));
        final Scheduler.Cancellable cancelledBeforeExecution =
            threadPool.schedule(() -> strings.add("cancelled before execution"), cancelledDelay, "");

        cancelledBeforeExecution.cancel();
        taskQueue.runAllTasks();

        assertThat(strings, containsInAnyOrder("runnable", "also runnable", "deferred", "not quite so deferred", "further deferred"));
    }

    public void testDelayVariabilityAppliesToImmediateTasks() {
        final DeterministicTaskQueue deterministicTaskQueue = newTaskQueue();
        advanceToRandomTime(deterministicTaskQueue);
        final long variabilityMillis = randomLongBetween(100, 500);
        deterministicTaskQueue.setExecutionDelayVariabilityMillis(variabilityMillis);
        for (int i = 0; i < 100; i++) {
            deterministicTaskQueue.scheduleNow(() -> {});
        }

        final long startTime = deterministicTaskQueue.getCurrentTimeMillis();
        deterministicTaskQueue.runAllTasks();
        final long elapsedTime = deterministicTaskQueue.getCurrentTimeMillis() - startTime;
        assertThat(elapsedTime, greaterThan(0L)); // fails with negligible probability 2^{-100}
        assertThat(elapsedTime, lessThanOrEqualTo(variabilityMillis));
    }

    public void testDelayVariabilityAppliesToFutureTasks() {
        final DeterministicTaskQueue deterministicTaskQueue = newTaskQueue();
        advanceToRandomTime(deterministicTaskQueue);
        final long nominalExecutionTime = randomLongBetween(0, 60000);
        final long variabilityMillis = randomLongBetween(1, 500);
        final long startTime = deterministicTaskQueue.getCurrentTimeMillis();
        deterministicTaskQueue.setExecutionDelayVariabilityMillis(variabilityMillis);
        for (int i = 0; i < 100; i++) {
            deterministicTaskQueue.scheduleAt(nominalExecutionTime, () -> {});
        }
        final long expectedEndTime = deterministicTaskQueue.getLatestDeferredExecutionTime();
        assertThat(expectedEndTime, greaterThan(nominalExecutionTime)); // fails if every task has zero variability -- vanishingly unlikely
        assertThat(expectedEndTime, lessThanOrEqualTo(Math.max(startTime, nominalExecutionTime + variabilityMillis)));

        deterministicTaskQueue.runAllTasks();
        assertThat(deterministicTaskQueue.getCurrentTimeMillis()).isEqualTo(expectedEndTime);
    }

    public void testThreadPoolSchedulesPeriodicFutureTasks() {
        final DeterministicTaskQueue taskQueue = newTaskQueue();
        advanceToRandomTime(taskQueue);
        final List<String> strings = new ArrayList<>(5);

        final ThreadPool threadPool = taskQueue.getThreadPool();
        final long intervalMillis = randomLongBetween(1, 100);

        final AtomicInteger counter = new AtomicInteger(0);
        Scheduler.Cancellable cancellable = threadPool.scheduleWithFixedDelay(
            () -> strings.add("periodic-" + counter.getAndIncrement()), TimeValue.timeValueMillis(intervalMillis), GENERIC);
        assertThat(taskQueue.hasRunnableTasks()).isFalse();
        assertThat(taskQueue.hasDeferredTasks()).isTrue();

        for (int i = 0; i < 3; ++i) {
            taskQueue.advanceTime();
            assertThat(taskQueue.hasRunnableTasks()).isTrue();
            taskQueue.runAllRunnableTasks();
        }

        assertThat(strings).containsExactly("periodic-0", "periodic-1", "periodic-2");

        cancellable.cancel();

        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();

        assertThat(strings).containsExactly("periodic-0", "periodic-1", "periodic-2");
    }

    static DeterministicTaskQueue newTaskQueue() {
        return newTaskQueue(random());
    }

    private static DeterministicTaskQueue newTaskQueue(Random random) {
        return new DeterministicTaskQueue(Settings.builder().put(NODE_NAME_SETTING.getKey(), "node").build(), random);
    }
}
