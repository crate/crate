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
package org.elasticsearch.cluster.service;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.LocalClusterUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.ClusterStatePublisher;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.BaseFuture;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import io.crate.common.collections.Tuple;
import io.crate.common.unit.TimeValue;

public class MasterServiceTests extends ESTestCase {

    private static ThreadPool threadPool;
    private static long relativeTimeInMillis;

    @BeforeClass
    public static void createThreadPool() {
        threadPool = new TestThreadPool(MasterServiceTests.class.getName()) {
            @Override
            public long relativeTimeInMillis() {
                return relativeTimeInMillis;
            }
        };
    }

    @AfterClass
    public static void stopThreadPool() {
        if (threadPool != null) {
            threadPool.shutdownNow();
            threadPool = null;
        }
    }

    @Before
    public void randomizeCurrentTime() {
        relativeTimeInMillis = randomLongBetween(0L, 1L << 62);
    }

    private MasterService createMasterService(boolean makeMaster) {
        final DiscoveryNode localNode = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(),
            emptySet(), Version.CURRENT);
        final MasterService masterService = new MasterService(Settings.builder()
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), MasterServiceTests.class.getSimpleName())
            .put(Node.NODE_NAME_SETTING.getKey(), "test_node")
            .build(), new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), threadPool);
        final ClusterState initialClusterState = ClusterState.builder(new ClusterName(MasterServiceTests.class.getSimpleName()))
            .nodes(DiscoveryNodes.builder()
                .add(localNode)
                .localNodeId(localNode.getId())
                .masterNodeId(makeMaster ? localNode.getId() : null))
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).build();
        final AtomicReference<ClusterState> clusterStateRef = new AtomicReference<>(initialClusterState);
        masterService.setClusterStatePublisher((event, publishListener, ackListener) -> {
            clusterStateRef.set(event.state());
            publishListener.onResponse(null);
        });
        masterService.setClusterStateSupplier(clusterStateRef::get);
        masterService.start();
        return masterService;
    }

    public void testMasterAwareExecution() throws Exception {
        final MasterService nonMaster = createMasterService(false);

        final boolean[] taskFailed = {false};
        final CountDownLatch latch1 = new CountDownLatch(1);
        nonMaster.submitStateUpdateTask("test", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                latch1.countDown();
                return currentState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                taskFailed[0] = true;
                latch1.countDown();
            }
        });

        latch1.await();
        assertTrue("cluster state update task was executed on a non-master", taskFailed[0]);

        final CountDownLatch latch2 = new CountDownLatch(1);
        nonMaster.submitStateUpdateTask("test", new LocalClusterUpdateTask() {
            @Override
            public ClusterTasksResult<LocalClusterUpdateTask> execute(ClusterState currentState) {
                taskFailed[0] = false;
                latch2.countDown();
                return unchanged();
            }

            @Override
            public void onFailure(String source, Exception e) {
                taskFailed[0] = true;
                latch2.countDown();
            }
        });
        latch2.await();
        assertFalse("non-master cluster state update task was not executed", taskFailed[0]);

        nonMaster.close();
    }


    /*
   * test that a listener throwing an exception while handling a
   * notification does not prevent publication notification to the
   * executor
   */
    public void testClusterStateTaskListenerThrowingExceptionIsOkay() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean published = new AtomicBoolean();

        try (MasterService masterService = createMasterService(true)) {
            masterService.submitStateUpdateTask(
                "testClusterStateTaskListenerThrowingExceptionIsOkay",
                new Object(),
                ClusterStateTaskConfig.build(Priority.NORMAL),
                new ClusterStateTaskExecutor<Object>() {
                    @Override
                    public ClusterTasksResult<Object> execute(ClusterState currentState, List<Object> tasks) {
                        ClusterState newClusterState = ClusterState.builder(currentState).build();
                        return ClusterTasksResult.builder().successes(tasks).build(newClusterState);
                    }

                    @Override
                    public void clusterStatePublished(ClusterChangedEvent clusterChangedEvent) {
                        published.set(true);
                        latch.countDown();
                    }
                },
                new ClusterStateTaskListener() {
                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        throw new IllegalStateException(source);
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                    }
                }
            );

            latch.await();
            assertTrue(published.get());
        }
    }

    @TestLogging("org.elasticsearch.cluster.service:TRACE") // To ensure that we log cluster state events on TRACE level
    public void testClusterStateUpdateLogging() throws Exception {
        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test1 start",
                MasterService.class.getCanonicalName(),
                Level.DEBUG,
                "executing cluster state update for [test1]"));
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test1 computation",
                MasterService.class.getCanonicalName(),
                Level.DEBUG,
                "took [1s] to compute cluster state update for [test1]"));
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test1 notification",
                MasterService.class.getCanonicalName(),
                Level.DEBUG,
                "took [0s] to notify listeners on unchanged cluster state for [test1]"));

        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test2 start",
                MasterService.class.getCanonicalName(),
                Level.DEBUG,
                "executing cluster state update for [test2]"));
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test2 failure",
                MasterService.class.getCanonicalName(),
                Level.TRACE,
                "failed to execute cluster state update (on version: [*], uuid: [*]) for [test2]*"));
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test2 computation",
                MasterService.class.getCanonicalName(),
                Level.DEBUG,
                "took [2s] to compute cluster state update for [test2]"));
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test2 notification",
                MasterService.class.getCanonicalName(),
                Level.DEBUG,
                "took [0s] to notify listeners on unchanged cluster state for [test2]"));

        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test3 start",
                MasterService.class.getCanonicalName(),
                Level.DEBUG,
                "executing cluster state update for [test3]"));
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test3 computation",
                MasterService.class.getCanonicalName(),
                Level.DEBUG,
                "took [3s] to compute cluster state update for [test3]"));
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test3 notification",
                MasterService.class.getCanonicalName(),
                Level.DEBUG,
                "took [4s] to notify listeners on successful publication of cluster state (version: *, uuid: *) for [test3]"));

        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test4",
                MasterService.class.getCanonicalName(),
                Level.DEBUG,
                "executing cluster state update for [test4]"));

        Logger clusterLogger = LogManager.getLogger(MasterService.class);
        Loggers.addAppender(clusterLogger, mockAppender);
        try (MasterService masterService = createMasterService(true)) {
            masterService.submitStateUpdateTask("test1", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    relativeTimeInMillis += TimeValue.timeValueSeconds(1).millis();
                    return currentState;
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) { }

                @Override
                public void onFailure(String source, Exception e) {
                    fail();
                }
            });
            masterService.submitStateUpdateTask("test2", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    relativeTimeInMillis += TimeValue.timeValueSeconds(2).millis();
                    throw new IllegalArgumentException("Testing handling of exceptions in the cluster state task");
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    fail();
                }

                @Override
                public void onFailure(String source, Exception e) { }
            });
            masterService.submitStateUpdateTask("test3", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    relativeTimeInMillis += TimeValue.timeValueSeconds(3).millis();
                    return ClusterState.builder(currentState).incrementVersion().build();
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    relativeTimeInMillis += TimeValue.timeValueSeconds(4).millis();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    fail();
                }
            });
            masterService.submitStateUpdateTask("test4", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return currentState;
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) { }

                @Override
                public void onFailure(String source, Exception e) {
                    fail();
                }
            });
            assertBusy(mockAppender::assertAllExpectationsMatched);
        } finally {
            Loggers.removeAppender(clusterLogger, mockAppender);
            mockAppender.stop();
        }
    }

    public void testClusterStateBatchedUpdates() throws BrokenBarrierException, InterruptedException {
        AtomicInteger counter = new AtomicInteger();
        class Task {
            private AtomicBoolean state = new AtomicBoolean();
            private final int id;

            Task(int id) {
                this.id = id;
            }

            public void execute() {
                if (!state.compareAndSet(false, true)) {
                    throw new IllegalStateException();
                } else {
                    counter.incrementAndGet();
                }
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                Task task = (Task) o;
                return id == task.id;

            }

            @Override
            public int hashCode() {
                return id;
            }

            @Override
            public String toString() {
                return Integer.toString(id);
            }
        }

        int numberOfThreads = randomIntBetween(2, 8);
        int taskSubmissionsPerThread = randomIntBetween(1, 64);
        int numberOfExecutors = Math.max(1, numberOfThreads / 4);
        final Semaphore semaphore = new Semaphore(numberOfExecutors);

        class TaskExecutor implements ClusterStateTaskExecutor<Task> {
            private final List<Set<Task>> taskGroups;
            private AtomicInteger counter = new AtomicInteger();
            private AtomicInteger batches = new AtomicInteger();
            private AtomicInteger published = new AtomicInteger();

            TaskExecutor(List<Set<Task>> taskGroups) {
                this.taskGroups = taskGroups;
            }

            @Override
            public ClusterTasksResult<Task> execute(ClusterState currentState, List<Task> tasks) throws Exception {
                for (Set<Task> expectedSet : taskGroups) {
                    long count = tasks.stream().filter(expectedSet::contains).count();
                    assertThat("batched set should be executed together or not at all. Expected " + expectedSet + "s. Executing " + tasks,
                        count, anyOf(equalTo(0L), equalTo((long) expectedSet.size())));
                }
                tasks.forEach(Task::execute);
                counter.addAndGet(tasks.size());
                ClusterState maybeUpdatedClusterState = currentState;
                if (randomBoolean()) {
                    maybeUpdatedClusterState = ClusterState.builder(currentState).build();
                    batches.incrementAndGet();
                    semaphore.acquire();
                }
                return ClusterTasksResult.<Task>builder().successes(tasks).build(maybeUpdatedClusterState);
            }

            @Override
            public void clusterStatePublished(ClusterChangedEvent clusterChangedEvent) {
                published.incrementAndGet();
                semaphore.release();
            }
        }

        ConcurrentMap<String, AtomicInteger> processedStates = new ConcurrentHashMap<>();

        List<Set<Task>> taskGroups = new ArrayList<>();
        List<TaskExecutor> executors = new ArrayList<>();
        for (int i = 0; i < numberOfExecutors; i++) {
            executors.add(new TaskExecutor(taskGroups));
        }

        // randomly assign tasks to executors
        List<Tuple<TaskExecutor, Set<Task>>> assignments = new ArrayList<>();
        int taskId = 0;
        for (int i = 0; i < numberOfThreads; i++) {
            for (int j = 0; j < taskSubmissionsPerThread; j++) {
                TaskExecutor executor = randomFrom(executors);
                Set<Task> tasks = new HashSet<>();
                for (int t = randomInt(3); t >= 0; t--) {
                    tasks.add(new Task(taskId++));
                }
                taskGroups.add(tasks);
                assignments.add(new Tuple<>(executor, tasks));
            }
        }

        Map<TaskExecutor, Integer> counts = new HashMap<>();
        int totalTaskCount = 0;
        for (Tuple<TaskExecutor, Set<Task>> assignment : assignments) {
            final int taskCount = assignment.v2().size();
            counts.merge(assignment.v1(), taskCount, (previous, count) -> previous + count);
            totalTaskCount += taskCount;
        }
        final CountDownLatch updateLatch = new CountDownLatch(totalTaskCount);
        final ClusterStateTaskListener listener = new ClusterStateTaskListener() {
            @Override
            public void onFailure(String source, Exception e) {
                throw new AssertionError(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                processedStates.computeIfAbsent(source, key -> new AtomicInteger()).incrementAndGet();
                updateLatch.countDown();
            }
        };

        try (MasterService masterService = createMasterService(true)) {
            final ConcurrentMap<String, AtomicInteger> submittedTasksPerThread = new ConcurrentHashMap<>();
            CyclicBarrier barrier = new CyclicBarrier(1 + numberOfThreads);
            for (int i = 0; i < numberOfThreads; i++) {
                final int index = i;
                Thread thread = new Thread(() -> {
                    final String threadName = Thread.currentThread().getName();
                    try {
                        barrier.await();
                        for (int j = 0; j < taskSubmissionsPerThread; j++) {
                            Tuple<TaskExecutor, Set<Task>> assignment = assignments.get(index * taskSubmissionsPerThread + j);
                            final Set<Task> tasks = assignment.v2();
                            submittedTasksPerThread.computeIfAbsent(threadName, key -> new AtomicInteger()).addAndGet(tasks.size());
                            final TaskExecutor executor = assignment.v1();
                            if (tasks.size() == 1) {
                                masterService.submitStateUpdateTask(
                                    threadName,
                                    tasks.stream().findFirst().get(),
                                    ClusterStateTaskConfig.build(randomFrom(Priority.values())),
                                    executor,
                                    listener);
                            } else {
                                Map<Task, ClusterStateTaskListener> taskListeners = new HashMap<>();
                                tasks.forEach(t -> taskListeners.put(t, listener));
                                masterService.submitStateUpdateTasks(
                                    threadName,
                                    taskListeners, ClusterStateTaskConfig.build(randomFrom(Priority.values())),
                                    executor
                                );
                            }
                        }
                        barrier.await();
                    } catch (BrokenBarrierException | InterruptedException e) {
                        throw new AssertionError(e);
                    }
                });
                thread.start();
            }

            // wait for all threads to be ready
            barrier.await();
            // wait for all threads to finish
            barrier.await();

            // wait until all the cluster state updates have been processed
            updateLatch.await();
            // and until all of the publication callbacks have completed
            semaphore.acquire(numberOfExecutors);

            // assert the number of executed tasks is correct
            assertEquals(totalTaskCount, counter.get());

            // assert each executor executed the correct number of tasks
            for (TaskExecutor executor : executors) {
                if (counts.containsKey(executor)) {
                    assertEquals((int) counts.get(executor), executor.counter.get());
                    assertEquals(executor.batches.get(), executor.published.get());
                }
            }

            // assert the correct number of clusterStateProcessed events were triggered
            for (Map.Entry<String, AtomicInteger> entry : processedStates.entrySet()) {
                assertThat(submittedTasksPerThread, hasKey(entry.getKey()));
                assertEquals("not all tasks submitted by " + entry.getKey() + " received a processed event",
                    entry.getValue().get(), submittedTasksPerThread.get(entry.getKey()).get());
            }
        }
    }

    public void testBlockingCallInClusterStateTaskListenerFails() throws InterruptedException {
        assumeTrue("assertions must be enabled for this test to work", BaseFuture.class.desiredAssertionStatus());
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<AssertionError> assertionRef = new AtomicReference<>();

        try (MasterService masterService = createMasterService(true)) {
            masterService.submitStateUpdateTask(
                "testBlockingCallInClusterStateTaskListenerFails",
                new Object(),
                ClusterStateTaskConfig.build(Priority.NORMAL),
                (currentState, tasks) -> {
                    ClusterState newClusterState = ClusterState.builder(currentState).build();
                    return ClusterStateTaskExecutor.ClusterTasksResult.builder().successes(tasks).build(newClusterState);
                },
                new ClusterStateTaskListener() {
                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        BaseFuture<Void> future = new BaseFuture<Void>() {
                        };
                        try {
                            if (randomBoolean()) {
                                future.get(1L, TimeUnit.SECONDS);
                            } else {
                                future.get();
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        } catch (AssertionError e) {
                            assertionRef.set(e);
                            latch.countDown();
                        }
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                    }
                }
            );

            latch.await();
            assertNotNull(assertionRef.get());
            assertThat(assertionRef.get().getMessage(), containsString("Reason: [Blocking operation]"));
        }
    }

    @TestLogging("org.elasticsearch.cluster.service:WARN") // To ensure that we log cluster state events on WARN level
    public void testLongClusterStateUpdateLogging() throws Exception {
        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        mockAppender.addExpectation(
            new MockLogAppender.UnseenEventExpectation(
                "test1 shouldn't log because it was fast enough",
                MasterService.class.getCanonicalName(),
                Level.WARN,
                "*took*test1*"));
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test2",
                MasterService.class.getCanonicalName(),
                Level.WARN,
                "*took [*], which is over [10s], to compute cluster state update for [test2]"));
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test3",
                MasterService.class.getCanonicalName(),
                Level.WARN,
                "*took [*], which is over [10s], to compute cluster state update for [test3]"));
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test4",
                MasterService.class.getCanonicalName(),
                Level.WARN,
                "*took [*], which is over [10s], to compute cluster state update for [test4]"));
        mockAppender.addExpectation(
            new MockLogAppender.UnseenEventExpectation(
                "test5 should not log despite publishing slowly",
                MasterService.class.getCanonicalName(),
                Level.WARN,
                "*took*test5*"));
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test6 should log due to slow and failing publication",
                MasterService.class.getCanonicalName(),
                Level.WARN,
                "took [*] and then failed to publish updated cluster state (version: *, uuid: *) for [test6]:*"));

        Logger clusterLogger = LogManager.getLogger(MasterService.class);
        Loggers.addAppender(clusterLogger, mockAppender);
        try (MasterService masterService = new MasterService(Settings.builder()
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), MasterServiceTests.class.getSimpleName())
            .put(Node.NODE_NAME_SETTING.getKey(), "test_node")
            .build(), new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), threadPool)) {

            final DiscoveryNode localNode = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(),
                emptySet(), Version.CURRENT);
            final ClusterState initialClusterState = ClusterState.builder(new ClusterName(MasterServiceTests.class.getSimpleName()))
                .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId()))
                .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).build();
            final AtomicReference<ClusterState> clusterStateRef = new AtomicReference<>(initialClusterState);
            masterService.setClusterStatePublisher((event, publishListener, ackListener) -> {
                if (event.source().contains("test5")) {
                    relativeTimeInMillis += MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(Settings.EMPTY).millis()
                        + randomLongBetween(1, 1000000);
                }
                if (event.source().contains("test6")) {
                    relativeTimeInMillis += MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(Settings.EMPTY).millis()
                        + randomLongBetween(1, 1000000);
                    throw new ElasticsearchException("simulated error during slow publication which should trigger logging");
                }
                clusterStateRef.set(event.state());
                publishListener.onResponse(null);
            });
            masterService.setClusterStateSupplier(clusterStateRef::get);
            masterService.start();

            final CountDownLatch latch = new CountDownLatch(6);
            final CountDownLatch processedFirstTask = new CountDownLatch(1);
            masterService.submitStateUpdateTask("test1", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    relativeTimeInMillis += randomLongBetween(0L,
                        MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(Settings.EMPTY).millis());
                    return currentState;
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                    processedFirstTask.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    fail();
                }
            });

            processedFirstTask.await();
            masterService.submitStateUpdateTask("test2", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    relativeTimeInMillis += MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(Settings.EMPTY).millis()
                        + randomLongBetween(1, 1000000);
                    throw new IllegalArgumentException("Testing handling of exceptions in the cluster state task");
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    fail();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    latch.countDown();
                }
            });
            masterService.submitStateUpdateTask("test3", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    relativeTimeInMillis += MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(Settings.EMPTY).millis()
                        + randomLongBetween(1, 1000000);
                    return ClusterState.builder(currentState).incrementVersion().build();
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    fail();
                }
            });
            masterService.submitStateUpdateTask("test4", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    relativeTimeInMillis += MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(Settings.EMPTY).millis()
                        + randomLongBetween(1, 1000000);
                    return currentState;
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    fail();
                }
            });
            masterService.submitStateUpdateTask("test5", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return ClusterState.builder(currentState).incrementVersion().build();
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    fail();
                }
            });
            masterService.submitStateUpdateTask("test6", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return ClusterState.builder(currentState).incrementVersion().build();
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    fail();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    fail(); // maybe we should notify here?
                }
            });
            // Additional update task to make sure all previous logging made it to the loggerName
            // We don't check logging for this on since there is no guarantee that it will occur before our check
            masterService.submitStateUpdateTask("test7", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return currentState;
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    fail();
                }
            });
            latch.await();
        } finally {
            Loggers.removeAppender(clusterLogger, mockAppender);
            mockAppender.stop();
        }
        mockAppender.assertAllExpectationsMatched();
    }

    public void testAcking() throws InterruptedException {
        final DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        final DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        final DiscoveryNode node3 = new DiscoveryNode("node3", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        try (MasterService masterService = new MasterService(Settings.builder()
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), MasterServiceTests.class.getSimpleName())
            .put(Node.NODE_NAME_SETTING.getKey(), "test_node")
            .build(), new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), threadPool)) {

            final ClusterState initialClusterState = ClusterState.builder(new ClusterName(MasterServiceTests.class.getSimpleName()))
                .nodes(DiscoveryNodes.builder()
                    .add(node1)
                    .add(node2)
                    .add(node3)
                    .localNodeId(node1.getId())
                    .masterNodeId(node1.getId()))
                .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).build();
            final AtomicReference<ClusterStatePublisher> publisherRef = new AtomicReference<>();
            masterService.setClusterStatePublisher((e, pl, al) -> publisherRef.get().publish(e, pl, al));
            masterService.setClusterStateSupplier(() -> initialClusterState);
            masterService.start();

            // check that we don't time out before even committing the cluster state
            {
                final CountDownLatch latch = new CountDownLatch(1);

                publisherRef.set((clusterChangedEvent, publishListener, ackListener) ->
                    publishListener.onFailure(new FailedToCommitClusterStateException("mock exception")));

                masterService.submitStateUpdateTask("test2", new AckedClusterStateUpdateTask<Void>(null) {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return ClusterState.builder(currentState).build();
                    }

                    @Override
                    public TimeValue ackTimeout() {
                        return TimeValue.ZERO;
                    }

                    @Override
                    public TimeValue timeout() {
                        return null;
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        fail();
                    }

                    @Override
                    protected Void newResponse(boolean acknowledged) {
                        fail();
                        return null;
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        latch.countDown();
                    }

                    @Override
                    public void onAckTimeout() {
                        fail();
                    }
                });

                latch.await();
            }

            // check that we timeout if commit took too long
            {
                final CountDownLatch latch = new CountDownLatch(2);

                final TimeValue ackTimeout = TimeValue.timeValueMillis(randomInt(100));

                publisherRef.set((clusterChangedEvent, publishListener, ackListener) -> {
                    publishListener.onResponse(null);
                    ackListener.onCommit(TimeValue.timeValueMillis(ackTimeout.millis() + randomInt(100)));
                    ackListener.onNodeAck(node1, null);
                    ackListener.onNodeAck(node2, null);
                    ackListener.onNodeAck(node3, null);
                });

                masterService.submitStateUpdateTask("test2", new AckedClusterStateUpdateTask<Void>(null) {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return ClusterState.builder(currentState).build();
                    }

                    @Override
                    public TimeValue ackTimeout() {
                        return ackTimeout;
                    }

                    @Override
                    public TimeValue timeout() {
                        return null;
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        latch.countDown();
                    }

                    @Override
                    protected Void newResponse(boolean acknowledged) {
                        fail();
                        return null;
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        fail();
                    }

                    @Override
                    public void onAckTimeout() {
                        latch.countDown();
                    }
                });

                latch.await();
            }
        }
    }

    /**
     * Returns the cluster state that the master service uses (and that is provided by the discovery layer)
     */
    public static ClusterState discoveryState(MasterService masterService) {
        return masterService.state();
    }
}
