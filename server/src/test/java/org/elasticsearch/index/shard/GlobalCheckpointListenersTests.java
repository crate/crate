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

package org.elasticsearch.index.shard;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.elasticsearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Assertions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.junit.After;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import io.crate.common.unit.TimeValue;

public class GlobalCheckpointListenersTests extends ESTestCase {

    @FunctionalInterface
    interface TestGlobalCheckpointListener extends GlobalCheckpointListeners.GlobalCheckpointListener {

        default Executor executor() {
            return Runnable::run;
        }

    }

    private final ShardId shardId = new ShardId(new Index("index", "uuid"), 0);
    private final ScheduledThreadPoolExecutor scheduler =
            new Scheduler.SafeScheduledThreadPoolExecutor(1, EsExecutors.daemonThreadFactory(Settings.EMPTY, "scheduler"));

    @After
    public void shutdownScheduler() {
        scheduler.shutdown();
    }

    @Test
    public void testGlobalCheckpointUpdated() throws IOException {
        final GlobalCheckpointListeners globalCheckpointListeners =
                new GlobalCheckpointListeners(shardId, scheduler, logger);
        globalCheckpointListeners.globalCheckpointUpdated(NO_OPS_PERFORMED);
        final int numberOfListeners = randomIntBetween(0, 64);
        final Map<GlobalCheckpointListeners.GlobalCheckpointListener, Long> listeners = new HashMap<>();
        final Map<GlobalCheckpointListeners.GlobalCheckpointListener, Long> notifiedListeners = new HashMap<>();
        for (int i = 0; i < numberOfListeners; i++) {
            final TestGlobalCheckpointListener listener = new TestGlobalCheckpointListener() {

                @Override
                public void accept(final long g, final Exception e) {
                    notifiedListeners.put(this, g);
                }

            };
            final long waitingGlobalCheckpoint = randomLongBetween(NO_OPS_PERFORMED, Long.MAX_VALUE);
            listeners.put(listener, waitingGlobalCheckpoint);
            globalCheckpointListeners.add(waitingGlobalCheckpoint, maybeMultipleInvocationProtectingListener(listener), null);
        }
        final long globalCheckpoint = randomLongBetween(NO_OPS_PERFORMED, Long.MAX_VALUE - 1);
        globalCheckpointListeners.globalCheckpointUpdated(globalCheckpoint);
        for (final Map.Entry<GlobalCheckpointListeners.GlobalCheckpointListener, Long> listener : listeners.entrySet()) {
            if (listener.getValue() <= globalCheckpoint) {
                // only listeners waiting on a lower global checkpoint will have been notified
                assertThat(notifiedListeners.get(listener.getKey())).isEqualTo(globalCheckpoint);
            } else {
                assertThat(notifiedListeners.get(listener.getKey())).isNull();
            }
        }

        // test the listeners are not invoked twice
        notifiedListeners.clear();
        final long nextGlobalCheckpoint = randomLongBetween(1 + globalCheckpoint, Long.MAX_VALUE);
        globalCheckpointListeners.globalCheckpointUpdated(nextGlobalCheckpoint);
        for (final Map.Entry<GlobalCheckpointListeners.GlobalCheckpointListener, Long> listener : listeners.entrySet()) {
            if (listener.getValue() > globalCheckpoint && listener.getValue() <= nextGlobalCheckpoint) {
                // these listeners will have been notified by the second global checkpoint update, and all the other listeners should not be
                assertThat(notifiedListeners.get(listener.getKey())).isEqualTo(nextGlobalCheckpoint);
            } else {
                assertThat(notifiedListeners.get(listener.getKey())).isNull();
            }
        }

        // closing should also not notify the listeners
        notifiedListeners.clear();
        globalCheckpointListeners.close();
        for (final Map.Entry<GlobalCheckpointListeners.GlobalCheckpointListener, Long> listener : listeners.entrySet()) {
            if (listener.getValue() > nextGlobalCheckpoint) {
                // these listeners should have been notified that we closed, and all the other listeners should not be
                assertThat(notifiedListeners.get(listener.getKey())).isEqualTo(UNASSIGNED_SEQ_NO);
            } else {
                assertThat(notifiedListeners.get(listener.getKey())).isNull();
            }
        }
    }

    @Test
    public void testListenersReadyToBeNotified() throws IOException {
        final GlobalCheckpointListeners globalCheckpointListeners =
                new GlobalCheckpointListeners(shardId, scheduler, logger);
        final long globalCheckpoint = randomLongBetween(0, Long.MAX_VALUE);
        globalCheckpointListeners.globalCheckpointUpdated(globalCheckpoint);
        final int numberOfListeners = randomIntBetween(0, 16);
        final long[] globalCheckpoints = new long[numberOfListeners];
        for (int i = 0; i < numberOfListeners; i++) {
            final int index = i;
            globalCheckpointListeners.add(
                    randomLongBetween(0, globalCheckpoint),
                    maybeMultipleInvocationProtectingListener((g, _) -> globalCheckpoints[index] = g),
                    null);
            // the listener should be notified immediately
            assertThat(globalCheckpoints[index]).isEqualTo(globalCheckpoint);
        }

        // test the listeners are not invoked twice
        final long nextGlobalCheckpoint = randomLongBetween(globalCheckpoint + 1, Long.MAX_VALUE);
        globalCheckpointListeners.globalCheckpointUpdated(nextGlobalCheckpoint);
        for (int i = 0; i < numberOfListeners; i++) {
            assertThat(globalCheckpoints[i]).isEqualTo(globalCheckpoint);
        }

        // closing should also not notify the listeners
        globalCheckpointListeners.close();
        for (int i = 0; i < numberOfListeners; i++) {
            assertThat(globalCheckpoints[i]).isEqualTo(globalCheckpoint);
        }
    }

    @Test
    public void testFailingListenerReadyToBeNotified() {
        final Logger mockLogger = mock(Logger.class);
        final GlobalCheckpointListeners globalCheckpointListeners =
                new GlobalCheckpointListeners(shardId, scheduler, mockLogger);
        final long globalCheckpoint = randomLongBetween(NO_OPS_PERFORMED + 1, Long.MAX_VALUE);
        globalCheckpointListeners.globalCheckpointUpdated(globalCheckpoint);
        final int numberOfListeners = randomIntBetween(0, 16);
        final long[] globalCheckpoints = new long[numberOfListeners];
        for (int i = 0; i < numberOfListeners; i++) {
            final int index = i;
            final boolean failure = randomBoolean();
            globalCheckpointListeners.add(
                    randomLongBetween(NO_OPS_PERFORMED, globalCheckpoint - 1),
                    maybeMultipleInvocationProtectingListener((_, _) -> {
                        if (failure) {
                            globalCheckpoints[index] = Long.MIN_VALUE;
                            throw new RuntimeException("failure");
                        } else {
                            globalCheckpoints[index] = globalCheckpoint;
                        }
                    }),
                    null);
            // the listener should be notified immediately
            if (failure) {
                assertThat(globalCheckpoints[i]).isEqualTo(Long.MIN_VALUE);
                final ArgumentCaptor<ParameterizedMessage> message = ArgumentCaptor.forClass(ParameterizedMessage.class);
                final ArgumentCaptor<RuntimeException> t = ArgumentCaptor.forClass(RuntimeException.class);
                verify(mockLogger).warn(message.capture(), t.capture());
                reset(mockLogger);
                assertThat(
                        message.getValue().getFormat()).isEqualTo("error notifying global checkpoint listener of updated global checkpoint [{}]");
                assertThat(message.getValue().getParameters()).isNotNull();
                assertThat(message.getValue().getParameters().length).isEqualTo(1);
                assertThat(message.getValue().getParameters()[0]).isEqualTo(globalCheckpoint);
                assertThat(t.getValue()).isNotNull();
                assertThat(t.getValue().getMessage()).isEqualTo("failure");
            } else {
                assertThat(globalCheckpoints[i]).isEqualTo(globalCheckpoint);
            }
        }
    }

    @Test
    public void testClose() throws IOException {
        final GlobalCheckpointListeners globalCheckpointListeners =
                new GlobalCheckpointListeners(shardId, scheduler, logger);
        globalCheckpointListeners.globalCheckpointUpdated(NO_OPS_PERFORMED);
        final int numberOfListeners = randomIntBetween(0, 16);
        final Exception[] exceptions = new Exception[numberOfListeners];
        for (int i = 0; i < numberOfListeners; i++) {
            final int index = i;
            globalCheckpointListeners.add(
                    0, maybeMultipleInvocationProtectingListener((_, e) -> exceptions[index] = e), null);
        }
        globalCheckpointListeners.close();
        for (int i = 0; i < numberOfListeners; i++) {
            assertThat(exceptions[i]).isNotNull();
            assertThat(exceptions[i]).isExactlyInstanceOf(IndexShardClosedException.class);
            assertThat(((IndexShardClosedException)exceptions[i]).getShardId()).isEqualTo(shardId);
        }

        // test the listeners are not invoked twice
        Arrays.fill(exceptions, null);
        globalCheckpointListeners.close();
        for (int i = 0; i < numberOfListeners; i++) {
            assertThat(exceptions[i]).isNull();
        }
    }

    @Test
    public void testAddAfterClose() throws InterruptedException, IOException {
        final GlobalCheckpointListeners globalCheckpointListeners =
                new GlobalCheckpointListeners(shardId, scheduler, logger);
        globalCheckpointListeners.globalCheckpointUpdated(NO_OPS_PERFORMED);
        globalCheckpointListeners.close();
        final AtomicBoolean invoked = new AtomicBoolean();
        final CountDownLatch latch = new CountDownLatch(1);
        globalCheckpointListeners.add(
                randomLongBetween(NO_OPS_PERFORMED, Long.MAX_VALUE),
                maybeMultipleInvocationProtectingListener((_, _) -> {
                    invoked.set(true);
                    latch.countDown();
                }),
                null);
        latch.await();
        assertThat(invoked.get()).isTrue();
    }

    @Test
    public void testFailingListenerOnUpdate() {
        final Logger mockLogger = mock(Logger.class);
        final GlobalCheckpointListeners globalCheckpointListeners =
                new GlobalCheckpointListeners(shardId, scheduler, mockLogger);
        globalCheckpointListeners.globalCheckpointUpdated(NO_OPS_PERFORMED);
        final int numberOfListeners = randomIntBetween(0, 16);
        final boolean[] failures = new boolean[numberOfListeners];
        final long[] globalCheckpoints = new long[numberOfListeners];
        for (int i = 0; i < numberOfListeners; i++) {
            final int index = i;
            final boolean failure = randomBoolean();
            failures[index] = failure;
            globalCheckpointListeners.add(
                    0,
                    maybeMultipleInvocationProtectingListener((g, _) -> {
                        if (failure) {
                            globalCheckpoints[index] = Long.MIN_VALUE;
                            throw new RuntimeException("failure");
                        } else {
                            globalCheckpoints[index] = g;
                        }
                    }),
                    null);
        }
        final long globalCheckpoint = randomLongBetween(NO_OPS_PERFORMED, Long.MAX_VALUE);
        globalCheckpointListeners.globalCheckpointUpdated(globalCheckpoint);
        for (int i = 0; i < numberOfListeners; i++) {
            if (failures[i]) {
                assertThat(globalCheckpoints[i]).isEqualTo(Long.MIN_VALUE);
            } else {
                assertThat(globalCheckpoints[i]).isEqualTo(globalCheckpoint);
            }
        }
        int failureCount = 0;
        for (int i = 0; i < numberOfListeners; i++) {
            if (failures[i]) {
                failureCount++;
            }
        }
        if (failureCount > 0) {
            final ArgumentCaptor<ParameterizedMessage> message = ArgumentCaptor.forClass(ParameterizedMessage.class);
            final ArgumentCaptor<RuntimeException> t = ArgumentCaptor.forClass(RuntimeException.class);
            verify(mockLogger, times(failureCount)).warn(message.capture(), t.capture());
            assertThat(
                    message.getValue().getFormat()).isEqualTo("error notifying global checkpoint listener of updated global checkpoint [{}]");
            assertThat(message.getValue().getParameters()).isNotNull();
            assertThat(message.getValue().getParameters().length).isEqualTo(1);
            assertThat(message.getValue().getParameters()[0]).isEqualTo(globalCheckpoint);
            assertThat(t.getValue()).isNotNull();
            assertThat(t.getValue().getMessage()).isEqualTo("failure");
        }
    }

    @Test
    public void testFailingListenerOnClose() throws IOException {
        final Logger mockLogger = mock(Logger.class);
        final GlobalCheckpointListeners globalCheckpointListeners =
                new GlobalCheckpointListeners(shardId, scheduler, mockLogger);
        globalCheckpointListeners.globalCheckpointUpdated(NO_OPS_PERFORMED);
        final int numberOfListeners = randomIntBetween(0, 16);
        final boolean[] failures = new boolean[numberOfListeners];
        final Exception[] exceptions = new Exception[numberOfListeners];
        for (int i = 0; i < numberOfListeners; i++) {
            final int index = i;
            final boolean failure = randomBoolean();
            failures[index] = failure;
            globalCheckpointListeners.add(
                    0,
                    maybeMultipleInvocationProtectingListener((_, e) -> {
                        if (failure) {
                            throw new RuntimeException("failure");
                        } else {
                            exceptions[index] = e;
                        }
                    }),
                    null);
        }
        globalCheckpointListeners.close();
        for (int i = 0; i < numberOfListeners; i++) {
            if (failures[i]) {
                assertThat(exceptions[i]).isNull();
            } else {
                assertThat(exceptions[i]).isNotNull();
                assertThat(exceptions[i]).isExactlyInstanceOf(IndexShardClosedException.class);
                assertThat(((IndexShardClosedException)exceptions[i]).getShardId()).isEqualTo(shardId);
            }
        }
        int failureCount = 0;
        for (int i = 0; i < numberOfListeners; i++) {
            if (failures[i]) {
                failureCount++;
            }
        }
        if (failureCount > 0) {
            final ArgumentCaptor<String> message = ArgumentCaptor.forClass(String.class);
            final ArgumentCaptor<RuntimeException> t = ArgumentCaptor.forClass(RuntimeException.class);
            verify(mockLogger, times(failureCount)).warn(message.capture(), t.capture());
            assertThat(message.getValue()).isEqualTo("error notifying global checkpoint listener of closed shard");
            assertThat(t.getValue()).isNotNull();
            assertThat(t.getValue().getMessage()).isEqualTo("failure");
        }
    }

    @Test
    public void testNotificationUsesExecutor() {
        final AtomicInteger count = new AtomicInteger();
        final Executor executor = command -> {
            count.incrementAndGet();
            command.run();
        };
        final GlobalCheckpointListeners globalCheckpointListeners = new GlobalCheckpointListeners(shardId, scheduler, logger);
        globalCheckpointListeners.globalCheckpointUpdated(NO_OPS_PERFORMED);
        final long globalCheckpoint = randomLongBetween(NO_OPS_PERFORMED, Long.MAX_VALUE);
        final AtomicInteger notified = new AtomicInteger();
        final int numberOfListeners = randomIntBetween(0, 16);
        for (int i = 0; i < numberOfListeners; i++) {
            globalCheckpointListeners.add(
                0,
                maybeMultipleInvocationProtectingListener(
                    new TestGlobalCheckpointListener() {

                        @Override
                        public Executor executor() {
                            return executor;
                        }

                        @Override
                        public void accept(final long g, final Exception e) {
                            notified.incrementAndGet();
                            assertThat(g).isEqualTo(globalCheckpoint);
                            assertThat(e).isNull();
                        }

                    }),
                null);
        }
        globalCheckpointListeners.globalCheckpointUpdated(globalCheckpoint);
        assertThat(notified.get()).isEqualTo(numberOfListeners);
        assertThat(count.get()).isEqualTo(numberOfListeners);
    }

    @Test
    public void testNotificationOnClosedUsesExecutor() throws IOException {
        final AtomicInteger count = new AtomicInteger();
        final Executor executor = command -> {
            count.incrementAndGet();
            command.run();
        };
        final GlobalCheckpointListeners globalCheckpointListeners = new GlobalCheckpointListeners(shardId, scheduler, logger);
        globalCheckpointListeners.close();
        final AtomicInteger notified = new AtomicInteger();
        final int numberOfListeners = randomIntBetween(0, 16);
        for (int i = 0; i < numberOfListeners; i++) {
            globalCheckpointListeners.add(
                NO_OPS_PERFORMED,
                maybeMultipleInvocationProtectingListener(
                    new TestGlobalCheckpointListener() {

                        @Override
                        public Executor executor() {
                            return executor;
                        }

                        @Override
                        public void accept(final long g, final Exception e) {
                            notified.incrementAndGet();
                            assertThat(g).isEqualTo(UNASSIGNED_SEQ_NO);
                            assertThat(e).isNotNull();
                            assertThat(e).isExactlyInstanceOf(IndexShardClosedException.class);
                            assertThat(((IndexShardClosedException) e).getShardId()).isEqualTo(shardId);
                        }

                    }),
                null);
        }
        assertThat(notified.get()).isEqualTo(numberOfListeners);
        assertThat(count.get()).isEqualTo(numberOfListeners);
    }

    @Test
    public void testListenersReadyToBeNotifiedUsesExecutor() {
        final AtomicInteger count = new AtomicInteger();
        final Executor executor = command -> {
            count.incrementAndGet();
            command.run();
        };
        final GlobalCheckpointListeners globalCheckpointListeners = new GlobalCheckpointListeners(shardId, scheduler, logger);
        final long globalCheckpoint = randomNonNegativeLong();
        globalCheckpointListeners.globalCheckpointUpdated(globalCheckpoint);
        final AtomicInteger notified = new AtomicInteger();
        final int numberOfListeners = randomIntBetween(0, 16);
        for (int i = 0; i < numberOfListeners; i++) {
            globalCheckpointListeners.add(
                randomLongBetween(0, globalCheckpoint),
                maybeMultipleInvocationProtectingListener(
                    new TestGlobalCheckpointListener() {

                        @Override
                        public Executor executor() {
                            return executor;
                        }

                        @Override
                        public void accept(final long g, final Exception e) {
                            notified.incrementAndGet();
                            assertThat(g).isEqualTo(globalCheckpoint);
                            assertThat(e).isNull();
                        }
                    }),
                null);
        }
        assertThat(notified.get()).isEqualTo(numberOfListeners);
        assertThat(count.get()).isEqualTo(numberOfListeners);
    }

    @Test
    public void testConcurrency() throws Exception {
        final ExecutorService executor = Executors.newFixedThreadPool(randomIntBetween(1, 8));
        final GlobalCheckpointListeners globalCheckpointListeners = new GlobalCheckpointListeners(shardId, scheduler, logger);
        final AtomicLong globalCheckpoint = new AtomicLong(NO_OPS_PERFORMED);
        globalCheckpointListeners.globalCheckpointUpdated(globalCheckpoint.get());
        // we are going to synchronize the actions of three threads: the updating thread, the listener thread, and the main test thread
        final CyclicBarrier barrier = new CyclicBarrier(3);
        final int numberOfIterations = randomIntBetween(1, 4096);
        final AtomicBoolean closed = new AtomicBoolean();
        final Thread updatingThread = new Thread(() -> {
            // synchronize starting with the listener thread and the main test thread
            awaitQuietly(barrier);
            for (int i = 0; i < numberOfIterations; i++) {
                if (i > numberOfIterations / 2 && rarely() && closed.get() == false) {
                    closed.set(true);
                    try {
                        globalCheckpointListeners.close();
                    } catch (final IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
                if (rarely() && closed.get() == false) {
                    globalCheckpointListeners.globalCheckpointUpdated(globalCheckpoint.incrementAndGet());
                }
            }
            // synchronize ending with the listener thread and the main test thread
            awaitQuietly(barrier);
        });

        final List<AtomicBoolean> invocations = new CopyOnWriteArrayList<>();
        final Thread listenersThread = new Thread(() -> {
            // synchronize starting with the updating thread and the main test thread
            awaitQuietly(barrier);
            for (int i = 0; i < numberOfIterations; i++) {
                final AtomicBoolean invocation = new AtomicBoolean();
                invocations.add(invocation);
                // sometimes this will notify the listener immediately
                globalCheckpointListeners.add(
                    globalCheckpoint.get(),
                    maybeMultipleInvocationProtectingListener(
                        new TestGlobalCheckpointListener() {

                            @Override
                            public Executor executor() {
                                return executor;
                            }

                            @Override
                            public void accept(final long g, final Exception e) {
                                if (invocation.compareAndSet(false, true) == false) {
                                    throw new IllegalStateException("listener invoked twice");
                                }
                            }

                        }),
                    randomBoolean() ? null : TimeValue.timeValueNanos(randomLongBetween(1, TimeUnit.MICROSECONDS.toNanos(1))));
            }
            // synchronize ending with the updating thread and the main test thread
            awaitQuietly(barrier);
        });
        updatingThread.start();
        listenersThread.start();
        // synchronize starting with the updating thread and the listener thread
        barrier.await();
        // synchronize ending with the updating thread and the listener thread
        barrier.await();
        // one last update to ensure all listeners are notified
        if (closed.get() == false) {
            globalCheckpointListeners.globalCheckpointUpdated(globalCheckpoint.incrementAndGet());
        }
        assertThat(globalCheckpointListeners.pendingListeners()).isEqualTo(0);
        // wait for all the listeners to be notified
        for (final AtomicBoolean invocation : invocations) {
            assertBusy(() -> assertThat(invocation.get()).isTrue());
        }
        // now shutdown
        executor.shutdown();
        assertThat(executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS)).isTrue();
        updatingThread.join();
        listenersThread.join();
    }

    @Test
    public void testTimeout() throws InterruptedException {
        final Logger mockLogger = mock(Logger.class);
        final GlobalCheckpointListeners globalCheckpointListeners =
                new GlobalCheckpointListeners(shardId, scheduler, mockLogger);
        final TimeValue timeout = TimeValue.timeValueMillis(randomIntBetween(1, 50));
        final AtomicBoolean notified = new AtomicBoolean();
        final CountDownLatch latch = new CountDownLatch(1);
        globalCheckpointListeners.add(
                NO_OPS_PERFORMED,
                maybeMultipleInvocationProtectingListener((g, e) -> {
                    try {
                        notified.set(true);
                        assertThat(g).isEqualTo(UNASSIGNED_SEQ_NO);
                        assertThat(e).isExactlyInstanceOf(TimeoutException.class);
                        assertThat(e).hasMessageContaining(timeout.getStringRep());
                        final ArgumentCaptor<String> message = ArgumentCaptor.forClass(String.class);
                        final ArgumentCaptor<TimeoutException> t = ArgumentCaptor.forClass(TimeoutException.class);
                        verify(mockLogger).trace(message.capture(), t.capture());
                        assertThat(message.getValue()).isEqualTo("global checkpoint listener timed out");
                        assertThat(t.getValue()).hasMessageContaining(timeout.getStringRep());
                    } catch (Exception _) {
                        fail(e.getMessage());
                    } finally {
                        latch.countDown();
                    }
                }),
                timeout);
        latch.await();

        assertThat(notified.get()).isTrue();
    }

    @Test
    public void testTimeoutNotificationUsesExecutor() throws InterruptedException {
        final AtomicInteger count = new AtomicInteger();
        final Executor executor = command -> {
            count.incrementAndGet();
            command.run();
        };
        final GlobalCheckpointListeners globalCheckpointListeners = new GlobalCheckpointListeners(shardId, scheduler, logger);
        final TimeValue timeout = TimeValue.timeValueMillis(randomIntBetween(1, 50));
        final AtomicBoolean notified = new AtomicBoolean();
        final CountDownLatch latch = new CountDownLatch(1);
        globalCheckpointListeners.add(
            NO_OPS_PERFORMED,
            maybeMultipleInvocationProtectingListener(
                new TestGlobalCheckpointListener() {

                    @Override
                    public Executor executor() {
                        return executor;
                    }

                    @Override
                    public void accept(final long g, final Exception e) {
                        try {
                            notified.set(true);
                            assertThat(g).isEqualTo(UNASSIGNED_SEQ_NO);
                            assertThat(e).isExactlyInstanceOf(TimeoutException.class);
                        } finally {
                            latch.countDown();
                        }
                    }

                }),
            timeout);
        latch.await();
        // ensure the listener notification occurred on the executor
        assertThat(notified.get()).isTrue();
        assertThat(count.get()).isEqualTo(1);
    }

    @Test
    public void testFailingListenerAfterTimeout() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final Logger mockLogger = mock(Logger.class);
        doAnswer(_ -> {
            latch.countDown();
            return null;
        }).when(mockLogger).warn(
            ArgumentMatchers.anyString(),
            ArgumentMatchers.any(Throwable.class)
        );
        final GlobalCheckpointListeners globalCheckpointListeners =
                new GlobalCheckpointListeners(shardId, scheduler, mockLogger);
        final TimeValue timeout = TimeValue.timeValueMillis(randomIntBetween(1, 50));
        globalCheckpointListeners.add(
                NO_OPS_PERFORMED,
                maybeMultipleInvocationProtectingListener((_, _) -> {
                    throw new RuntimeException("failure");
                }),
                timeout);
        latch.await();
        final ArgumentCaptor<String> message = ArgumentCaptor.forClass(String.class);
        final ArgumentCaptor<RuntimeException> t = ArgumentCaptor.forClass(RuntimeException.class);
        verify(mockLogger).warn(message.capture(), t.capture());
        assertThat(message.getValue()).isEqualTo("error notifying global checkpoint listener of timeout");
        assertThat(t.getValue()).isNotNull();
        assertThat(t.getValue()).isExactlyInstanceOf(RuntimeException.class);
        assertThat(t.getValue().getMessage()).isEqualTo("failure");
    }

    @Test
    public void testTimeoutCancelledAfterListenerNotified() {
        final GlobalCheckpointListeners globalCheckpointListeners =
                new GlobalCheckpointListeners(shardId, scheduler, logger);
        final TimeValue timeout = TimeValue.timeValueNanos(Long.MAX_VALUE);
        final GlobalCheckpointListeners.GlobalCheckpointListener globalCheckpointListener =
                maybeMultipleInvocationProtectingListener((g, e) -> {
                    assertThat(g).isEqualTo(NO_OPS_PERFORMED);
                    assertThat(e).isNull();
                });
        globalCheckpointListeners.add(NO_OPS_PERFORMED, globalCheckpointListener, timeout);
        final ScheduledFuture<?> future = globalCheckpointListeners.getTimeoutFuture(globalCheckpointListener);
        assertThat(future != null).isTrue();
        globalCheckpointListeners.globalCheckpointUpdated(NO_OPS_PERFORMED);
        assertThat(future.isCancelled()).isTrue();
    }

    private GlobalCheckpointListeners.GlobalCheckpointListener maybeMultipleInvocationProtectingListener(
            final TestGlobalCheckpointListener globalCheckpointListener) {
        if (Assertions.ENABLED) {
            final AtomicBoolean invoked = new AtomicBoolean();
            return new GlobalCheckpointListeners.GlobalCheckpointListener() {

                @Override
                public Executor executor() {
                    return globalCheckpointListener.executor();
                }

                @Override
                public void accept(final long g, final Exception e) {
                    if (invoked.compareAndSet(false, true) == false) {
                        throw new AssertionError("listener invoked twice");
                    }
                    globalCheckpointListener.accept(g, e);
                }

            };
        } else {
            return globalCheckpointListener;
        }
    }

    private void awaitQuietly(final CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (final BrokenBarrierException | InterruptedException e) {
            throw new AssertionError(e);
        }
    }

}
