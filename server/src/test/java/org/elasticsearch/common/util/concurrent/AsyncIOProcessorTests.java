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
package org.elasticsearch.common.util.concurrent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.common.collections.Tuple;

public class AsyncIOProcessorTests extends ESTestCase {

    @Test
    public void testPut() throws InterruptedException {
        boolean blockInternal = randomBoolean();
        AtomicInteger received = new AtomicInteger(0);
        AsyncIOProcessor<Object> processor = new AsyncIOProcessor<Object>(logger, scaledRandomIntBetween(1, 2024)) {
            @Override
            protected void write(List<Tuple<Object, Consumer<Exception>>> candidates) throws IOException {
                if (blockInternal) {
                    synchronized (this) {
                        // TODO: check why we need a loop, can't we just use received.addAndGet(candidates.size())
                        for (int i = 0; i < candidates.size(); i++) {
                            received.incrementAndGet();
                        }
                    }
                } else {
                    received.addAndGet(candidates.size());
                }
            }
        };
        Semaphore semaphore = new Semaphore(Integer.MAX_VALUE);
        final int count = randomIntBetween(1000, 20000);
        Thread[] thread = new Thread[randomIntBetween(3, 10)];
        CountDownLatch latch = new CountDownLatch(thread.length);
        for (int i = 0; i < thread.length; i++) {
            thread[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        latch.countDown();
                        latch.await();
                        for (int i = 0; i < count; i++) {
                            semaphore.acquire();
                            processor.put(new Object(), (ex) -> semaphore.release());
                        }
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                }
            };
            thread[i].start();
        }

        for (int i = 0; i < thread.length; i++) {
            thread[i].join();
        }
        assertThat(semaphore.tryAcquire(Integer.MAX_VALUE, 10, TimeUnit.SECONDS)).isTrue();
        assertThat(received.get()).isEqualTo(count * thread.length);
    }

    @Test
    public void testRandomFail() throws InterruptedException {
        AtomicInteger received = new AtomicInteger(0);
        AtomicInteger failed = new AtomicInteger(0);
        AtomicInteger actualFailed = new AtomicInteger(0);
        AsyncIOProcessor<Object> processor = new AsyncIOProcessor<Object>(logger, scaledRandomIntBetween(1, 2024)) {
            @Override
            protected void write(List<Tuple<Object, Consumer<Exception>>> candidates) throws IOException {
                received.addAndGet(candidates.size());
                if (randomBoolean()) {
                    failed.addAndGet(candidates.size());
                    if (randomBoolean()) {
                        throw new IOException();
                    } else {
                        throw new RuntimeException();
                    }
                }
            }
        };
        Semaphore semaphore = new Semaphore(Integer.MAX_VALUE);
        final int count = randomIntBetween(1000, 20000);
        Thread[] thread = new Thread[randomIntBetween(3, 10)];
        CountDownLatch latch = new CountDownLatch(thread.length);
        for (int i = 0; i < thread.length; i++) {
            thread[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        latch.countDown();
                        latch.await();
                        for (int i = 0; i < count; i++) {
                            semaphore.acquire();
                            processor.put(new Object(), (ex) -> {
                                if (ex != null) {
                                    actualFailed.incrementAndGet();
                                }
                                semaphore.release();
                            });
                        }
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                }
            };
            thread[i].start();
        }

        for (int i = 0; i < thread.length; i++) {
            thread[i].join();
        }
        assertThat(semaphore.tryAcquire(Integer.MAX_VALUE, 10, TimeUnit.SECONDS)).isTrue();
        assertThat(received.get()).isEqualTo(count * thread.length);
        assertThat(failed.get()).isEqualTo(actualFailed.get());
    }

    @Test
    public void testConsumerCanThrowExceptions() {
        AtomicInteger received = new AtomicInteger(0);
        AtomicInteger notified = new AtomicInteger(0);

        AsyncIOProcessor<Object> processor = new AsyncIOProcessor<Object>(logger, scaledRandomIntBetween(1, 2024)) {
            @Override
            protected void write(List<Tuple<Object, Consumer<Exception>>> candidates) throws IOException {
                received.addAndGet(candidates.size());
            }
        };
        processor.put(new Object(), (e) -> {
            notified.incrementAndGet();
            throw new RuntimeException();
        });
        processor.put(new Object(), (e) -> {
            notified.incrementAndGet();
            throw new RuntimeException();
        });
        assertThat(notified.get()).isEqualTo(2);
        assertThat(received.get()).isEqualTo(2);
    }

    @Test
    public void testNullArguments() {
        AsyncIOProcessor<Object> processor = new AsyncIOProcessor<Object>(logger, scaledRandomIntBetween(1, 2024)) {
            @Override
            protected void write(List<Tuple<Object, Consumer<Exception>>> candidates) throws IOException {
            }
        };

        assertThatThrownBy(() -> processor.put(null, (e) -> {})).isExactlyInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> processor.put(new Object(), null)).isExactlyInstanceOf(NullPointerException.class);
    }


    @Test
    public void testSlowConsumer() {
        AtomicInteger received = new AtomicInteger(0);
        AtomicInteger notified = new AtomicInteger(0);

        AsyncIOProcessor<Object> processor = new AsyncIOProcessor<Object>(logger, scaledRandomIntBetween(1, 2024)) {
            @Override
            protected void write(List<Tuple<Object, Consumer<Exception>>> candidates) throws IOException {
                received.addAndGet(candidates.size());
            }
        };

        int threadCount = randomIntBetween(2, 10);
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        Semaphore serializePutSemaphore = new Semaphore(1);
        List<Thread> threads = IntStream.range(0, threadCount).mapToObj(i -> new Thread(getTestName() + "_" + i) {
            {
                setDaemon(true);
            }

            @Override
            public void run() {
                try {
                    assertThat(serializePutSemaphore.tryAcquire(10, TimeUnit.SECONDS)).isTrue();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                processor.put(new Object(), (e) -> {
                    serializePutSemaphore.release();
                    try {
                        barrier.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException | BrokenBarrierException | TimeoutException ex) {
                        throw new RuntimeException(ex);
                    }
                    notified.incrementAndGet();
                });
            }
        }).collect(Collectors.toList());
        threads.forEach(Thread::start);
        threads.forEach(t -> {
            try {
                t.join(20000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        assertThat(notified.get()).isEqualTo(threadCount);
        assertThat(received.get()).isEqualTo(threadCount);
        threads.forEach(t -> assertFalse(t.isAlive()));
    }
}
