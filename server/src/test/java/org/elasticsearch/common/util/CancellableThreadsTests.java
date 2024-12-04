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
package org.elasticsearch.common.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.common.util.CancellableThreads.ExecutionCancelledException;
import org.elasticsearch.common.util.CancellableThreads.Interruptable;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class CancellableThreadsTests extends ESTestCase {

    public static class CustomException extends RuntimeException {
        CustomException(String msg) {
            super(msg);
        }
    }

    private static class ThrowOnCancelException extends RuntimeException {
    }

    private class TestPlan {
        public final int id;
        final boolean busySpin;
        final boolean exceptBeforeCancel;
        final boolean exitBeforeCancel;
        final boolean exceptAfterCancel;
        final boolean presetInterrupt;

        private TestPlan(int id) {
            this.id = id;
            this.busySpin = randomBoolean();
            this.exceptBeforeCancel = randomBoolean();
            this.exitBeforeCancel = randomBoolean();
            this.exceptAfterCancel = randomBoolean();
            this.presetInterrupt = randomBoolean();
        }
    }

    static class TestRunnable implements Interruptable {
        final TestPlan plan;
        final CountDownLatch readyForCancel;

        TestRunnable(TestPlan plan, CountDownLatch readyForCancel) {
            this.plan = plan;
            this.readyForCancel = readyForCancel;
        }

        @Override
        public void run() throws InterruptedException {
            assertThat(Thread.currentThread().isInterrupted()).as("interrupt thread should have been clear").isFalse();
            if (plan.exceptBeforeCancel) {
                throw new CustomException("thread [" + plan.id + "] pre-cancel exception");
            } else if (plan.exitBeforeCancel) {
                return;
            }
            readyForCancel.countDown();
            try {
                if (plan.busySpin) {
                    while (!Thread.currentThread().isInterrupted()) {
                    }
                } else {
                    Thread.sleep(50000);
                }
            } finally {
                if (plan.exceptAfterCancel) {
                    throw new CustomException("thread [" + plan.id + "] post-cancel exception");
                }
            }
        }
    }

    @Test
    public void testCancellableThreads() throws InterruptedException {
        Thread[] threads = new Thread[randomIntBetween(3, 10)];
        final TestPlan[] plans = new TestPlan[threads.length];
        final Exception[] exceptions = new Exception[threads.length];
        final boolean[] interrupted = new boolean[threads.length];
        final CancellableThreads cancellableThreads = new CancellableThreads();
        final CountDownLatch readyForCancel = new CountDownLatch(threads.length);
        for (int i = 0; i < threads.length; i++) {
            final TestPlan plan = new TestPlan(i);
            plans[i] = plan;
            threads[i] = new Thread(() -> {
                try {
                    if (plan.presetInterrupt) {
                        Thread.currentThread().interrupt();
                    }
                    cancellableThreads.execute(new TestRunnable(plan, readyForCancel));
                } catch (Exception e) {
                    exceptions[plan.id] = e;
                }
                if (plan.exceptBeforeCancel || plan.exitBeforeCancel) {
                    // we have to mark we're ready now (actually done).
                    readyForCancel.countDown();
                }
                interrupted[plan.id] = Thread.currentThread().isInterrupted();
            });
            threads[i].setDaemon(true);
            threads[i].start();
        }

        readyForCancel.await();
        final boolean throwInOnCancel = randomBoolean();
        final AtomicInteger invokeTimes = new AtomicInteger();
        cancellableThreads.setOnCancel((reason, beforeCancelException) -> {
            invokeTimes.getAndIncrement();
            if (throwInOnCancel) {
                ThrowOnCancelException e = new ThrowOnCancelException();
                if (beforeCancelException != null) {
                    e.addSuppressed(beforeCancelException);
                }
                throw e;
            }
        });

        cancellableThreads.cancel("test");
        for (Thread thread : threads) {
            thread.join(20000);
            assertThat(thread.isAlive()).isFalse();
        }
        for (int i = 0; i < threads.length; i++) {
            TestPlan plan = plans[i];
            final Class<?> exceptionClass = CustomException.class;
            if (plan.exceptBeforeCancel) {
                assertThat(exceptions[i]).isExactlyInstanceOf(exceptionClass);
            } else if (plan.exitBeforeCancel) {
                assertThat(exceptions[i]).isNull();
            } else {
                // in all other cases, we expect a cancellation exception.
                if (throwInOnCancel) {
                    assertThat(exceptions[i]).isExactlyInstanceOf(ThrowOnCancelException.class);
                } else {
                    assertThat(exceptions[i]).isExactlyInstanceOf(ExecutionCancelledException.class);
                }
                if (plan.exceptAfterCancel) {
                    assertThat(exceptions[i].getSuppressed()).satisfiesExactly(
                        e -> assertThat(e).isExactlyInstanceOf(exceptionClass));
                } else {
                    assertThat(exceptions[i].getSuppressed()).isEmpty();
                }
            }
            assertThat(interrupted[plan.id]).isEqualTo(plan.presetInterrupt);
        }
        assertThat(invokeTimes.longValue()).isEqualTo(
            Arrays.stream(plans)
                .filter(p -> p.exceptBeforeCancel == false && p.exitBeforeCancel == false)
                .count());
        if (throwInOnCancel) {
            assertThatThrownBy(cancellableThreads::checkForCancel)
                .isExactlyInstanceOf(ThrowOnCancelException.class);
        } else {
            assertThatThrownBy(cancellableThreads::checkForCancel)
                .isExactlyInstanceOf(ExecutionCancelledException.class);
        }
        assertThat(invokeTimes.longValue()).isEqualTo(
            Arrays.stream(plans)
                .filter(p -> p.exceptBeforeCancel == false && p.exitBeforeCancel == false)
                .count() + 1);
    }
}
