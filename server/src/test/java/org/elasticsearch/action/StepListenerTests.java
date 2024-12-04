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

package org.elasticsearch.action;


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class StepListenerTests extends ESTestCase {
    private ThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

    @Test
    public void testSimpleSteps() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Consumer<Exception> onFailure = e -> {
            latch.countDown();
            fail("test a happy path");
        };

        StepListener<String> step1 = new StepListener<>(); //[a]sync provide a string
        executeAction(() -> step1.onResponse("hello"));
        StepListener<Integer> step2 = new StepListener<>(); //[a]sync calculate the length of the string
        step1.whenComplete(str -> executeAction(() -> step2.onResponse(str.length())), onFailure);
        step2.whenComplete(length -> executeAction(latch::countDown), onFailure);
        latch.await();
        assertThat(step1.result()).isEqualTo("hello");
        assertThat(step2.result()).isEqualTo(5);
    }

    @Test
    public void testAbortOnFailure() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        int failedStep = randomBoolean() ? 1 : 2;
        AtomicInteger failureNotified = new AtomicInteger();
        Consumer<Exception> onFailure = e -> {
            failureNotified.getAndIncrement();
            latch.countDown();
            assertThat(e.getMessage()).isEqualTo("failed at step " + failedStep);
        };

        StepListener<String> step1 = new StepListener<>(); //[a]sync provide a string
        if (failedStep == 1) {
            executeAction(() -> step1.onFailure(new RuntimeException("failed at step 1")));
        } else {
            executeAction(() -> step1.onResponse("hello"));
        }

        StepListener<Integer> step2 = new StepListener<>(); //[a]sync calculate the length of the string
        step1.whenComplete(str -> {
            if (failedStep == 2) {
                executeAction(() -> step2.onFailure(new RuntimeException("failed at step 2")));
            } else {
                executeAction(() -> step2.onResponse(str.length()));
            }
        }, onFailure);

        step2.whenComplete(length -> latch.countDown(), onFailure);
        latch.await();
        assertThat(failureNotified.get()).isEqualTo(1);

        if (failedStep == 1) {
            assertThatThrownBy(step1::result)
                .isExactlyInstanceOf(RuntimeException.class)
                .hasMessage("failed at step 1");
            assertThatThrownBy(step2::result)
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessage("step is not completed yet");
        } else {
            assertThat(step1.result()).isEqualTo("hello");
            assertThatThrownBy(step2::result)
                .isExactlyInstanceOf(RuntimeException.class)
                .hasMessage("failed at step 2");
        }
    }

    private void executeAction(Runnable runnable) {
        if (randomBoolean()) {
            threadPool.generic().execute(runnable);
        } else {
            runnable.run();
        }
    }
}
