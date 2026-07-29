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

package io.crate.execution.jobs;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.annotations.Repeat;

import io.crate.Streamer;
import io.crate.data.ArrayBucket;
import io.crate.data.Bucket;
import io.crate.data.testing.TestingRowConsumer;
import io.crate.exceptions.JobKilledException;
import io.crate.execution.engine.distribution.merge.PassThroughPagingIterator;
import io.crate.types.DataTypes;

@Repeat(iterations = 50)
public class CumulativePageBucketReceiverTest extends ESTestCase {

    @Test
    public void test_page_bucket_is_always_completed() throws Exception {
        int numBuckets = randomInt(20) + 1;
        AtomicBoolean killed = new AtomicBoolean(false);

        TestingRowConsumer rowConsumer = new TestingRowConsumer();
        try (var executor = Executors.newSingleThreadExecutor()) {
            CumulativePageBucketReceiver bucketReceiver = new CumulativePageBucketReceiver(
                "n1",
                1,
                executor,
                new Streamer[] { DataTypes.INTEGER.streamer() },
                rowConsumer,
                PassThroughPagingIterator.oneShot(),
                numBuckets
            );
            bucketReceiver.consumeRows();
            assertThat(rowConsumer.completionFuture()).isNotDone();

            CyclicBarrier cyclicBarrier = new CyclicBarrier(numBuckets);
            ArrayList<Thread> threads = new ArrayList<>(numBuckets);
            for (int i = 0; i < numBuckets; i++) {
                final int bucketIdx = i;
                Thread thread = new Thread(() -> {
                    try {
                        cyclicBarrier.await();
                    } catch (InterruptedException | BrokenBarrierException e) {
                        return;
                    }

                    // Rarely to ensure the "no-failure" path is also tested sometimes
                    // With the number of buckets a "randomBoolean" would always cover only the testing path
                    if (rarely()) {
                        killed.set(true);
                        bucketReceiver.kill(new IllegalStateException("Alles kaputt"));
                        return;
                    }
                    var bucket = new ArrayBucket(new Object[][] {
                        new Object[] { 1 }
                    });
                    bucketReceiver.setBucket(bucketIdx, bucket, true, new PageResultListener() {

                        @Override
                        public void needMore(boolean needMore) {
                        }
                    });
                });
                thread.start();
                threads.add(thread);
            }

            for (var thread : threads) {
                thread.join();
            }

            if (killed.get()) {
                assertThat(rowConsumer.completionFuture()).failsWithin(2, TimeUnit.SECONDS);
            } else {
                assertThat(rowConsumer.completionFuture()).succeedsWithin(2, TimeUnit.SECONDS);
                assertThat(rowConsumer.getResult())
                    .as("Receives one row per bucket")
                    .hasSize(numBuckets);
            }
        }
    }

    @Test
    public void test_completes_immediately_with_no_upstreams() throws Exception {
        TestingRowConsumer rowConsumer = new TestingRowConsumer();
        try (var executor = Executors.newSingleThreadExecutor()) {
            CumulativePageBucketReceiver bucketReceiver = new CumulativePageBucketReceiver(
                "n1",
                1,
                executor,
                new Streamer[] { DataTypes.INTEGER.streamer() },
                rowConsumer,
                PassThroughPagingIterator.oneShot(),
                0
            );
            bucketReceiver.consumeRows();
            assertThat(rowConsumer.completionFuture()).isDone();
        }
    }

    /**
     * A pass-through merge must hand a bucket on and acknowledge its upstream without waiting for the
     * other upstreams of the same page.
     * <p>
     * Withholding the acknowledgement until every upstream of a page delivered can deadlock a chain of
     * distributed joins. An upstream that filled its own output page stops consuming its input until
     * all of its downstreams acknowledge
     * ({@code DistributingConsumer#countdownAndMaybeContinue} resumes only once every response
     * arrived). So if this receiver holds back the acknowledgement until a sibling upstream delivers,
     * and that sibling cannot produce because it is starved of input by the very upstream being held
     * back, no node can make progress. Nothing on that path times out, so the query hangs indefinitely
     * without consuming CPU.
     * <p>
     * Reintroducing the all-buckets barrier for an unordered merge makes this test time out.
     */
    @Test
    public void test_unordered_merge_acknowledges_upstream_without_waiting_for_the_others() throws Exception {
        TestingRowConsumer rowConsumer = new TestingRowConsumer();
        try (var executor = Executors.newSingleThreadExecutor()) {
            CumulativePageBucketReceiver bucketReceiver = new CumulativePageBucketReceiver(
                "n1",
                1,
                executor,
                new Streamer[] { DataTypes.INTEGER.streamer() },
                rowConsumer,
                PassThroughPagingIterator.oneShot(),
                2
            );
            bucketReceiver.consumeRows();

            CountDownLatch askedForMore = new CountDownLatch(1);
            PageResultListener firstUpstream = needMore -> {
                if (needMore) {
                    askedForMore.countDown();
                }
            };

            // Only upstream 0 delivers. Upstream 1 stays silent, standing in for a sibling instance
            // that cannot produce anything until upstream 0 has been acknowledged.
            bucketReceiver.setBucket(
                0,
                new ArrayBucket(new Object[][] { new Object[] { 1 } }),
                false,
                firstUpstream
            );

            assertThat(askedForMore.await(10, TimeUnit.SECONDS))
                .as("upstream 0 is asked for more data although upstream 1 delivered nothing")
                .isTrue();

            // Let both upstreams finish, to also cover that nothing got dropped along the way.
            bucketReceiver.setBucket(0, Bucket.EMPTY, true, needMore -> {});
            bucketReceiver.setBucket(
                1,
                new ArrayBucket(new Object[][] { new Object[] { 2 } }),
                true,
                needMore -> {}
            );

            assertThat(rowConsumer.completionFuture()).succeedsWithin(10, TimeUnit.SECONDS);
            assertThat(rowConsumer.getResult())
                .as("Receives the row of each upstream")
                .hasSize(2);
        }
    }

    @Test
    public void two_threads_running_2_kill_chains_dont_deadlock() throws Exception {
        try (var executor = Executors.newFixedThreadPool(2)) {
            CumulativePageBucketReceiver receiver1 = new CumulativePageBucketReceiver(
                "n1",
                1,
                executor,
                new Streamer[]{DataTypes.INTEGER.streamer()},
                new TestingRowConsumer(),
                PassThroughPagingIterator.oneShot(),
                1
            );
            CumulativePageBucketReceiver receiver2 = new CumulativePageBucketReceiver(
                "n1",
                2,
                executor,
                new Streamer[]{DataTypes.INTEGER.streamer()},
                new TestingRowConsumer(),
                PassThroughPagingIterator.oneShot(),
                1
            );

            // Imitating 2 concurrent kill chains.
            // RootTask.killTasks and RootTask.taskFinishedListener.onFailure.
            // Let's say we have the following tasks in orderedTasks:
            // rx1_rec1, rx_rec2.

            // rx1_rec1 gets killed in TaskService, forwardFailure handler kills rx1_rec1
            receiver1.completionFuture().whenComplete((r, t) -> {
                if (t != null) {
                    // rx_rec2 is picked up in the RootTask.taskFinishedListener.onFailure
                    // because other kill chain RootTasks.killTasks hasn't picked up rx_rec2 yet.
                    receiver2.kill(t);
                }
            });
            // RootTasks.killTasks goes to the next task in orderedTasks:rx_rec2
            // and grabs its lock right after another kill chain passed isDone check
            receiver2.completionFuture().whenComplete((r, t) -> {
                if (t != null) {
                    receiver1.kill(t);
                }
            });
            Thread t1 = new Thread(() -> receiver1.kill(JobKilledException.of("test kill")));
            Thread t2 = new Thread(() -> receiver2.kill(JobKilledException.of("test kill")));

            t1.start();
            t2.start();

            t1.join(1000);
            t2.join(1000);

            assertThat(t1.isAlive()).isFalse();
            assertThat(t2.isAlive()).isFalse();
        }
    }
}

