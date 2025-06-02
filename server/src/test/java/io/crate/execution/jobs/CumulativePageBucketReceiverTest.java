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
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.Streamer;
import io.crate.data.ArrayBucket;
import io.crate.data.testing.TestingRowConsumer;
import io.crate.execution.engine.distribution.merge.PassThroughPagingIterator;
import io.crate.types.DataTypes;

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
}

