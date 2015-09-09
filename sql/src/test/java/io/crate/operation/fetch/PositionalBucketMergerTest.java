/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.fetch;

import com.google.common.collect.ImmutableList;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class PositionalBucketMergerTest extends CrateUnitTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testConcurrentSetNextBucket() throws Exception {
        /**
         * Propagate 2 buckets of the same upstream concurrent, every bucket is ordered.
         */
        int numUpstreams = 2;

        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        final PositionalBucketMerger bucketMerger = new PositionalBucketMerger(rowReceiver, numUpstreams, 1);

        final List<List<List<Object[]>>> bucketsPerUpstream = new ArrayList<>(numUpstreams);
        List<List<Object[]>> upstream1 = new ArrayList<>(2);
        upstream1.add(ImmutableList.of(new Object[]{4}, new Object[]{6}, new Object[]{7}));
        upstream1.add(ImmutableList.of(new Object[]{0}, new Object[]{1}, new Object[]{3}));
        bucketsPerUpstream.add(upstream1);

        List<List<Object[]>> upstream2 = new ArrayList<>(2);
        upstream2.add(ImmutableList.of(new Object[]{2}, new Object[]{5}));
        upstream2.add(ImmutableList.of(new Object[]{8}, new Object[]{9}));
        bucketsPerUpstream.add(upstream2);

        final List<Throwable> setNextRowExceptions = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(numUpstreams);
        final ExecutorService executorService = Executors.newScheduledThreadPool(numUpstreams);

        // register upstreams
        for (int i = 0; i < bucketsPerUpstream.size(); i++) {
            bucketMerger.registerUpstream(null);
        }

        for (int i = 0; i < bucketsPerUpstream.size(); i++) {
            final int upstreamId = i;

            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (final List<Object[]> bucket : bucketsPerUpstream.get(upstreamId)) {
                            List<Row> rows1 = new ArrayList<>();
                            for (Object[] row : bucket) {
                                rows1.add(new PositionalRowDelegate(new RowN(row), (int) row[0]));
                            }
                            bucketMerger.setNextBucket(rows1, upstreamId);
                        }
                    } catch (Throwable t) {
                        setNextRowExceptions.add(t);
                        bucketMerger.fail(t);
                    }
                    try {
                        bucketMerger.finish();
                    } catch (Throwable t) {
                        setNextRowExceptions.add(t);
                    }
                    latch.countDown();
                }
            });
        }
        latch.await(10, TimeUnit.SECONDS);
        executorService.shutdown();
        bucketMerger.finish();

        assertThat(setNextRowExceptions, empty());

        Bucket result = rowReceiver.result();
        assertThat(result.size(), is(10));
        Iterator<Row> it = result.iterator();
        for (int i = 0; i < 10; i++) {
            assertThat((int) it.next().get(0), is(i));
        }

        executorService.awaitTermination(1, TimeUnit.SECONDS);
    }

    @Test
    public void testOneUpstreamWillFail() throws Exception {
        final int numUpstreams = 2;

        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        final PositionalBucketMerger bucketMerger = new PositionalBucketMerger(rowReceiver, numUpstreams, 1);

        final List<List<List<Object[]>>> bucketsPerUpstream = new ArrayList<>(numUpstreams);
        List<List<Object[]>> upstream1 = new ArrayList<>(2);
        upstream1.add(ImmutableList.of(new Object[]{4}, new Object[]{6}, new Object[]{7}));
        upstream1.add(ImmutableList.of(new Object[]{0}, new Object[]{1}, new Object[]{3}));
        bucketsPerUpstream.add(upstream1);

        List<List<Object[]>> upstream2 = new ArrayList<>(2);
        upstream2.add(ImmutableList.of(new Object[]{2}, new Object[]{5}));
        upstream2.add(ImmutableList.of(new Object[]{8}, new Object[]{9}));
        bucketsPerUpstream.add(upstream2);

        final List<Throwable> setNextRowExceptions = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(numUpstreams);
        final ExecutorService executorService = Executors.newScheduledThreadPool(numUpstreams);
        // register upstreams
        for (int i = 0; i < bucketsPerUpstream.size(); i++) {
            bucketMerger.registerUpstream(null);
        }

        for (int i = 0; i < bucketsPerUpstream.size(); i++) {
            final int upstreamId = i;

            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    if (upstreamId == numUpstreams - 1) {
                        // last upstream will fail
                        bucketMerger.fail(new Throwable(String.format("[%d] I'm failing", upstreamId)));
                        latch.countDown();
                        return;
                    }
                    try {
                        for (final List<Object[]> bucket : bucketsPerUpstream.get(upstreamId)) {
                            List<Row> rows1 = new ArrayList<>();
                            for (Object[] row : bucket) {
                                rows1.add(new PositionalRowDelegate(new RowN(row), (int) row[0]));
                            }
                            bucketMerger.setNextBucket(rows1, upstreamId);
                        }
                    } catch (Throwable t) {
                        bucketMerger.fail(t);
                        setNextRowExceptions.add(t);
                    }
                    try {
                        bucketMerger.finish();
                    } catch (Throwable t) {
                        setNextRowExceptions.add(t);
                    }
                    latch.countDown();
                }
            });
        }
        latch.await(10, TimeUnit.SECONDS);
        executorService.shutdown();

        assertThat(setNextRowExceptions, empty());

        expectedException.expect(Throwable.class);
        expectedException.expectMessage(String.format("[%d] I'm failing", numUpstreams - 1));
        rowReceiver.result();
        executorService.awaitTermination(1, TimeUnit.SECONDS);
    }

}
