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
import io.crate.operation.projectors.RowReceiver;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class PositionalRowMergerTest extends CrateUnitTest {


    @Test
    public void testConcurrentSetNextRow() throws Exception {
        int numUpstreams = 3;

        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        final PositionalRowMerger rowMerger = new PositionalRowMerger(rowReceiver, 1);

        final List<List<Object[]>> rowsPerUpstream = new ArrayList<>(numUpstreams);
        rowsPerUpstream.add(ImmutableList.of(new Object[]{0}, new Object[]{2}, new Object[]{6}));
        rowsPerUpstream.add(ImmutableList.of(new Object[]{1}, new Object[]{4}, new Object[]{7}));
        rowsPerUpstream.add(ImmutableList.of(new Object[]{3}, new Object[]{5}, new Object[]{8}, new Object[]{9}));

        final List<Throwable> setNextRowExceptions = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(numUpstreams);
        final ExecutorService executorService = Executors.newScheduledThreadPool(numUpstreams);

        final List<RowReceiver> downstreamHandles = new ArrayList<>(numUpstreams);
        // register upstreams
        for (int i = 0; i < numUpstreams; i++) {
            downstreamHandles.add(rowMerger.newRowReceiver());
        }
        for (int i = 0; i < numUpstreams; i++) {
            final int upstreamId = i;
            final RowReceiver upstreamBuffer = downstreamHandles.get(i);
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    List<Object[]> rows = rowsPerUpstream.get(upstreamId);
                    for (Object[] row : rows) {
                        try {
                            upstreamBuffer.setNextRow(new PositionalRowDelegate(new RowN(row), (int)row[0]));
                        } catch (Exception e) {
                            setNextRowExceptions.add(e);
                        }
                    }
                    upstreamBuffer.finish();
                    latch.countDown();
                }
            });
        }
        latch.await();
        executorService.shutdown();

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
    public void testOneUpstreamFail() throws Exception {
        final int numUpstreams = 3;

        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        final PositionalRowMerger rowMerger = new PositionalRowMerger(rowReceiver, 1);

        final List<List<Object[]>> rowsPerUpstream = new ArrayList<>(numUpstreams);
        rowsPerUpstream.add(ImmutableList.of(new Object[]{0}, new Object[]{2}));
        rowsPerUpstream.add(ImmutableList.of(new Object[]{1}));
        rowsPerUpstream.add(ImmutableList.<Object[]>of());

        final List<Throwable> setNextRowExceptions = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(numUpstreams);
        final ExecutorService executorService = Executors.newScheduledThreadPool(numUpstreams);

        final List<RowReceiver> downstreamHandles = new ArrayList<>(numUpstreams);
        // register upstreams
        for (int i = 0; i < numUpstreams; i++) {
            downstreamHandles.add(rowMerger.newRowReceiver());
        }
        for (int i = 0; i < numUpstreams; i++) {
            final int upstreamId = i;
            final RowReceiver upstreamBuffer = downstreamHandles.get(i);
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    if (upstreamId == numUpstreams-1) {
                        // last upstream will fail
                        upstreamBuffer.fail(new Throwable(String.format("[%d] I'm failing", upstreamId)));
                    } else {
                        List<Object[]> rows = rowsPerUpstream.get(upstreamId);
                        for (Object[] row : rows) {
                            try {
                                upstreamBuffer.setNextRow(new PositionalRowDelegate(new RowN(row), (int) row[0]));
                            } catch (Exception e) {
                                setNextRowExceptions.add(e);
                            }
                        }
                    }
                    latch.countDown();
                }
            });
        }
        latch.await();
        executorService.shutdown();

        assertThat(setNextRowExceptions, empty());

        expectedException.expect(Throwable.class);
        expectedException.expectMessage(String.format("[%d] I'm failing", numUpstreams-1));
        rowReceiver.result();

        executorService.awaitTermination(1, TimeUnit.SECONDS);
    }

}
