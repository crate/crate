/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.projectors;

import com.google.common.collect.ImmutableList;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class SortingRowMergerTest extends CrateUnitTest {

    private Row spare(Object... cells) {
        if (cells == null) {
            cells = new Object[]{null};
        }
        return new RowN(cells);
    }

    @Test
    public void testSortMerge() throws Exception {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        SortingRowMerger rowMerger = new SortingRowMerger(
                rowReceiver,
                new int[]{0},
                new boolean[]{false},
                new Boolean[]{null}
        );

        RowReceiver handle1 = rowMerger.newRowReceiver();
        RowReceiver handle2 = rowMerger.newRowReceiver();
        RowReceiver handle3 = rowMerger.newRowReceiver();

        handle1.setNextRow(spare(1));
        handle1.setNextRow(spare(3));
        handle1.setNextRow(spare(4));
        /**
         *      Handle 1        Handle 2        Handle 3
         *      1
         *      3
         *      4
         */
        assertThat(rowReceiver.rows.size(), is(0));

        handle2.setNextRow(spare(2));
        handle2.setNextRow(spare(3));

        /**
         *      Handle 1        Handle 2        Handle 3
         *      1               2
         *      3               3
         *      4
         */

        handle3.setNextRow(spare(1));
        handle3.setNextRow(spare(3));
        /**
         *      Handle 1        Handle 2        Handle 3
         *      1               2               1
         *      3               3               3
         *      4
         *      {1,2,3} are emitted now
         */

        assertThat(rowReceiver.rows.size(), is(6));
        assertThat((Integer)rowReceiver.rows.get(0)[0], is(1));
        assertThat((Integer)rowReceiver.rows.get(1)[0], is(1));
        assertThat((Integer)rowReceiver.rows.get(2)[0], is(2));
        assertThat((Integer)rowReceiver.rows.get(3)[0], is(3));
        assertThat((Integer)rowReceiver.rows.get(4)[0], is(3));
        assertThat((Integer)rowReceiver.rows.get(5)[0], is(3));

        handle3.setNextRow(spare(3));
        /**
         *      Handle 1        Handle 2        Handle 3
         *      4                               3
         *
         *      3 is emitted immediately
         */
        assertThat(rowReceiver.rows.size(), is(7));
        assertThat((Integer)rowReceiver.rows.get(6)[0], is(3));

        handle3.setNextRow(spare(4));
        handle3.finish(); // finish upstream, with non empty handle
        /**
         *      Handle 1        Handle 2        Handle 3 (finished)
         *      4                               4
         *
         */

        handle2.setNextRow(spare(5));
        /**
         *      Handle 1        Handle 2        Handle 3 (finished)
         *      4               5               4
         *
         *      4 is emitted
         */
        assertThat(rowReceiver.rows.size(), is(9));
        assertThat((Integer)rowReceiver.rows.get(7)[0], is(4));
        assertThat((Integer)rowReceiver.rows.get(8)[0], is(4));


        handle1.finish();
        /**
         *      Handle 1 (finished)        Handle 2        Handle 3 (finished)
         *                                 5
         *
         *      5 is emitted
         */
        assertThat(rowReceiver.rows.size(), is(10));
        assertThat((Integer)rowReceiver.rows.get(9)[0], is(5));
    }

    @Test
    /**
     * If an empty upstream is closed it may be possible to
     * emit on other upstreams.
     */
    public void finishEmptyUpstream() {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        SortingRowMerger projector = new SortingRowMerger(
                rowReceiver,
                new int[]{0},
                new boolean[]{false},
                new Boolean[]{null}
        );
        RowReceiver handle1 = projector.newRowReceiver();
        RowReceiver handle2 = projector.newRowReceiver();

        handle1.setNextRow(spare(1));
        handle2.setNextRow(spare(1));
        assertThat(rowReceiver.rows.size(), is(2));
        assertThat((Integer)rowReceiver.rows.get(0)[0], is(1));
        assertThat((Integer)rowReceiver.rows.get(1)[0], is(1));

        handle2.setNextRow(spare(5));
        /**
         *      Handle 1              Handle 2
         *                            5
         */
        assertThat(rowReceiver.rows.size(), is(2));

        handle1.finish();
        /**
         *   Handle 1  (finish)    Handle 2
         *                         5
         *  5 is emitted
         */
        assertThat(rowReceiver.rows.size(), is(3));
        assertThat((Integer)rowReceiver.rows.get(2)[0], is(5));

    }

    @Test
    public void finishUpstreamWithUnemittedRows() {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        SortingRowMerger projector = new SortingRowMerger(
                rowReceiver,
                new int[]{0},
                new boolean[]{false},
                new Boolean[]{null}
        );
        RowReceiver handle1 = projector.newRowReceiver();
        RowReceiver handle2 = projector.newRowReceiver();
        RowReceiver handle3 = projector.newRowReceiver();

        handle1.setNextRow(spare(4));
        handle3.setNextRow(spare(4));
        handle3.finish();
        handle1.finish();
        /**
         *      Handle 1 (finished)      Handle 2        Handle 3 (finished)
         *      4                               4
         *
         */

        handle2.setNextRow(spare(5));
        /**
         *      Handle 1 (finished)        Handle 2        Handle 3 (finished)
         *      4                          5               4
         *
         *      // everything is emitted
         */
        assertThat(rowReceiver.rows.size(), is(3));
        assertThat((Integer)rowReceiver.rows.get(0)[0], is(4));
        assertThat((Integer)rowReceiver.rows.get(1)[0], is(4));
        assertThat((Integer)rowReceiver.rows.get(2)[0], is(5));
    }

    @Test
    public void testConcurrentSortMerge() throws Exception {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        SortingRowMerger rowMerger = new SortingRowMerger(
                rowReceiver,
                new int[]{0},
                new boolean[]{false},
                new Boolean[]{null}
        );
        int numUpstreams = 3;
        final List<List<Integer>> valuesPerUpstream = new ArrayList<>(numUpstreams);
        valuesPerUpstream.add(ImmutableList.of(1, 3, 4, 6));
        valuesPerUpstream.add(ImmutableList.of(2, 3, 5));
        valuesPerUpstream.add(ImmutableList.of(1, 3, 3, 4));

        final List<Throwable> setNextRowExceptions = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(numUpstreams);
        final ExecutorService executorService = Executors.newScheduledThreadPool(numUpstreams);

        // register upstreams
        List<RowReceiver> downstreamHandles = new ArrayList<>(numUpstreams);
        for (int i = 0; i < numUpstreams; i++) {
            downstreamHandles.add(rowMerger.newRowReceiver());
        }

        for (int i = 0; i < numUpstreams; i++) {
            final int upstreamId = i;
            final RowReceiver downstreamHandle = downstreamHandles.get(i);
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    List<Integer> values = valuesPerUpstream.get(upstreamId);
                    for (Integer value: values) {
                        try {
                            downstreamHandle.setNextRow(spare(value));
                        } catch (Exception e) {
                            setNextRowExceptions.add(e);
                        }
                    }
                    downstreamHandle.finish();
                    latch.countDown();
                }
            });
        }
        latch.await();
        executorService.shutdown();
        assertThat(setNextRowExceptions, empty());

        assertThat(rowReceiver.rows.size(), is(11));
        assertThat((Integer)rowReceiver.rows.get(0)[0], is(1));
        assertThat((Integer)rowReceiver.rows.get(1)[0], is(1));
        assertThat((Integer)rowReceiver.rows.get(2)[0], is(2));
        assertThat((Integer)rowReceiver.rows.get(3)[0], is(3));
        assertThat((Integer)rowReceiver.rows.get(4)[0], is(3));
        assertThat((Integer)rowReceiver.rows.get(5)[0], is(3));
        assertThat((Integer)rowReceiver.rows.get(6)[0], is(3));
        assertThat((Integer)rowReceiver.rows.get(7)[0], is(4));
        assertThat((Integer)rowReceiver.rows.get(8)[0], is(4));
        assertThat((Integer)rowReceiver.rows.get(9)[0], is(5));
        assertThat((Integer)rowReceiver.rows.get(10)[0], is(6));

        executorService.awaitTermination(1, TimeUnit.SECONDS);
    }
}
