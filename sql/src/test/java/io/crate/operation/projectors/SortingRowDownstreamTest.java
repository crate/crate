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

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.jobs.ExecutionState;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowUpstream;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import io.crate.testing.TestingHelpers;
import org.apache.commons.lang3.RandomUtils;
import org.elasticsearch.common.collect.HppcMaps;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.hamcrest.Matchers;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class SortingRowDownstreamTest extends CrateUnitTest {

    private static Row spare(Object... cells) {
        if (cells == null) {
            cells = new Object[]{null};
        }
        return new RowN(cells);
    }

    private static class Upstream implements RowUpstream {

        private final ArrayList<Object[]> rows = new ArrayList<>();
        private final RowReceiver downstreamHandle;

        public Upstream(RowDownstream rowDownstream, Object[]... rows) {
            for (int i = 0; i < rows.length; i++) {
                this.rows.add(rows[i]);
            }

            downstreamHandle = rowDownstream.newRowReceiver();
            downstreamHandle.setUpstream(this);
        }


        @Override
        public void pause() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void resume(boolean threaded) {
            throw new UnsupportedOperationException();
        }

        private void doStart() {
            while (rows.size() > 0) {
                Object[] row = rows.remove(0);
                downstreamHandle.setNextRow(spare(row));
            }
            downstreamHandle.finish();
        }

        public void start() {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    doStart();
                }
            });
            thread.start();
        }

    }

    private static class SpareUpstream implements RowUpstream {

        private final int numRows;
        private final RowReceiver downstreamHandle;

        public SpareUpstream(RowDownstream rowDownstream, int numRows) {
            this.numRows = numRows;
            downstreamHandle = rowDownstream.newRowReceiver();
            downstreamHandle.setUpstream(this);
        }

        private void doStart() {
            int currentValue = RandomUtils.nextInt(0, 10);
            int sameValues = RandomUtils.nextInt(1, 5);
            int i = 0;
            Object[] cells = new Object[1];
            Row rowN = new RowN(cells);
            downstreamHandle.prepare(mock(ExecutionState.class));
            while (i < numRows) {
                for (int j = 0; j < sameValues; j++) {
                    cells[0] = currentValue;
                    downstreamHandle.setNextRow(rowN);
                    if (++i >= numRows) {
                        break;
                    }
                }
                currentValue++;
            }
            downstreamHandle.finish();
        }

        public void start() {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    doStart();
                }
            });
            thread.start();
        }

        @Override
        public void pause() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void resume(boolean async) {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    public void testSortMergeThreaded() throws Exception {
        CollectingRowReceiver finalReceiver = new CollectingRowReceiver();
        SortingRowMerger projector = new SortingRowMerger(
                finalReceiver,
                new int[]{0},
                new boolean[]{false},
                new Boolean[]{null}
        );

        Upstream upstream1 = new Upstream(projector, new Object[]{1}, new Object[]{3}, new Object[]{4});
        Upstream upstream2 = new Upstream(projector, new Object[]{2}, new Object[]{3}, new Object[]{5});
        Upstream upstream3 = new Upstream(projector, new Object[]{1}, new Object[]{3}, new Object[]{3}, new Object[]{4});
        Upstream upstream4 = new Upstream(projector, new Object[]{1}, new Object[]{3}, new Object[]{4});
        Upstream upstream5 = new Upstream(projector);


        upstream1.start();
        upstream2.start();
        upstream3.start();
        upstream4.start();
        upstream5.start();

        Bucket result = finalReceiver.result();

        assertThat(result.size(), is(13));
        Object[] column = TestingHelpers.getColumn(result, 0);
        assertThat(column, Matchers.<Object>arrayContaining(1, 1, 1, 2, 3, 3, 3, 3, 3, 4, 4, 4, 5));

    }

    @Test
    @Repeat(iterations = 10)
    @TestLogging("io.crate.operation.projectors.BlockingSortingQueuedRowDownstream:TRACE")
    public void testBlockingSortingQueuedRowDownstreamThreaded() throws Exception {
        CollectingRowReceiver receiver = new CollectingRowReceiver();
        BlockingSortingQueuedRowDownstream projector = new BlockingSortingQueuedRowDownstream(
                receiver,
                1,
                new int[]{0},
                new boolean[]{false},
                new Boolean[]{null}
        );

        SpareUpstream upstream1 = new SpareUpstream(projector, 30);
        SpareUpstream upstream2 = new SpareUpstream(projector, 30);
        SpareUpstream upstream3 = new SpareUpstream(projector, 7);
        SpareUpstream upstream4 = new SpareUpstream(projector, 13);
        SpareUpstream upstream5 = new SpareUpstream(projector, 0);

        upstream1.start();
        upstream2.start();
        upstream3.start();
        upstream4.start();
        upstream5.start();

        Bucket result = receiver.result();
        assertThat(result.size(), is(80));
        assertThat(result, TestingHelpers.isSorted(0));
    }

}
