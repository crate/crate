/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.data;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.concurrent.CompletionListenable;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.operation.collect.CrateCollector;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class BatchCursorTest {


    static abstract class BaseBatchConsumer implements BatchConsumer, CompletionListenable {
        protected  BatchCursor cursor;
        private SettableFuture<Void> completionFuture = SettableFuture.create();

        private void consume() {
            if (cursor.status() == BatchCursor.Status.ON_ROW) {
                do {
                    handleRow();
                } while (cursor.moveNext());
            }
            if (!cursor.allLoaded()) {
                cursor.loadNextBatch().addListener(this::consume, MoreExecutors.directExecutor());
            } else {
                completionFuture.set(null);
                cursor.close();
            }
        }

        @Override
        public ListenableFuture<?> completionFuture() {
            return completionFuture;
        }

        protected abstract void handleRow();

        @Override
        public void accept(BatchCursor batchCursor) {
            this.cursor = batchCursor;
            consume();
        }
    }

    static class BatchPrinter extends BaseBatchConsumer {
        protected void handleRow() {
            System.out.println("row: " + Arrays.toString(cursor.materialize()));
        }
    }

    static class BatchCollector extends BaseBatchConsumer {

        private final List<Row> rows = new ArrayList<>();

        @Override
        protected void handleRow() {
            System.err.println("handleRow: " + Arrays.toString(cursor.materialize()));
            rows.add(new RowN(cursor.materialize()));
        }
    }

    static class DataCursor implements BatchCursor {

        private final Object[][] data;
        private final int rowSize;
        private int idx = 0;
        private final int numBatches;
        private int batchNum = 1;

        DataCursor(Object[][] data, int rowSize, int numBatches) {
            this.data = data;
            this.rowSize = rowSize;
            this.numBatches = numBatches;
        }


        @Override
        public boolean moveFirst() {
            return data.length > (idx = 0);
        }

        @Override
        public boolean moveNext() {
            return data.length * batchNum > ++idx;
        }

        @Override
        public void close() {
            idx = -1;
        }

        @Override
        public Status status() {
            if (idx >= data.length * batchNum) {
                return Status.OFF_ROW;
            } else if (idx == -1) {
                return Status.CLOSED;
            }
            return Status.ON_ROW;
        }

        @Override
        public ListenableFuture<Void> loadNextBatch() {
            if (allLoaded()){
                throw new IllegalStateException("loadNextBatch not allowed on loaded cursor");
            }
            if (status() != Status.OFF_ROW){
                throw new IllegalStateException("loadNextBatch not OFF_ROW");
            }
            batchNum++;
            return Futures.immediateFuture(null);
        }

        @Override
        public boolean allLoaded() {
            return batchNum >= numBatches;
        }

        @Override
        public int size() {
            return rowSize;
        }

        @Override
        public Object get(int index) {
            assert status() == Status.ON_ROW;
            assert index <= rowSize;
            assert idx < data.length*batchNum;
            return data[idx%data.length][index];
        }

        @Override
        public Object[] materialize() {
            assert status() == Status.ON_ROW;
            return Arrays.copyOf(data[idx%data.length], rowSize);
        }
    }

    static class SomeDataSource implements CrateCollector {

        private final BatchConsumer downstream;
        private final BatchCursor data;


        SomeDataSource(BatchConsumer downstream, BatchCursor data) {
            this.downstream = downstream;
            this.data = data;
        }

        @Override
        public void doCollect() {
            downstream.accept(data);
        }

        @Override
        public void kill(@Nullable Throwable throwable) {

        }
    }

    class LimitProjector implements BatchProjector {

        private final int limit;

        LimitProjector(int limit) {
            this.limit = limit;
        }

        @Override
        public BatchCursor apply(BatchCursor batchCursor) {
            return new Cursor(batchCursor);
        }

        private class Cursor extends BatchCursorProxy {

            private int pos = 1;
            private boolean limitReached = false;

            public Cursor(BatchCursor delegate) {
                super(delegate);
            }

            @Override
            public boolean moveNext() {
                assert pos <= limit || limit == 0: "pos run over limit";
                if (pos >= limit) {
                    limitReached = true;
                    return false;
                }
                pos++;
                return super.moveNext();
            }

            @Override
            public Status status() {
                Status s = super.status();
                // if the limit is reached and the upstream is still on a row return off row;
                if (s == Status.ON_ROW && limitReached) {
                    return Status.OFF_ROW;
                }
                return s;
            }

            @Override
            public ListenableFuture<?> loadNextBatch() {
                if (limitReached) {
                    throw new IllegalStateException("all data loaded already");
                }
                return super.loadNextBatch();
            }

            @Override
            public boolean moveFirst() {
                limitReached = limit == (pos = 1);
                return super.moveFirst();
            }

            @Override
            public boolean allLoaded() {
                return limitReached || super.allLoaded();
            }

            @Override
            public Object get(int index) {
                if (limitReached) {
                    throw new IllegalStateException("not on a row");
                }
                return super.get(index);
            }

            @Override
            public Object[] materialize() {
                if (limitReached) {
                    throw new IllegalStateException("not on a row");
                }
                return super.materialize();
            }
        }

    }

    @Test
    public void testDataCursor() throws Exception {
        DataCursor d = new DataCursor(new Object[][]{{1}, {3}, {5}, {7}}, 1, 1);
        BatchCollector c = new BatchCollector();
        assertThat(d.allLoaded(), is(true));
        c.accept(d);
        c.completionFuture().get();
        assertThat(c.rows.size(), is(4));

        d = new DataCursor(new Object[][]{{1}, {3}, {5}, {7}}, 1, 2);
        c = new BatchCollector();
        assertThat(d.allLoaded(), is(false));
        c.accept(d);
        c.completionFuture().get();
        assertThat(d.allLoaded(), is(true));
        assertThat(c.rows.size(), is(8));
    }

    @Test
    public void testLimitProjector() throws Exception {
        DataCursor d = new DataCursor(new Object[][]{{1, 2}, {3, 4}, {5, 6}, {7, 8}}, 2, 2);
        BatchProjector lp = new LimitProjector(3);
        List<BatchProjector> projectors = Arrays.asList(lp);
        BatchCollector c = new BatchCollector();
        BatchConsumer chain = c.projected(projectors);
        SomeDataSource s = new SomeDataSource(chain, d);
        s.doCollect();

        assertThat(c.rows.size(), is(3));

    }


    @Test
    public void testNestedLoop() throws Exception {
        DataCursor d1 = new DataCursor(new Object[][]{{1}, {3}, {5}, {7}}, 1, 2);
        DataCursor d2 = new DataCursor(new Object[][]{{2}, {4}, {6}, {8}}, 1, 2);
        BatchCollector c = new BatchCollector();

        BatchedNestedLoopOperation nl = new BatchedNestedLoopOperation(c);

        nl.left().accept(d1);
        nl.right().accept(d2);

        nl.completionFuture().get();
        assertThat(c.rows.size(), is(64));


    }
}
