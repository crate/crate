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

package io.crate.operation.projectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.crate.core.collections.ArrayRow;
import io.crate.core.collections.CollectionBucket;
import io.crate.core.collections.Row;
import io.crate.jobs.ExecutionState;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowUpstream;
import io.crate.operation.projectors.sorting.OrderingByPosition;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class RowMergers {

    private RowMergers() {}

    public static RowDownstream passThroughRowMerger(RowReceiver delegate) {
        MultiUpstreamRowReceiver rowReceiver;
        if (delegate.requiresRepeatSupport()) {
            rowReceiver = new RowCachingMultiUpstreamRowReceiver(delegate);
        } else {
            rowReceiver = new MultiUpstreamRowReceiver(delegate);
        }
        return new MultiUpstreamRowMerger(rowReceiver);
    }

    public static RowDownstream sortingRowMerger(RowReceiver delegate, int[] orderByPositions, boolean[] reverseFlags, Boolean[] nullsFirst) {
        if (delegate.requiresRepeatSupport()) {
            Ordering<Object[]> ordering = OrderingByPosition.arrayOrdering(orderByPositions, reverseFlags, nullsFirst);
            return new MultiUpstreamRowMerger(new SortingRowCachingMultiUpstreamRowReceiver(delegate, ordering));
        }
        return new SortingRowMerger(delegate, orderByPositions, reverseFlags, nullsFirst);
    }

    static class MultiUpstreamRowReceiver implements RowReceiver {

        private static final ESLogger LOGGER = Loggers.getLogger(MultiUpstreamRowReceiver.class);

        final RowReceiver delegate;
        final Set<RowUpstream> rowUpstreams = Sets.newConcurrentHashSet();
        private final AtomicInteger activeUpstreams = new AtomicInteger(0);
        private final AtomicBoolean prepared = new AtomicBoolean(false);
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final Object lock = new Object();
        ExecutionState executionState;

        volatile boolean paused = false;

        public MultiUpstreamRowReceiver(RowReceiver delegate) {
            this.delegate = delegate;
        }

        public List<Object[]> cachedRows() {
            throw new UnsupportedOperationException("Doesn't cache rows");
        }

        @Override
        public boolean setNextRow(Row row) {
            synchronized (lock) {
                return delegate.setNextRow(row);
            }
        }

        @Override
        public final void finish() {
            countdown();
        }

        @Override
        public final void fail(Throwable throwable) {
            failure.set(throwable);
            countdown();
        }

        /**
         * triggered if the last remaining upstream finished or failed
         */
        protected void onFinish() {
            delegate.finish();
        }

        /**
         * triggered if the last remaining upstream finished or failed
         */
        protected void onFail(Throwable t) {
            delegate.fail(t);
        }

        @Override
        public void prepare(ExecutionState executionState) {
            if (prepared.compareAndSet(false, true)) {
                delegate.prepare(executionState);
                this.executionState = executionState;
            }
        }

        @Override
        public boolean requiresRepeatSupport() {
            return false;
        }

        @Override
        public void setUpstream(RowUpstream rowUpstream) {
            if (!rowUpstreams.add(rowUpstream)) {
                LOGGER.debug("Upstream {} registered itself twice", rowUpstream);
            }
        }

        protected final void pause() {
            paused = true;
            for (RowUpstream rowUpstream : rowUpstreams) {
                rowUpstream.pause();
            }
        }

        protected final void resume(boolean async) {
            paused = false;
            for (RowUpstream rowUpstream : rowUpstreams) {
                rowUpstream.resume(async);
            }
        }

        private void countdown() {
            int remainingUpstreams = activeUpstreams.decrementAndGet();
            assert remainingUpstreams >= 0 : "activeUpstreams must not get negative: " + remainingUpstreams;
            if (remainingUpstreams == 0) {
                Throwable t = failure.get();
                if (t == null) {
                    onFinish();
                } else {
                    onFail(t);
                }
            }
        }
    }

    static class RowCachingMultiUpstreamRowReceiver extends MultiUpstreamRowReceiver {

        private final List<Object[]> rows = new ArrayList<>();
        private final Object lock = new Object();
        private final Queue<Object[]> pauseFifo = new LinkedList<>();
        private final ArrayRow sharedRow = new ArrayRow();

        public RowCachingMultiUpstreamRowReceiver(RowReceiver delegate) {
            super(delegate);
        }

        @Override
        public List<Object[]> cachedRows() {
            return rows;
        }

        @Override
        public boolean setNextRow(Row row) {
            boolean wantMore;
            synchronized (lock) {
                Object[] materializedRow = row.materialize();
                if (paused) {
                    pauseFifo.add(materializedRow);
                    wantMore = true;
                } else {
                    Object[] poll = pauseFifo.poll();
                    if (poll == null) {
                        wantMore = delegate.setNextRow(row);
                    } else {
                        pauseFifo.add(materializedRow);
                        sharedRow.cells(poll);
                        wantMore = delegate.setNextRow(sharedRow);
                    }
                }
                rows.add(materializedRow);
            }
            return wantMore;
        }

        @Override
        protected void onFinish() {
            assert !paused : "must not receive a finish call if upstream should be paused";
            for (Object[] objects : pauseFifo) {
                sharedRow.cells(objects);
                boolean wantMore = delegate.setNextRow(sharedRow);
                if (!wantMore) {
                    break;
                }
            }
            super.onFinish();
        }
    }

    static class SortingRowCachingMultiUpstreamRowReceiver extends MultiUpstreamRowReceiver {

        private final Ordering<Object[]> ordering;
        private final List<Object[]> rows = new ArrayList<>();
        private final Object lock = new Object();

        public SortingRowCachingMultiUpstreamRowReceiver(RowReceiver delegate, Ordering<Object[]> ordering) {
            super(delegate);
            this.ordering = ordering.reverse();
        }

        @Override
        public boolean setNextRow(Row row) {
            synchronized (lock) {
                rows.add(row.materialize());
            }
            return true;
        }

        @Override
        protected void onFinish() {
            Collections.sort(rows, ordering);
            IterableRowEmitter rowEmitter = new IterableRowEmitter(
                    delegate,
                    executionState,
                    new CollectionBucket(rows));
            rowEmitter.run();
        }
    }

    private static class MultiUpstreamRowMerger implements RowMerger {

        private final MultiUpstreamRowReceiver rowReceiver;
        private boolean repeated = false;

        public MultiUpstreamRowMerger(MultiUpstreamRowReceiver rowReceiver) {
            this.rowReceiver = rowReceiver;
            rowReceiver.delegate.setUpstream(this);
        }

        @Override
        public RowReceiver newRowReceiver() {
            rowReceiver.activeUpstreams.incrementAndGet();
            return rowReceiver;
        }

        @Override
        public void pause() {
            rowReceiver.pause();
        }

        @Override
        public void resume(boolean async) {
            rowReceiver.resume(async);
        }

        @Override
        public void repeat() {
            Preconditions.checkState(!repeated,
                    "Row receiver should have changed it's upstream after the first repeat call");
            repeated = true;
            // the rowEmitter becomes the new upstream for rowReceiver.delegate and handles further pause/resume/repeat calls
            IterableRowEmitter iterableRowEmitter = new IterableRowEmitter(
                    rowReceiver.delegate,
                    rowReceiver.executionState,
                    new CollectionBucket(rowReceiver.cachedRows()));
            iterableRowEmitter.run();
        }
    }
}
