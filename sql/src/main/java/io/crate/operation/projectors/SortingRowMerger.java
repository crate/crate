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

package io.crate.operation.projectors;

import com.google.common.collect.Ordering;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.jobs.ExecutionState;
import io.crate.operation.RowUpstream;
import io.crate.operation.projectors.sorting.OrderingByPosition;
import org.elasticsearch.common.Nullable;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SortingRowMerger implements RowMerger {

    private final Ordering<Row> ordering;
    private final List<MergeProjectorDownstreamHandle> downstreamHandles = new ArrayList<>();
    private final List<RowUpstream> upstreams = new ArrayList<>();
    private final AtomicInteger remainingUpstreams = new AtomicInteger(0);
    private final AtomicBoolean downstreamAborted = new AtomicBoolean(false);
    private final RowReceiver rowReceiver;
    private Row lowestToEmit = null;
    private final Object lowestToEmitLock = new Object();


    public SortingRowMerger(RowReceiver rowReceiver, int[] orderBy, boolean[] reverseFlags, Boolean[] nullsFirst) {
        this.rowReceiver = rowReceiver;
        rowReceiver.setUpstream(this);
        List<Comparator<Row>> comparators = new ArrayList<>(orderBy.length);
        for (int i = 0; i < orderBy.length; i++) {
            comparators.add(OrderingByPosition.rowOrdering(orderBy[i], reverseFlags[i], nullsFirst[i]));
        }
        ordering = Ordering.compound(comparators);
    }

    @Override
    public RowReceiver newRowReceiver() {
        remainingUpstreams.incrementAndGet();
        MergeProjectorDownstreamHandle handle = new MergeProjectorDownstreamHandle(this);
        downstreamHandles.add(handle);
        return handle;
    }

    public void upstreamFinished() {
        emit();
        if (remainingUpstreams.decrementAndGet() <= 0) {
            rowReceiver.finish();
        }
    }

    public void upstreamFailed(Throwable throwable) {
        downstreamAborted.compareAndSet(false, true);
        if (remainingUpstreams.decrementAndGet() == 0) {
            rowReceiver.fail(throwable);
        }
    }

    public boolean emit() {
        Row lowestToEmit;
        synchronized (lowestToEmitLock) {
            lowestToEmit = this.lowestToEmit;
        }
        if (lowestToEmit == null) {
            lowestToEmit = findLowestRow();
        }

        while (lowestToEmit != null) {
            Row nextLowest = null;
            boolean emptyHandle = false;
            for (MergeProjectorDownstreamHandle handle : downstreamHandles) {
                if (!handle.emitUntil(lowestToEmit)) {
                    downstreamAborted.set(true);
                    return false;
                }
                Row row = handle.firstRow();
                if (row == null) {
                    if (!handle.isFinished()) {
                        emptyHandle = true;
                    }
                    continue;
                }

                if (nextLowest == null || ordering.compare(row, nextLowest) > 0) {
                    nextLowest = row;
                }
            }
            // If there is one empty handle everything which can be emitted is emitted
            if (emptyHandle) {
                break;
            } else {
                lowestToEmit = nextLowest;
            }
        }
        synchronized (lowestToEmitLock) {
            this.lowestToEmit = lowestToEmit;
        }
        return true;
    }

    @Nullable
    private Row findLowestRow() {
        Row lowest = null;
        for (MergeProjectorDownstreamHandle handle : downstreamHandles ) {
            Row row = handle.firstRow();
            if (row == null) {
                if (!handle.isFinished()) {
                    return null; // There is an empty downstreamHandle, abort
                }
                continue;
            }

            if (lowest == null) {
                lowest = row;
            } else if (ordering.compare(row, lowest) > 0) {
                lowest = row;
            }
        }
        return lowest;
    }

    @Override
    public void pause() {
        for (RowUpstream upstream : upstreams) {
            upstream.pause();
        }
    }

    @Override
    public void resume(boolean async) {
        for (RowUpstream upstream : upstreams) {
            upstream.resume(async);
        }
    }

    /**
     * tells the RowUpstream that it should push all rows again
     */
    @Override
    public void repeat() {
        throw new UnsupportedOperationException();
    }

    public class MergeProjectorDownstreamHandle implements RowReceiver {

        private final SortingRowMerger projector;
        private final Object lock = new Object();
        private AtomicBoolean finished = new AtomicBoolean(false);
        private Row firstRow = null;

        private ArrayDeque<Row> rows = new ArrayDeque<>();

        public MergeProjectorDownstreamHandle(SortingRowMerger projector) {
            this.projector = projector;
        }

        public Row firstRow() throws NoSuchElementException {
            synchronized (lock) {
                return firstRow;
            }
        }

        public Row poll() {
            synchronized (lock) {
                Row row = rows.poll();
                if (rows.size() == 0) {
                    firstRow = null;
                } else {
                    firstRow = rows.getFirst();
                }
                return row;
            }
        }

        public boolean emitUntil(Row until) {
            synchronized (lock) {
                while (firstRow != null && ordering.compare(firstRow, until) >= 0) {
                    if (!rowReceiver.setNextRow(poll())) {
                        return false;
                    }
                }
            }
            return true;
        }

        @Override
        public boolean setNextRow(Row row) {
            if (projector.downstreamAborted.get()) {
                return false;
            }
            row = new RowN(row.materialize());
            int size;
            synchronized (lock) {
                rows.add(row);
                size = rows.size();
                if (firstRow == null) {
                    firstRow = row;
                }
            }
            // Only try to emit if this handler was empty before
            // else we know that there must be a handler with a lower highest value than this.
            return size != 1 || projector.emit();
        }

        @Override
        public void finish() {
            if (finished.compareAndSet(false, true)) {
                projector.upstreamFinished();
            }
        }

        public boolean isFinished() {
            return finished.get();
        }

        @Override
        public void fail(Throwable throwable) {
            projector.upstreamFailed(throwable);
        }

        @Override
        public void prepare(ExecutionState executionState) {

        }

        @Override
        public Set<Requirement> requirements() {
            return projector.rowReceiver.requirements();
        }

        @Override
        public void setUpstream(RowUpstream rowUpstream) {
            upstreams.add(rowUpstream);
        }
    }
}
