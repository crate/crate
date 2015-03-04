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
import io.crate.core.collections.Buckets;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.RowUpstream;
import io.crate.operation.projectors.sorting.OrderingByPosition;
import org.elasticsearch.common.Nullable;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class MergeProjector implements Projector  {

    private final Ordering<Row> ordering;
    private List<MergeProjectorDownstreamHandle> downstreamHandles = new ArrayList<>();
    private final AtomicReference<Throwable> upstreamFailure = new AtomicReference<>(null);

    private RowDownstreamHandle downstreamContext;

    private Row lowestToEmit = null;

    private AtomicBoolean downstreamAborted = new AtomicBoolean(false);

    public MergeProjector(int[] orderBy,
                          boolean[] reverseFlags,
                          Boolean[] nullsFirst) {
        List<Comparator<Row>> comparators = new ArrayList<>(orderBy.length);
        for (int i = 0; i < orderBy.length; i++) {
            comparators.add(OrderingByPosition.rowOrdering(orderBy[i], reverseFlags[i], nullsFirst[i]));
        }
        ordering = Ordering.compound(comparators);
    }

    @Override
    public void startProjection() {
        if (downstreamHandles.size() <= 0) {
            finishDownStream();
        }
    }

    @Override
    public RowDownstreamHandle registerUpstream(RowUpstream upstream) {
        MergeProjectorDownstreamHandle handle = new MergeProjectorDownstreamHandle(this);
        downstreamHandles.add(handle);
        return handle;
    }

    @Override
    public void downstream(RowDownstream downstream) {
        downstreamContext = downstream.registerUpstream(this);
    }

    public void upstreamFinished(MergeProjectorDownstreamHandle handle) {
        if (!handle.isEmpty()) {
            return;
        }
        synchronized (this) {
            downstreamHandles.remove(handle);
        }
        if (!downstreamAborted.get()) {
            emit();
        }
        finishDownStream();
    }

    public void finishDownStream() {
        if (downstreamHandles.size() > 0) {
            return;
        }
        if (downstreamContext != null) {
            Throwable throwable = upstreamFailure.get();
            if (throwable == null) {
                downstreamContext.finish();
            } else {
                downstreamContext.fail(throwable);
            }
        }
    }

    public void upstreamFailed(Throwable throwable, MergeProjectorDownstreamHandle handle) {
        upstreamFailure.set(throwable);
        synchronized (this) {
            downstreamHandles.remove(handle);
        }
        downstreamAborted();
    }

    private void downstreamAborted() {
        if(downstreamAborted.compareAndSet(false, true)) {
            if (downstreamContext != null) {
                Throwable throwable = upstreamFailure.get();
                if (throwable == null) {
                    downstreamContext.finish();
                } else {
                    downstreamContext.fail(throwable);
                }
            }
            for (MergeProjectorDownstreamHandle handle : downstreamHandles) {
                handle.close();
            }
        }
    }

    public synchronized boolean emit() {
        boolean success = true;
        if (lowestToEmit == null ) {
            lowestToEmit = findLowestRow();
        }
        while (lowestToEmit != null) {
            Row nextLowest = null;
            boolean emptyHandle = false;
            ArrayList<RowDownstreamHandle> toRemove = new ArrayList<>();
            for (MergeProjectorDownstreamHandle handle : downstreamHandles) {
                try {
                    success &= handle.emitUntil(lowestToEmit);
                    // if there is an emptyHandle don't try to fetch the nextLowest because we'll abort anyway
                    if(success && !emptyHandle && (nextLowest == null || ordering.compare(handle.firstRow(), nextLowest) > 0)) {
                        nextLowest = handle.firstRow();
                    }
                } catch (NoSuchElementException e) {
                    if (handle.isFinished()) {
                        toRemove.add(handle);
                    } else {
                        emptyHandle = true;
                    }
                }
                if (!success) {
                    downstreamAborted();
                    return success;
                }
            }
            if (toRemove.size() > 0) {
                downstreamHandles.removeAll(toRemove);
                finishDownStream();
            }
            // If there is one empty handle everything which can be emitted is emitted
            if(emptyHandle) {
                break;
            } else {
                lowestToEmit = nextLowest;
            }
        }
        return success;
    }

    private @Nullable Row findLowestRow() {
        Row lowest = null;
        for (MergeProjectorDownstreamHandle handle : downstreamHandles ) {
            try {
                if (lowest == null) {
                    lowest = handle.firstRow();
                } else if (ordering.compare(handle.firstRow(), lowest) > 0) {
                    lowest = handle.firstRow();
                }
            } catch (NoSuchElementException e) {
                if (handle.isFinished()) {
                    continue;
                } else {
                    return null; // There is an empty downstreamHandle, abort
                }
            }
        }
        return lowest;
    }

    public class MergeProjectorDownstreamHandle implements RowDownstreamHandle {

        private final MergeProjector projector;

        private boolean finished = false;

        private boolean closed = false;

        private LinkedList<Row> rows = new LinkedList<>();

        public MergeProjectorDownstreamHandle(MergeProjector projector) {
            this.projector = projector;
        }

        public Row firstRow() throws NoSuchElementException{
            return rows.getFirst();
        }

        public boolean isEmpty() {
            return rows.isEmpty();
        }

        public void close() {
            rows.clear();
            closed = true;
        }

        public boolean emitUntil(Row until) {
            boolean success = true;
            while (success && ordering.compare(rows.getFirst(), until) >= 0) {
                success &= downstreamContext.setNextRow(rows.pollFirst());
            }
            return success;
        }

        @Override
        public boolean setNextRow(Row row) {
            if (closed) {
                return false;
            }
            row = new RowN(Buckets.materialize(row));
            synchronized (this.projector) {
                rows.addLast(row);
            }
            // Only try to emit if this handler was empty before
            // else we know that there must be a handler with a lower highest value than this.
            if (rows.size() == 1) {
                return projector.emit();
            } else {
                return true;
            }
        }

        @Override
        public void finish() {
            finished = true;
            projector.upstreamFinished(this);
        }

        public boolean isFinished() {
            return finished;
        }

        @Override
        public void fail(Throwable throwable) {
            projector.upstreamFailed(throwable, this);
        }
    }
}
