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
import io.crate.operation.RowDownstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.RowUpstream;
import io.crate.operation.projectors.sorting.OrderingByPosition;
import org.elasticsearch.common.Nullable;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class BlockingSortingQueuedRowDownstream implements Projector  {

    private final Ordering<Object[]> ordering;
    private final List<BlockingSortingQueuedRowDownstreamHandle> downstreamHandles = new ArrayList<>();
    private final List<RowUpstream> upstreams = new ArrayList<>();
    private final AtomicInteger remainingUpstreams = new AtomicInteger(0);
    private final AtomicBoolean downstreamAborted = new AtomicBoolean(false);
    private RowDownstreamHandle downstreamContext;
    private Object[] lowestToEmit = null;
    private final Object lowestToEmitLock = new Object();
    private final int rowSize;

    /**
     * To prevent the Handles to block and unblock frequently when the queue size is reached
     * we use two limits
     *
     * So the handle is blocked when queue has reached MAX_QUEUE_SIZE
     * And unblock the handle if the queue has 0 rows
     */
    public static int MAX_QUEUE_SIZE = 10;

    public BlockingSortingQueuedRowDownstream(int rowSize,
                                              int[] orderBy,
                                              boolean[] reverseFlags,
                                              Boolean[] nullsFirst) {
        this.rowSize = rowSize;
        List<Comparator<Object[]>> comparators = new ArrayList<>(orderBy.length);
        for (int i = 0; i < orderBy.length; i++) {
            comparators.add(OrderingByPosition.arrayOrdering(orderBy[i], reverseFlags[i], nullsFirst[i]));
        }
        ordering = Ordering.compound(comparators);
    }

    @Override
    public void startProjection(ExecutionState executionState) {
        if (remainingUpstreams.get() == 0) {
            upstreamFinished();
        }
    }

    @Override
    public RowDownstreamHandle registerUpstream(RowUpstream upstream) {
        upstreams.add(upstream);
        remainingUpstreams.incrementAndGet();
        BlockingSortingQueuedRowDownstreamHandle handle = new BlockingSortingQueuedRowDownstreamHandle(this, rowSize);
        downstreamHandles.add(handle);
        return handle;
    }

    @Override
    public void downstream(RowDownstream downstream) {
        downstreamContext = downstream.registerUpstream(this);
    }

    public void upstreamFinished() {
        emit();
        if (remainingUpstreams.decrementAndGet() <= 0) {
            if (downstreamContext != null) {
                downstreamContext.finish();
            }
        }
    }

    public void upstreamFailed(Throwable throwable) {
        downstreamAborted.compareAndSet(false, true);
        if (remainingUpstreams.decrementAndGet() == 0) {
            if (downstreamContext != null) {
                downstreamContext.fail(throwable);
            }
        }
    }

    public boolean emit() {
        Object[] lowestToEmit;
        synchronized (lowestToEmitLock) {
            lowestToEmit = this.lowestToEmit;
        }
        if (lowestToEmit == null) {
            lowestToEmit = findLowestCells();
        }

        while (lowestToEmit != null) {
            Object[] nextLowest = null;
            boolean emptyHandle = false;
            for (BlockingSortingQueuedRowDownstreamHandle handle : downstreamHandles) {
                if (!handle.emitUntil(lowestToEmit)) {
                    downstreamAborted.set(true);
                    return false;
                }
                Object[] cells = handle.firstCells();
                if (cells == null) {
                    if (!handle.isFinished()) {
                        emptyHandle = true;
                    }
                    continue;
                }

                if (nextLowest == null || ordering.compare(cells, nextLowest) > 0) {
                    nextLowest = cells;
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
    private Object[] findLowestCells() {
        Object[] lowest = null;
        for (BlockingSortingQueuedRowDownstreamHandle handle : downstreamHandles ) {
            Object[] cells = handle.firstCells();
            if (cells == null) {
                if (!handle.isFinished()) {
                    return null; // There is an empty downstreamHandle, abort
                }
                continue;
            }

            if (lowest == null) {
                lowest = cells;
            } else if (ordering.compare(cells, lowest) > 0) {
                lowest = cells;
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

    public class BlockingSortingQueuedRowDownstreamHandle implements RowDownstreamHandle {

        private final BlockingSortingQueuedRowDownstream projector;
        private final Object lock = new Object();
        private final Object pauseLock = new Object();
        private AtomicBoolean finished = new AtomicBoolean(false);
        private Object[] firstCells = null;
        private final RowN row;

        private ArrayDeque<Object[]> cellsQueue = new ArrayDeque<>();

        private final AtomicBoolean pendingPause = new AtomicBoolean(false);
        private boolean paused = false;

        public BlockingSortingQueuedRowDownstreamHandle(BlockingSortingQueuedRowDownstream projector, int rowSize) {
            this.projector = projector;
            this.row = new RowN(rowSize);
        }

        public Object[] firstCells() throws NoSuchElementException {
            synchronized (lock) {
                return firstCells;
            }
        }

        public Row poll() {
            synchronized (lock) {
                Object[] cells = cellsQueue.poll();
                int size = cellsQueue.size();
                if (size == 0) {
                    firstCells = null;
                } else {
                    firstCells = cellsQueue.getFirst();
                }
                row.cells(cells);
                return row;
            }
        }

        public boolean emitUntil(Object[] until) {
            boolean res = true;
            synchronized (lock) {
                while (firstCells != null && ordering.compare(firstCells, until) >= 0) {
                    if (!downstreamContext.setNextRow(poll())) {
                        res = false;
                        break;
                    }
                }
                if (paused && cellsQueue.size() < MAX_QUEUE_SIZE) {
                    resume();
                }
            }
            return res;
        }

        @Override
        public boolean setNextRow(Row row) {
            if (projector.downstreamAborted.get()) {
                return false;
            }
            int size;
            boolean pause = false;
            synchronized (lock) {
                Object[] cells = addCellsToQueue(row);
                size = cellsQueue.size();
                if (firstCells == null) {
                    firstCells = cells;
                }
                if (!paused && size == MAX_QUEUE_SIZE){
                    pause = true;
                    pendingPause.set(true);
                }
            }
            if (pause) {
                pause();
            }
            // Only try to emit if this handler was empty before
            // else we know that there must be a handler with a lower highest value than this.
            return size != 1 || projector.emit();
        }

        private Object[] addCellsToQueue(Row row) {
            Object[] cells = new Object[rowSize];
            for (int i = 0; i < rowSize; i++) {
                cells[i] = row.get(i);
            }
            cellsQueue.add(cells);
            return cells;
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

        private void pause() {
            synchronized (pauseLock) {
                if (pendingPause.compareAndSet(true, false)) {
                    try {
                        paused = true;
                        pauseLock.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

        }

        private void resume() {
            synchronized (pauseLock) {
                if (!pendingPause.getAndSet(false)) {
                    paused = false;
                    pauseLock.notify();
                }
            }
        }
    }
}
