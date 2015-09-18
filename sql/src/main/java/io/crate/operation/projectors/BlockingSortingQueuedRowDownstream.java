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
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class BlockingSortingQueuedRowDownstream implements RowMerger {

    private static final ESLogger LOGGER = Loggers.getLogger(BlockingSortingQueuedRowDownstream.class);

    /**
     * To prevent the Handles to block and unblock frequently when the queue size is reached
     * we use two limits
     *
     * So the handle is blocked when queue has reached MAX_QUEUE_SIZE
     * And unblock the handle if the queue has 0 rows
     */
    public static int MAX_QUEUE_SIZE = 1000;
    public static int RESUME_AFTER = 10;

    private final Ordering<Object[]> ordering;
    private final List<BlockingSortingQueuedRowDownstreamHandle> downstreamHandles = new ArrayList<>();
    private final List<RowUpstream> upstreams = new ArrayList<>();
    private final AtomicInteger remainingUpstreams = new AtomicInteger(0);
    private final AtomicBoolean downstreamAborted = new AtomicBoolean(false);
    private final RowReceiver downstreamRowReceiver;
    private final SharedArrayRef lowestToEmit;
    private final int rowSize;
    private final AtomicInteger runningHandles = new AtomicInteger(0);

    private int pauseCount = 0;


    public BlockingSortingQueuedRowDownstream(RowReceiver rowReceiver,
                                              int rowSize,
                                              int[] orderBy,
                                              boolean[] reverseFlags,
                                              Boolean[] nullsFirst) {
        this.rowSize = rowSize;
        List<Comparator<Object[]>> comparators = new ArrayList<>(orderBy.length);
        for (int i = 0; i < orderBy.length; i++) {
            comparators.add(OrderingByPosition.arrayOrdering(orderBy[i], reverseFlags[i], nullsFirst[i]));
        }
        ordering = Ordering.compound(comparators);
        lowestToEmit = new SharedArrayRef(rowSize);
        this.downstreamRowReceiver = rowReceiver;
        rowReceiver.setUpstream(this);
    }

    @Override
    public RowReceiver newRowReceiver() {
        remainingUpstreams.incrementAndGet();
        int handleIdx = runningHandles.getAndIncrement();
        BlockingSortingQueuedRowDownstreamHandle handle = new BlockingSortingQueuedRowDownstreamHandle(
                handleIdx, this, rowSize);
        downstreamHandles.add(handle);
        return handle;
    }

    public void upstreamFinished() {
        runningHandles.decrementAndGet();
        int remaining = remainingUpstreams.decrementAndGet();
        if (!emit()) {
            // if downstream is finished, wake every handle up, so it can signal its upstream that it's done
            for (BlockingSortingQueuedRowDownstreamHandle handle : downstreamHandles) {
                if (!handle.isFinished()) {
                    handle.resume();
                }
            }
        }
        if (remaining == 0) {
            LOGGER.trace("paused {} times, per handle rate: {}", pauseCount, pauseCount/(double)downstreamHandles.size());
            downstreamRowReceiver.finish();
        }
    }

    public void upstreamFailed(Throwable throwable) {
        runningHandles.decrementAndGet();
        downstreamAborted.compareAndSet(false, true);
        int remaining = remainingUpstreams.decrementAndGet();
        if (remaining == 0) {
            downstreamRowReceiver.fail(throwable);
        }
    }

    public boolean emit() {
        do {
            Object[] currentLowest = lowestToEmit.get();
            Object[] nextLowest = null;
            boolean emptyHandle = false;
            for (BlockingSortingQueuedRowDownstreamHandle handle : downstreamHandles) {
                if (currentLowest != null && !handle.emitUntil(currentLowest)) {
                    downstreamAborted.set(true);
                    return false;
                }
                Object[] cells = handle.firstCells();
                if (cells == null) {
                    if (!handle.isFinished()) {
                        emptyHandle = true;
                        break;
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
                lowestToEmit.set(nextLowest);
            }
        } while (lowestToEmit.isValid());
        return true;
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

    private boolean anyOtherNotStartedYet(final int butMe) {
        for (BlockingSortingQueuedRowDownstreamHandle handle : downstreamHandles) {
            if (!handle.started && handle.index != butMe) {
                return true;
            }
        }
        return false;
    }

    public class BlockingSortingQueuedRowDownstreamHandle implements RowReceiver {

        private final int index;
        private final BlockingSortingQueuedRowDownstream projector;
        private final RowN row;

        private final AtomicBoolean pendingPause = new AtomicBoolean(false);
        private final Object lock = new Object();
        private final Object pauseLock = new Object();

        private final AtomicBoolean collectorPaused = new AtomicBoolean(false);
        private volatile boolean paused = false;

        private RowUpstream myUpstream;
        private AtomicBoolean finished = new AtomicBoolean(false);
        private Object[] firstCells = null;
        private boolean started = false;

        private final ArrayDeque<Object[]> cellsQueue = new ArrayDeque<>(MAX_QUEUE_SIZE);
        private final ObjectPool<Object[]> pool = new ObjectPool<Object[]>(MAX_QUEUE_SIZE) {
            @Override
            public Object[] createObject() {
                return new Object[rowSize];
            }
        };


        public BlockingSortingQueuedRowDownstreamHandle(int index, BlockingSortingQueuedRowDownstream projector, int rowSize) {
            this.index = index;
            this.projector = projector;
            this.row = new RowN(rowSize);
        }

        @Nullable
        public Object[] firstCells() {
            synchronized (lock) {
                return firstCells;
            }
        }

        public Object[] poll() {
            // only called when holding the lock
            Object[] cells = cellsQueue.poll();
            firstCells = cellsQueue.peekFirst();
            return cells;
        }

        public boolean emitUntil(Object[] until) {
            boolean res = true;
            synchronized (lock) {
                while (firstCells != null && ordering.compare(firstCells, until) >= 0) {
                    Object[] cells = poll();
                    row.cells(cells);
                    boolean wantMore = downstreamRowReceiver.setNextRow(row);
                    pool.checkin(cells);
                    if (!wantMore) {
                        res = false;
                        break;
                    }
                }
                int size = cellsQueue.size();
                int running = runningHandles.get();
                if (paused && (size <= RESUME_AFTER || ( size < MAX_QUEUE_SIZE && running == 1 ))) {
                    resume();
                }
            }
            return res;
        }

        @Override
        public boolean setNextRow(Row row) {
            started = true;
            if (projector.downstreamAborted.get()) {
                return false;
            }
            int size;
            synchronized (lock) {
                Object[] cells = addCellsToQueue(row);
                size = cellsQueue.size();
                if (firstCells == null) {
                    firstCells = cells;
                }
                if (!paused && size == MAX_QUEUE_SIZE) {
                    paused = true;
                    pendingPause.set(true);
                }
            }
            if (paused) {
                if (projector.anyOtherNotStartedYet(index)) {
                    pauseCollector();
                    return !projector.downstreamAborted.get();
                } else {
                    pauseThread();
                }
            }
            // Only try to emit if this handler was empty before
            // else we know that there must be a handler with a lower highest value than this.
            return size != 1 || projector.emit();
        }

        private Object[] addCellsToQueue(Row row) {
            Object[] cells = pool.checkout();
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

        @Override
        public void prepare(ExecutionState executionState) {

        }

        @Override
        public void setUpstream(RowUpstream rowUpstream) {
            this.myUpstream = rowUpstream;
            upstreams.add(rowUpstream);
        }

        private void pauseThread() {
            synchronized (pauseLock) {
                // WE HAVE TO PAUSE THE UPSTREAM
                // IN CASE WE HAVE TO WAIT FOR SOME
                // OTHER UPSTREAMS TO PRODUCE ROWS
                if (pendingPause.compareAndSet(true, false)) {
                    pauseCount++;
                    try {
                        runningHandles.decrementAndGet();
                        pauseLock.wait();
                        runningHandles.incrementAndGet();
                    } catch (InterruptedException e) {
                        LOGGER.trace("interrupted while paused", e);
                    }
                }
            }
        }

        private void pauseCollector() {
            synchronized (pauseLock) {
                if (pendingPause.compareAndSet(true, false)) {
                    assert myUpstream != null;
                    if (collectorPaused.compareAndSet(false, true)) {
                        pauseCount++;
                        runningHandles.decrementAndGet();
                        myUpstream.pause();
                    }
                }
            }
        }

        private void resume() {
            synchronized (pauseLock) {
                if (!pendingPause.getAndSet(false)) {
                    paused = false;
                    if (collectorPaused.compareAndSet(true, false)) {
                        resumeCollector();
                    } else {
                        resumeThread();
                    }
                }
            }
        }

        private void resumeThread() {
            pauseLock.notify();
        }

        private void resumeCollector() {
            myUpstream.resume(true);
            runningHandles.incrementAndGet();
        }
    }


    /**
     * Simple object pool with fixed size.
     *
     * it is possible that it creates more objects than initially defined, but at no point,
     * more are put in the pool. they are silently discarded
     * @param <T> the type of the pooled instances
     */
    private abstract static class ObjectPool<T> {
        private final ArrayDeque<T> spareQueue;
        private final int maxSize;

        public ObjectPool(int size) {
            this.spareQueue = new ArrayDeque<>(size);
            this.maxSize = size;
        }

        public abstract T createObject();

        private T checkout() {
            if (spareQueue.isEmpty()) {
                return createObject();
            } else {
                return spareQueue.poll();
            }
        }

        private void checkin(T obj) {
            if (spareQueue.size() < maxSize) {
                spareQueue.add(obj);
            }
        }
    }

    /**
     * keep a reference on a single array,
     * encapsulating invalidity (which was done with null before)
     * and copying array contents to the internal array instead of
     * keeping a shared array around whose contents may change.
     *
     * This class is not thread safe.
     */
    private static class SharedArrayRef {

        private final Object[] emitMe;
        private boolean valid;

        public SharedArrayRef(int rowSize) {
            emitMe = new Object[rowSize];
            valid = false;
        }


        public void set(@Nullable Object[] newEmitMe) {
            if (newEmitMe == null) {
                valid = false;
            } else {
                System.arraycopy(newEmitMe, 0, emitMe, 0, emitMe.length);
                valid = true;
            }
        }

        @Nullable
        public Object[] get() {
            if (!valid) {
                return null;
            }
            return emitMe;
        }

        public boolean isValid() {
            return valid;
        }
    }
}
