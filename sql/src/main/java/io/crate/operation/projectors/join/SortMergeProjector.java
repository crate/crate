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

package io.crate.operation.projectors.join;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.crate.operation.ProjectorUpstream;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.projectors.Projector;
import io.crate.operation.projectors.YProjector;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


/**
 * SortMergeProjector a.k.a. DoubleHeadedSortMergeProjectorMonsterTruckSpaceLordMotherF***erFromHell
 *
 * Assumptions:
 *  * inputs to both {@link #leftProjector()} and {@link #rightProjector()} are sorted by the join condition
 *  * left and right projectors are executed on different threads
 *
 *
 * Example Flow:
 *
 * LEFT                                 RIGHT
 *
 * setNextLeftRows([1,1,1])
 *  right not set, cannot join
 *  wait for RIGHT --------------------> setNextRightRows([0, 0])
 *                                       right is lower than left
 *                                       remove right
 *
 *                                       setNextRightRows([1, 1])
 *                                       join()
 *                                       remove left
 * setNextLeftRows([2, 2]) <------------ signal left can continue
 * ...                                   remove right
 *
 *                                       setNextRightRows([3, 3])
 *                                       ...
 */
public class SortMergeProjector implements YProjector, ProjectorUpstream {

    private final ESLogger logger = Loggers.getLogger(getClass());

    private static enum Side {
        LEFT,
        RIGHT,
        BOTH;

        public boolean matches(Side side) {
            return side == this || this == BOTH;
        }
    }

    private static List<Object[]> SENTINEL = Collections.emptyList();

    private final Projector leftProjector;
    private final Projector rightProjector;

    private Projector downstream;
    private final int offset;
    private final int limit;
    private final AtomicBoolean wantMore;
    private final AtomicInteger rowsSkipped;
    private final AtomicInteger rowsProduced;

    private final AtomicBoolean leftFinished;
    private final AtomicBoolean rightFinished;

    private final CollectExpression[] leftCollectExpressions;
    private final CollectExpression[] rightCollectExpressions;

    private final Ordering[] comparators;

    private final AtomicReference<List<Object[]>> currentRightRows;
    private final AtomicReference<List<Object[]>> currentLeftRows;
    private final ReentrantLock lock;
    private final Condition leftCanContinue;
    private final Condition rightCanContinue;
    private final AtomicBoolean projectionStarted;

    public SortMergeProjector(int offset,
                              int limit,
                              CollectExpression[] leftCollectExpressions,
                              CollectExpression[] rightCollectExpressions,
                              Ordering[] comparators) {
        Preconditions.checkArgument(leftCollectExpressions.length == rightCollectExpressions.length,
                "number of join attributes on each side differ");
        Preconditions.checkArgument(leftCollectExpressions.length == comparators.length,
                "number of comparators differs from join attributes");
        this.offset = offset;
        this.limit = limit;
        this.comparators = comparators;
        this.rowsSkipped = new AtomicInteger(0);
        this.rowsProduced = new AtomicInteger(0);
        this.wantMore = new AtomicBoolean(true);

        // marker that internal projectors are finished
        this.leftFinished = new AtomicBoolean(false);
        this.rightFinished = new AtomicBoolean(false);

        this.leftCollectExpressions = leftCollectExpressions;
        this.rightCollectExpressions = rightCollectExpressions;
        this.currentRightRows = new AtomicReference<>();
        this.currentLeftRows = new AtomicReference<>();
        this.lock = new ReentrantLock();
        this.leftCanContinue = lock.newCondition();
        this.rightCanContinue = lock.newCondition();

        this.leftProjector = new InternalProjector(leftCollectExpressions) {
            @Override
            boolean doSetNextRows(List<Object[]> rows) {
                return setNextLeftRows(rows);
            }
        };
        this.rightProjector = new InternalProjector(rightCollectExpressions) {
            @Override
            boolean doSetNextRows(List<Object[]> rows) {
                return setNextRightRows(rows);
            }
        };
        this.projectionStarted = new AtomicBoolean(false);
    }

    private void onProjectorFinished() {
        if (rightFinished.get() && leftFinished.get()) {
            downstream.upstreamFinished();
        }
    }

    private synchronized void onProjectorFailed(Throwable throwable) {
        downstream.upstreamFailed(throwable);
        this.wantMore.set(false);
    }


    @Override
    public void startProjection() {
        if (!projectionStarted.getAndSet(true)) {
            downstream.startProjection();
        }
    }

    @SuppressWarnings("unchecked")
    private int compare(Object[] left, Object[] right) {
        for (CollectExpression leftCollectExpression : leftCollectExpressions) {
            leftCollectExpression.setNextRow(left);
        }
        for (CollectExpression rightCollectExpression : rightCollectExpressions) {
            rightCollectExpression.setNextRow(right);
        }
        int comparisonResult = 0;
        for (int i = 0, size = rightCollectExpressions.length; i < size; i++) {

            comparisonResult = comparators[i].compare(
                    leftCollectExpressions[i].value(),
                    rightCollectExpressions[i].value()
            );
            if (comparisonResult != 0) {
                return comparisonResult;
            }
        }
        return comparisonResult;
    }

    @Override
    public Projector leftProjector() {
        return leftProjector;
    }

    @Override
    public Projector rightProjector() {
        return rightProjector;
    }

    private void removeFromLeft() throws InterruptedException {
        currentLeftRows.set(null);
        if (leftFinished.get()) {
            onProjectorFinished();
        }
    }

    private void removeFromRight() throws InterruptedException {
        currentRightRows.set(null);
        if (rightFinished.get()) {
            onProjectorFinished();
        }
    }

    private boolean internalProjectorsFinished() {
        return rightFinished.get() || leftFinished.get();
    }

    /**
     * @return true if we can continue, false if we should stop because projection is finished
     * @throws InterruptedException
     */
    private boolean waitForSide(Side side) throws InterruptedException{
        assert side != Side.BOTH;
        Condition condition = side.matches(Side.LEFT) ? rightCanContinue : leftCanContinue;
        do {
            logger.trace("waiting for {} to proceed", side);
            // releases the lock
            if (condition.await(100, TimeUnit.MILLISECONDS)) {
                // can continue
                return true;
            }
        } while (!internalProjectorsFinished());
        return false;
    }

    private boolean setNextLeftRows(List<Object[]> leftRows) {
        try {
            lock.lockInterruptibly();
            try {
                if (leftRows == SENTINEL) {
                    leftFinished.set(true);
                    this.wantMore.set(false);
                    onProjectorFinished();
                } else if (leftRows != null && !internalProjectorsFinished()) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("get left rows {}", Arrays.deepToString(leftRows.toArray()));
                    }
                    currentLeftRows.set(leftRows);
                    List<Object[]> rightRows = currentRightRows.get();
                    if (rightRows == null) {
                        if (!waitForSide(Side.RIGHT)) {
                            // remove and optionally finish this projector if we're done
                            removeFromLeft();
                        }
                    } else {
                        // join and wait for right if we need more from it
                        Side toProceed = consumeRows(leftRows, rightRows);
                        if (!toProceed.matches(Side.LEFT)) {
                            waitForSide(Side.RIGHT);
                        }
                    }
                }
            } finally {
                lock.unlock();
            }
        } catch (InterruptedException e) {
            // TODO: really propagate?
            logger.trace("left interrupted", e);
            downstream.upstreamFailed(e);
            Thread.currentThread().interrupt();
        }
        return wantMore.get();
    }

    private boolean setNextRightRows(List<Object[]> rightRows) {
        try {
            // ignore null or empty groups
            lock.lockInterruptibly();
            try {
                if (rightRows == SENTINEL) {
                    rightFinished.set(true);
                    this.wantMore.set(false);
                    onProjectorFinished();
                } else if (rightRows != null && !internalProjectorsFinished()) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("get right rows {}", Arrays.deepToString(rightRows.toArray()));
                    }
                    currentRightRows.set(rightRows);
                        List<Object[]> leftRows = currentLeftRows.get();
                        if (leftRows == null) {
                            if (!waitForSide(Side.LEFT)) {
                                removeFromRight();
                            }
                        } else {
                            Side toProceed = consumeRows(leftRows, rightRows);
                            if (!toProceed.matches(Side.RIGHT)) {
                                waitForSide(Side.LEFT);
                            }
                        }
                }
            } finally {
                lock.unlock();
            }
        } catch (InterruptedException e) {
            // TODO: really propagate?
            logger.trace("right interrupted", e);
            downstream.upstreamFailed(e);
            Thread.currentThread().interrupt();
        }
        return wantMore.get();
    }

    /**
     *
     * @param leftRows
     * @param rightRows
     * @return which side should continue
     * @throws InterruptedException
     */
    private Side consumeRows(List<Object[]> leftRows, List<Object[]> rightRows) throws InterruptedException {
        if (wantMore.get()) {
            int compared = compare(leftRows.get(0), rightRows.get(0));
            if (logger.isTraceEnabled()) {
                logger.trace("consume left: {}, right: {}, compared: {}", Arrays.deepToString(leftRows.toArray()), Arrays.deepToString(rightRows.toArray()), compared);
            }
            if (compared < 0) {
                // left rows are smaller than right, skip to next left set
                removeFromLeft();
                leftCanContinue.signal();
                return Side.LEFT;
            } else if (compared == 0) {
                // both groups have same join conditions
                // NESTEDLOOP FTW
                Outer:
                for (Object[] leftRow : leftRows) {
                    for (Object[] rightRow : rightRows) {
                        if (rowsSkipped.getAndIncrement() < offset) {
                            continue;
                        }
                        boolean downStreamWantsMore = downstream.setNextRow(RowCombinator.combineRow(leftRow, rightRow));
                        if (rowsProduced.incrementAndGet() >= limit || !downStreamWantsMore) {
                            wantMore.set(false);
                            break Outer;
                        }
                    }
                }
                removeFromLeft();
                removeFromRight();
                leftCanContinue.signal();
                rightCanContinue.signal();
            } else {
                // right rows are smaller than left, skip to next right set
                removeFromRight();
                rightCanContinue.signal();
                return Side.RIGHT;
            }
        }
        return Side.BOTH;
    }

    @Override
    public void downstream(Projector downstream) {
        this.downstream = downstream;
        downstream.registerUpstream(this);
    }

    private abstract class InternalProjector implements Projector {

        private final List<Object[]> sameValueRows;
        private final AtomicInteger remainingUpstreams;
        private final CollectExpression[] collectExpressions;

        private InternalProjector(CollectExpression[] collectExpressions) {
            this.remainingUpstreams = new AtomicInteger(0);
            this.sameValueRows = new LinkedList<>();
            this.collectExpressions = collectExpressions;
        }

        private int compareFromSameRelation(Object[] row, Object[] otherRow) {
            Object[] buf = new Object[row.length];
            int i = 0;
            for (CollectExpression collectExpression : collectExpressions) {
                collectExpression.setNextRow(row);
                buf[i] = collectExpression.value();
                i++;
            }
            i = 0;
            Object[] otherBuf = new Object[otherRow.length];
            for (CollectExpression collectExpression : collectExpressions) {
                collectExpression.setNextRow(otherRow);
                otherBuf[i] = collectExpression.value();
                i++;
            }

            int compared = 0;
            for (int j = 0, size=comparators.length; j < size; j++) {
                compared = comparators[j].compare(
                        buf[j],
                        otherBuf[j]
                );
                if (compared != 0) {
                    return compared;
                }
            }
            return compared;
        }

        @Override
        public boolean setNextRow(Object... row) {
            boolean wantMore = true;
            if (sameValueRows.isEmpty()) {
                sameValueRows.add(row);
            } else {
                Object[] lastRow = sameValueRows.get(0);
                int result = compareFromSameRelation(lastRow, row);
                if (result < 0) {
                    // TODO: optimize to not copy
                    wantMore = doSetNextRows(
                            ImmutableList.copyOf(sameValueRows)
                    );
                    sameValueRows.clear();
                    if (wantMore) {
                        sameValueRows.add(row);
                    }

                } else if (result == 0) {
                    sameValueRows.add(row);
                } else {
                    throw new IllegalStateException("source not ordered");
                }
            }
            return wantMore;
        }

        abstract boolean doSetNextRows(List<Object[]> row);

        @Override
        public void startProjection() {
            SortMergeProjector.this.startProjection();
        }

        private void projectionFinished() {
            doSetNextRows(SENTINEL);
        }

        @Override
        public void registerUpstream(ProjectorUpstream upstream) {
            remainingUpstreams.incrementAndGet();
            upstream.downstream(this);
        }

        @Override
        public void upstreamFinished() {
            if (remainingUpstreams.decrementAndGet() == 0) {
                if (!sameValueRows.isEmpty()) {
                    doSetNextRows(ImmutableList.copyOf(sameValueRows));
                    sameValueRows.clear();
                }
                projectionFinished();
            }
        }

        @Override
        public void upstreamFailed(Throwable throwable) {
            onProjectorFailed(throwable);
        }
    }
}
