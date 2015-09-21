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

package io.crate.operation.join;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.jobs.ExecutionState;
import io.crate.operation.RowUpstream;
import io.crate.operation.projectors.ListenableRowReceiver;
import io.crate.operation.projectors.Requirement;
import io.crate.operation.projectors.Requirements;
import io.crate.operation.projectors.RowReceiver;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class NestedLoopOperation implements RowUpstream {

    private final static ESLogger LOGGER = Loggers.getLogger(NestedLoopOperation.class);

    private final LeftRowReceiver left;
    private final RightRowReceiver right;

    private RowReceiver downstream;
    private volatile boolean downstreamWantsMore = true;

    /**
     * state of the left and right side.
     *
     * LEAD_ELECTION is the initial state.
     *
     * If any side receives a row the other side can be in one of the following 3 states:
     *
     *    lead_election     (hasn't received a row yet, or is just about to receive one)
     *    paused            (this state can only be changed by the side that is currently receiving a row)
     *    finished          (this won't change anymore - but the right side can still receive rows if it is repeating)
     *
     * the lead_election state is only seen in the beginning until one side has has taken charge and set its next state.
     * After that one side will always be paused and the other one will receive rows.
     */
    private enum State {
        LEAD_ELECTION,
        PAUSED,
        FINISHED
    }

    private final AtomicBoolean leadAcquired = new AtomicBoolean(false);

    public NestedLoopOperation(RowReceiver rowReceiver) {
        this.downstream = rowReceiver;
        downstream.setUpstream(this);
        left = new LeftRowReceiver();
        right = new RightRowReceiver();
    }

    public ListenableRowReceiver leftRowReceiver() {
        return left;
    }

    public ListenableRowReceiver rightRowReceiver() {
        return right;
    }

    @Override
    public void pause() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resume(boolean async) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void repeat() {
        throw new UnsupportedOperationException();
    }

    static class CombinedRow implements Row {

        volatile Row outerRow;
        volatile Row innerRow;

        @Override
        public int size() {
            return outerRow.size() + innerRow.size();
        }

        @Override
        public Object get(int index) {
            if (index < outerRow.size()) {
                return outerRow.get(index);
            }
            return innerRow.get(index - outerRow.size());
        }

        @Override
        public Object[] materialize() {
            Object[] left = outerRow.materialize();
            Object[] right = innerRow.materialize();

            Object[] newRow = new Object[left.length + right.length];
            System.arraycopy(left, 0, newRow, 0, left.length);
            System.arraycopy(right, 0, newRow, left.length, right.length);
            return newRow;
        }

        @Override
        public String toString() {
            return "CombinedRow{" +
                    " outer=" + outerRow +
                    ", inner=" + innerRow +
                    '}';
        }
    }

    private abstract class AbstractRowReceiver implements ListenableRowReceiver {

        final SettableFuture<Void> finished = SettableFuture.create();
        final AtomicReference<State> state = new AtomicReference<>(State.LEAD_ELECTION);

        volatile RowUpstream upstream;

        @Override
        public ListenableFuture<Void> finishFuture() {
            return finished;
        }

        @Override
        public void prepare(ExecutionState executionState) {
        }

        @Override
        public void setUpstream(RowUpstream rowUpstream) {
            assert rowUpstream != null : "rowUpstream must not be null";
            this.upstream = rowUpstream;
        }

        private void finishThisSide() {
            finished.set(null);
            state.set(State.FINISHED);
        }

        protected void finish(AtomicReference<State> otherStateRef, RowUpstream otherUpstream) {
            State otherState = otherStateRef.get();
            if (otherState == State.LEAD_ELECTION) {
                // other side hasn't received any rows
                if (leadAcquired.compareAndSet(false, true)) {
                    finishThisSide();
                    return;
                } else {
                    otherState = waitForStateChange(otherStateRef);
                }
            }
            finishThisSide();
            if (otherState == State.FINISHED) {
                downstream.finish();
            } else {
                otherUpstream.resume(false);
            }
        }
    }

    private class LeftRowReceiver extends AbstractRowReceiver {

        private Row lastRow = null;

        @Override
        public boolean setNextRow(Row row) {
            LOGGER.trace("LEFT downstream received a row {}", row);

            State rightState = right.state.get();
            switch (rightState) {
                case LEAD_ELECTION:
                    if (leadAcquired.compareAndSet(false, true)) {
                        lastRow = row;
                        state.set(State.PAUSED);
                        upstream.pause();
                        return true;
                    } else {
                        rightState = waitForStateChange(right.state);
                        return handlePauseOrFinished(row, rightState);
                    }
                default:
                    return handlePauseOrFinished(row, rightState);
            }
        }

        private boolean handlePauseOrFinished(Row row, State rightState) {
            switch (rightState) {
                case PAUSED:
                    lastRow = row;
                    state.set(State.PAUSED);
                    upstream.pause();
                    return right.resume(rightState);
                case FINISHED:
                    //noinspection SimplifiableIfStatement
                    if (right.receivedRows && downstreamWantsMore) {
                        lastRow = row;
                        state.set(State.PAUSED);
                        upstream.pause();
                        return right.resume(rightState);
                    }
                    return false;
                default:
                    throw new AssertionError("lead election state should have been handled already");
            }
        }

        @Override
        public void finish() {
            LOGGER.trace("LEFT downstream finished");
            finish(right.state, right.upstream);
        }

        @Override
        public void fail(Throwable throwable) {
            downstream.fail(throwable);
            finished.setException(throwable);
        }

        @Override
        public Set<Requirement> requirements() {
            return Requirements.add(downstream.requirements(), Requirement.REPEAT);
        }
    }

    private class RightRowReceiver extends AbstractRowReceiver {

        private final CombinedRow combinedRow = new CombinedRow();
        private final Set<Requirement> requirements;

        Row lastRow = null;
        boolean receivedRows = false;

          // local non-volatile variable for faster access for the usual case
        boolean leftIsSuspended = false;

        public RightRowReceiver() {
            requirements = Requirements.add(downstream.requirements(), Requirement.REPEAT);
        }

        @Override
        public boolean setNextRow(final Row rightRow) {
            LOGGER.trace("RIGHT downstream received a row {}", rightRow);

            receivedRows = true;
            if (leftIsSuspended) {
                return emitRow(rightRow);
            }

            State leftState = left.state.get();
            switch (leftState) {
                case LEAD_ELECTION:
                    if (leadAcquired.compareAndSet(false, true)) {
                        state.set(State.PAUSED);
                        lastRow = rightRow;
                        upstream.pause();
                        return true;
                    } else {
                        return handlePauseOrFinished(rightRow, waitForStateChange(left.state));
                    }
                default:
                    return handlePauseOrFinished(rightRow, leftState);
            }
        }

        private boolean handlePauseOrFinished(Row rightRow, State leftState) {
            switch (leftState) {
                case FINISHED:
                    if (left.lastRow == null) {
                        // left never received a row
                        return false;
                    }
                case PAUSED:
                    leftIsSuspended = true;
                    return emitRow(rightRow);
                default:
                    throw new AssertionError("There are only 3 different states");
            }
        }

        private boolean emitRow(Row row) {
            combinedRow.outerRow = left.lastRow;
            combinedRow.innerRow = row;
            boolean wantsMore = downstream.setNextRow(combinedRow);
            downstreamWantsMore = wantsMore;
            return wantsMore;
        }

        @Override
        public void finish() {
            LOGGER.trace("RIGHT downstream finished");

            leftIsSuspended = false;
            finish(left.state, left.upstream);
        }

        @Override
        public void fail(Throwable throwable) {
            downstream.fail(throwable);
            finished.setException(throwable);
        }

        @Override
        public Set<Requirement> requirements() {
            return requirements;
        }

        boolean resume(State rightState) {
            if (lastRow != null) {
                boolean wantMore = emitRow(lastRow);
                if (!wantMore) {
                    return false;
                }
                lastRow = null;
            } else {
                left.lastRow = new RowN(left.lastRow.materialize());
            }

            if (rightState == State.FINISHED) {
                if (receivedRows) {
                    upstream.repeat();
                } else {
                    return false;
                }
            } else {
                upstream.resume(false);
            }
            return true;
        }
    }

    private State waitForStateChange(AtomicReference<State> stateRef) {
        // loop shouldn't take long as other side has just acquired the lead and is about to change the state
        State state;
        do {
            state = stateRef.get();
        } while (state == State.LEAD_ELECTION);
        return state;
    }
}
