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
import io.crate.operation.projectors.RowReceiver;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class NestedLoopOperation implements RowUpstream {

    private final static ESLogger LOGGER = Loggers.getLogger(NestedLoopOperation.class);

    private final CombinedRow combinedRow = new CombinedRow();
    private final LeftRowReceiver leftRowReceiver;
    private final RightRowReceiver rightRowReceiver;
    private final Object mutex = new Object();

    private RowReceiver downstream;
    private volatile boolean leftFinished = false;
    private volatile boolean rightFinished = false;
    private volatile boolean downstreamWantsMore = true;


    public NestedLoopOperation(RowReceiver rowReceiver) {
        this.downstream = rowReceiver;
        downstream.setUpstream(this);
        leftRowReceiver = new LeftRowReceiver();
        rightRowReceiver = new RightRowReceiver();
    }


    public ListenableRowReceiver leftRowReceiver() {
        return leftRowReceiver;
    }

    public ListenableRowReceiver rightRowReceiver() {
        return rightRowReceiver;
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
    }

    private class LeftRowReceiver extends AbstractRowReceiver {

        private volatile Row lastRow = null;

        @Override
        public boolean setNextRow(Row row) {
            LOGGER.trace("LEFT downstream received a row {}", row);

            synchronized (mutex) {
                if (rightFinished && (!rightRowReceiver.receivedRows || !downstreamWantsMore)) {
                    return false;
                }
                lastRow = row;
                upstream.pause();
                rightRowReceiver.leftIsPaused = true;
            }
            return rightRowReceiver.resume();
        }

        @Override
        public void finish() {
            LOGGER.trace("LEFT downstream finished");
            synchronized (mutex) {
                leftFinished = true;
                finished.set(null);
                if (rightFinished) {
                    downstream.finish();
                } else {
                    rightRowReceiver.upstream.resume(false);
                }
            }
        }

        @Override
        public void fail(Throwable throwable) {
            downstream.fail(throwable);
            finished.setException(throwable);
        }

        @Override
        public boolean requiresRepeatSupport() {
            return false;
        }
    }

    private class RightRowReceiver extends AbstractRowReceiver {

        volatile Row lastRow = null;

        boolean receivedRows = false;
        boolean leftIsPaused = false;

        @Override
        public boolean setNextRow(final Row rightRow) {
            LOGGER.trace("RIGHT downstream received a row {}", rightRow);
            receivedRows = true;


            if (leftIsPaused) {
                return emitRow(rightRow);
            }

            synchronized (mutex) {
                if (leftRowReceiver.lastRow == null) {
                    if (leftFinished) {
                        return false;
                    }
                    lastRow = new RowN(rightRow.materialize());
                    upstream.pause();
                    return true;
                }
            }
            return emitRow(rightRow);
        }

        private boolean emitRow(Row row) {
            combinedRow.outerRow = leftRowReceiver.lastRow;
            combinedRow.innerRow = row;
            boolean wantsMore = downstream.setNextRow(combinedRow);
            downstreamWantsMore = wantsMore;
            return wantsMore;
        }

        @Override
        public void finish() {
            LOGGER.trace("RIGHT downstream finished");
            synchronized (mutex) {
                rightFinished = true;
                finished.set(null);
                if (leftFinished) {
                    downstream.finish();
                } else {
                    leftRowReceiver.upstream.resume(false);
                }
            }
        }

        @Override
        public void fail(Throwable throwable) {
            downstream.fail(throwable);
            finished.setException(throwable);
        }

        @Override
        public boolean requiresRepeatSupport() {
            return true;
        }

        boolean resume() {
            if (lastRow != null) {
                boolean wantMore = emitRow(lastRow);
                if (!wantMore) {
                    return false;
                }
                lastRow = null;
            } else {
                leftRowReceiver.lastRow = new RowN(leftRowReceiver.lastRow.materialize());
            }

            if (rightFinished) {
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
}
