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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.RowUpstream;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class NestedLoopOperation implements RowUpstream, RowDownstream {

    private final static ESLogger LOGGER = Loggers.getLogger(NestedLoopOperation.class);

    private final CombinedRow combinedRow = new CombinedRow();
    private final LeftDownstreamHandle leftDownstreamHandle;
    private final RightDownstreamHandle rightDownstreamHandle;
    private final Object mutex = new Object();
    private final AtomicInteger numUpstreams = new AtomicInteger(0);

    private RowUpstream leftUpstream;
    private RowUpstream rightUpstream;

    private RowDownstreamHandle downstream;
    private volatile boolean leftFinished = false;
    private volatile boolean rightFinished = false;
    private List<Row> innerRows = new ArrayList<>();


    public NestedLoopOperation() {
        leftDownstreamHandle = new LeftDownstreamHandle();
        rightDownstreamHandle = new RightDownstreamHandle();
    }


    @Override
    public RowDownstreamHandle registerUpstream(RowUpstream upstream) {
        if (numUpstreams.incrementAndGet() == 1) {
            leftUpstream = upstream;
            return leftDownstreamHandle;
        } else {
            assert numUpstreams.get() <= 2: "Only 2 upstreams supported";
            rightUpstream = upstream;
            return rightDownstreamHandle;
        }
    }

    public void downstream(RowDownstream downstream) {
        this.downstream = downstream.registerUpstream(this);
    }

    @Override
    public void pause() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resume() {
        throw new UnsupportedOperationException();
    }

    static class CombinedRow implements Row {

        Row outerRow;
        Row innerRow;

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

    private class LeftDownstreamHandle implements RowDownstreamHandle {

        @Override
        public boolean setNextRow(Row row) {
            LOGGER.trace("left downstream received a row {}", row);

            if (rightFinished) {
                return loopInnerRowsAndEmit(row);
            } else if (rightDownstreamHandle.leftRow == null) {
                synchronized (mutex) {
                    rightDownstreamHandle.leftRowFuture.set(materializeRow(row));
                }
                rightUpstream.resume();
                if (!rightFinished) {
                    leftUpstream.pause();
                }
                return true;
            } else {
                throw new IllegalStateException("Shouldn't receive any more rows if paused");
            }
        }

        private boolean loopInnerRowsAndEmit(Row row) {
            for (Row innerRow : innerRows) {
                combinedRow.outerRow = row;
                combinedRow.innerRow = innerRow;
                boolean shouldContinue = downstream.setNextRow(combinedRow);
                if (!shouldContinue) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public void finish() {
            LOGGER.trace("left downstream finished");
            synchronized (mutex) {
                leftFinished = true;
                if (rightFinished) {
                    downstream.finish();
                } else {
                    rightUpstream.resume();
                }
            }
        }

        @Override
        public void fail(Throwable throwable) {
            downstream.fail(throwable);
        }
    }

    private class RightDownstreamHandle implements RowDownstreamHandle {

        public SettableFuture<Row> leftRowFuture = SettableFuture.create();
        private Row leftRow = null;


        public RightDownstreamHandle() {
            Futures.addCallback(leftRowFuture, new FutureCallback<Row>() {
                @Override
                public void onSuccess(@Nullable Row result) {
                    leftRow = result;
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                }
            });
        }

        @Override
        public boolean setNextRow(final Row rightRow) {
            LOGGER.trace("right downstream received a row {}", rightRow);

            synchronized (mutex) {
                if (leftFinished) {
                    if (leftRow == null) {
                        return false;
                    }
                    return emitRow(rightRow);
                }
                if (leftRow == null) {
                    Futures.addCallback(leftRowFuture, new FutureCallback<Row>() {
                                @Override
                                public void onSuccess(Row leftRow) {
                                    emitAndCacheRow(rightRow);
                                }

                                @Override
                                public void onFailure(@Nonnull Throwable t) {
                                }
                            });

                    rightUpstream.pause();
                    return true;
                }
            }

            return emitAndCacheRow(rightRow);
        }

        private boolean emitAndCacheRow(Row row) {
            if (!emitRow(row)) return false;

            innerRows.add(materializeRow(row));
            return true;
        }

        private boolean emitRow(Row row) {
            combinedRow.outerRow = leftRow;
            combinedRow.innerRow = row;

            return downstream.setNextRow(combinedRow);
        }

        @Override
        public void finish() {
            LOGGER.trace("right downstream finished");

            boolean resumeLeft = false;
            synchronized (mutex) {
                rightFinished = true;
                if (leftFinished) {
                    downstream.finish();
                } else {
                    resumeLeft = true;
                }
            }
            if (resumeLeft) {
                LOGGER.trace("right finished, resuming left");
                leftUpstream.resume();
            }
        }

        @Override
        public void fail(Throwable throwable) {
            downstream.fail(throwable);
        }
    }


    private static Row materializeRow(Row row) {
        if (row instanceof RowN) {
            return row;
        } else {
            return new RowN(row.materialize());
        }
    }
}
