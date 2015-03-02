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

import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.jobs.ExecutionState;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.RowUpstream;
import io.crate.operation.projectors.NoOpProjector;
import io.crate.operation.projectors.Projector;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Nested Loop Projector that exposes a left and a right Projector
 *
 * basically this is:
 *
 * for (leftRow in leftRows) {
 *     for (rightRow in rightRows) {
 *         downstream.setNextRow ( leftRow + rightRow )
 *     }
 * }
 *
 * left- and rightProjector have to be accessed from different threads as setNextRow() might block
 * until the other side pushes a row or calls upstreamFinished()
 */
public class NestedLoopProjector implements Projector, RowUpstream {

    private final RowDownstreamHandle leftDownstreamHandle;
    private final RowDownstreamHandle rightDownstreamHandle;
    private final ArrayList<Row> innerRows = new ArrayList<>();
    private final CombinedRow combinedRow = new CombinedRow();

    private RowDownstreamHandle downstream = NoOpProjector.INSTANCE;

    private final AtomicBoolean leftFinished = new AtomicBoolean(false);
    private volatile boolean rightFinished = false;
    private final Object finishedLock = new Object();
    private final static Row SENTINEL = new Row() {
        @Override
        public int size() {
            return 0;
        }

        @Override
        public Object get(int index) {
            return null;
        }

        @Override
        public Object[] materialize() {
            return new Object[0];
        }
    };

    private final ArrayBlockingQueue<Row> innerRowsQ = new ArrayBlockingQueue<>(1);

    public NestedLoopProjector() {
        leftDownstreamHandle = new RowDownstreamHandle() {
            @Override
            public boolean setNextRow(Row row) {
                if (rightFinished) {
                    return loopInnerRowAndEmit(row);
                } else {
                    Row innerRow;
                    while (true) {
                        if (rightFinished) {
                            while ((innerRow = innerRowsQ.poll()) != null) {
                                if (innerRow == SENTINEL) {
                                    break;
                                }
                                boolean shouldContinue = emitAndSaveInnerRow(innerRow, row);
                                if (!shouldContinue) {
                                    return false;
                                }
                            }
                            return true;
                        }

                        try {
                            innerRow = innerRowsQ.take();
                            if (innerRow == SENTINEL) {
                                continue;
                            }
                            boolean shouldContinue = emitAndSaveInnerRow(innerRow, row);
                            if (!shouldContinue) {
                                return false;
                            }
                        } catch (InterruptedException e) {
                            fail(e);
                            return false;
                        }
                    }
                }
            }

            private boolean loopInnerRowAndEmit(Row row) {
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
                synchronized (finishedLock) {
                    leftFinished.set(true);
                    if (rightFinished) {
                        downstream.finish();
                    }
                }
            }

            @Override
            public void fail(Throwable throwable) {
                downstream.fail(throwable);
            }
        };

        rightDownstreamHandle = new RowDownstreamHandle() {
            @Override
            public boolean setNextRow(Row row) {
                try {
                    while (true) {
                        boolean added = innerRowsQ.offer(row, 100, TimeUnit.MICROSECONDS);
                        if (added) {
                            return true;
                        } else if (leftFinished.get()) {
                            return true;
                        }
                    }
                } catch (InterruptedException e) {
                    fail(e);
                    return false;
                }
            }

            @Override
            public void finish() {
                synchronized (finishedLock) {
                    rightFinished = true;
                    innerRowsQ.offer(SENTINEL); // unblock .take() in case of race condition
                    if (leftFinished.get()) {
                        downstream.finish();
                    }
                }
            }

            @Override
            public void fail(Throwable throwable) {
                downstream.fail(throwable);
            }
        };
    }

    private boolean emitAndSaveInnerRow(Row innerRow, Row outerRow) {
        if (innerRow instanceof RowN) {
            innerRows.add(innerRow);
        } else {
            innerRows.add(new RowN(innerRow.materialize()));
        }
        combinedRow.outerRow = outerRow;
        combinedRow.innerRow = innerRow;
        return downstream.setNextRow(combinedRow);
    }

    public RowDownstreamHandle leftDownstreamHandle() {
        return leftDownstreamHandle;
    }

    public RowDownstreamHandle rightDownstreamHandle() {
        return rightDownstreamHandle;
    }

    @Override
    public void startProjection(ExecutionState executionState) {
    }

    @Override
    public void downstream(RowDownstream downstream) {
        this.downstream = downstream.registerUpstream(this);
    }

    @Override
    public RowDownstreamHandle registerUpstream(RowUpstream upstream) {
        throw new UnsupportedOperationException("registerUpstream not supported. " +
                "Use leftDownstreamHandle() or rightDownstreamHandle()");
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
    }
}
