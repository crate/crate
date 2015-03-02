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

import io.crate.operation.ProjectorUpstream;
import io.crate.operation.projectors.Projector;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
public class NestedLoopProjector implements ProjectorUpstream {

    private final InternalProjector leftProjector;
    private final InternalProjector rightProjector;
    private final ArrayList<Object[]> innerRows;
    private Projector downstream;
    private final AtomicBoolean leftFinished = new AtomicBoolean(false);
    private volatile boolean rightFinished = false;
    private final Object finishedLock = new Object();
    private final static Object[] SENTINEL = new Object[0];

    private final ArrayBlockingQueue<Object[]> innerRowsQ = new ArrayBlockingQueue<>(1);

    public NestedLoopProjector() {
        innerRows = new ArrayList<>();

        // TODO: set downstream to NoOpProjector

        leftProjector = new InternalProjector() {

            @Override
            public void upstreamFailed(Throwable throwable) {
                downstream.upstreamFailed(throwable);
            }

            @Override
            protected void doUpstreamsFinished() {
                synchronized (finishedLock) {
                    leftFinished.set(true);
                    if (rightFinished) {
                        downstream.upstreamFinished();
                    }
                }
            }

            @Override
            public boolean setNextRow(Object... row) {
                if (rightFinished) {
                    return loopInnerRowAndEmit(row);
                } else {
                    Object[] innerRow;
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
                            upstreamFailed(e);
                            return false;
                        }
                    }
                }
            }

            private boolean loopInnerRowAndEmit(Object[] row) {
                for (Object[] innerRow : innerRows) {
                    boolean shouldContinue = downstream.setNextRow(RowCombinator.combineRow(innerRow, row));
                    if (!shouldContinue) {
                        return false;
                    }
                }
                return true;
            }
        };

        rightProjector = new InternalProjector() {

            @Override
            public void upstreamFailed(Throwable throwable) {
                downstream.upstreamFailed(throwable);
            }

            @Override
            protected void doUpstreamsFinished() {
                synchronized (finishedLock) {
                    rightFinished = true;
                    innerRowsQ.offer(SENTINEL); // unblock .take() in case of race condition
                    if (leftFinished.get()) {
                        downstream.upstreamFinished();
                    }
                }
            }

            @Override
            public boolean setNextRow(Object... row) {
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
                    upstreamFailed(e);
                    return false;
                }
            }
        };
    }

    private boolean emitAndSaveInnerRow(Object[] innerRow, Object[] outerRow) {
        innerRows.add(innerRow);
        return downstream.setNextRow(RowCombinator.combineRow(innerRow, outerRow));
    }

    public void startProjection() {
        downstream.startProjection();;
        leftProjector.startProjection();
        rightProjector.startProjection();
    }

    public Projector leftProjector() {
        return leftProjector;
    }

    public Projector rightProjector() {
        return rightProjector;
    }

    @Override
    public void downstream(Projector downstream) {
        this.downstream = downstream;
        downstream.registerUpstream(this);
    }

    private abstract static class InternalProjector implements Projector {

        private final AtomicInteger numUpstreams = new AtomicInteger(0);

        @Override
        public void startProjection() {
        }

        @Override
        public void registerUpstream(ProjectorUpstream upstream) {
            numUpstreams.incrementAndGet();
        }

        @Override
        public void upstreamFinished() {
            if (numUpstreams.decrementAndGet() <= 0) {
                doUpstreamsFinished();
            }
        }

        protected abstract void doUpstreamsFinished();
    }
}
