/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operator.projectors;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.crate.operator.Input;
import io.crate.operator.aggregation.CollectExpression;
import org.cratedb.Constants;
import org.cratedb.core.collections.ArrayIterator;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * wrapper for concrete implementations
 */
public class SimpleTopNProjector implements Projector {

    /**
     * used when no upstream is set
     */
    static class GatheringTopNRowCollector extends AbstractProjector {

        private final AtomicInteger collected = new AtomicInteger();
        private int endPos;
        private final Object endPosMutex = new Object();

        private final Object[][] result;
        private int resultIdx = 0;
        private final int start;
        private final int end;

        public GatheringTopNRowCollector(Input<?>[] inputs, CollectExpression<?>[] collectExpressions,
                                         int offset, int limit) {
            super(inputs, collectExpressions);
            this.start = offset;
            this.end = start + limit;
            this.result = new Object[end - start][];
        }

        @Override
        public void startProjection() {
            endPos = 0;
            collected.set(0);
        }

        @Override
        public boolean setNextRow(Object... row) {
            int pos = collected.getAndIncrement();
            if (pos >= end) {
                return false;
            } else if (pos < start) {
                // do not collect, still in offset
                return true;
            } else {
                int arrayPos = pos - start;
                Object[] evaluatedRow = new Object[inputs.length];
                for (CollectExpression<?> collectExpression : collectExpressions) {
                    collectExpression.setNextRow(row);
                }

                int i = 0;
                for (Input<?> input : inputs) {
                    evaluatedRow[i++] = input.value();
                }
                result[arrayPos] = evaluatedRow;
                synchronized (endPosMutex) {
                    endPos = Math.max(endPos, arrayPos);
                }
            }
            return true;
        }

        @Override
        public void finishProjection() {
            // ignore
        }

        @Override
        public Object[][] getRows() throws IllegalStateException {
            if (endPos == 0) {
                return Constants.EMPTY_RESULT;
            }
            if (result.length == endPos + 1) {
                return result;
            }
            return Arrays.copyOf(result, endPos + 1);
        }

        @Override
        public Iterator<Object[]> iterator() {
            if (endPos == 0) {
                return Collections.emptyIterator();
            } else {
                return new ArrayIterator(result, 0, endPos+1);
            }
        }
    }


    /**
     * used with upstream - pass rows to upstream if inside <code>start</code> and <code>end</code>
     */
    static class PassThroughTopNRowCollector extends AbstractProjector {
        private final AtomicInteger collected = new AtomicInteger();
        private final int start;
        private final int end;

        public PassThroughTopNRowCollector(Input<?>[] inputs,
                                           CollectExpression<?>[] collectExpressions,
                                           Projector upStream,
                                           int offset, int limit) {
            super(inputs, collectExpressions, upStream);
            this.start = offset;
            this.end = start + limit;
        }

        @Override
        public void startProjection() {
            upStream.get().startProjection();
            collected.set(0);
        }

        @Override
        public boolean setNextRow(Object... row) {
            int pos = collected.getAndIncrement();
            if (pos >= end) {
                return false;
            } else if (pos < start) {
                // do not collect, still in offset
                return true;
            }else {
                return upStream.get().setNextRow(row);
            }
        }

        @Override
        public void finishProjection() {
            upStream.get().finishProjection();
        }

        @Override
        public Object[][] getRows() throws IllegalStateException {
            return new Object[0][];
        }

        @Override
        public Iterator<Object[]> iterator() {
            // Iteration not supported
            return Collections.emptyIterator();
        }
    }

    private Projector wrappedProjector;
    private final Input<?>[] inputs;
    private final CollectExpression<?>[] collectExpressions;
    private final int limit;
    private final int offset;
    private Optional<Projector> upStream;

    public SimpleTopNProjector(Input<?>[] inputs,
                               CollectExpression<?>[] collectExpressions,
                               int limit,
                               int offset) {
        Preconditions.checkArgument(limit >= TopN.NO_LIMIT, "invalid limit");
        Preconditions.checkArgument(offset>=0, "invalid offset");
        this.inputs = inputs;
        this.collectExpressions = collectExpressions;
        if (limit == TopN.NO_LIMIT) {
            limit = Constants.DEFAULT_SELECT_LIMIT;
        }
        this.limit = limit;
        this.offset = offset;
        this.upStream = Optional.absent();
    }

    @Override
    public void setUpStream(Projector upStream) {
        this.upStream = Optional.of(upStream);
    }

    @Override
    public void startProjection() {
        if (upStream.isPresent()) {
            wrappedProjector = new PassThroughTopNRowCollector(inputs, collectExpressions, upStream.get(), offset, limit);
        } else {
            wrappedProjector = new GatheringTopNRowCollector(inputs, collectExpressions, offset, limit);
        }
        wrappedProjector.startProjection();
    }

    @Override
    public synchronized boolean setNextRow(Object[] row) {
        return wrappedProjector.setNextRow(row);
    }

    @Override
    public void finishProjection() {
        wrappedProjector.finishProjection();
    }

    @Override
    public Object[][] getRows() {
        return wrappedProjector.getRows();
    }

    // ITERATOR/ITERABLE

    @Override
    public Iterator<Object[]> iterator() {
        return wrappedProjector.iterator();
    }
}
