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

package io.crate.operation.fetch;

import io.crate.core.collections.Row;
import io.crate.jobs.ExecutionState;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowUpstream;
import io.crate.operation.projectors.Requirement;
import io.crate.operation.projectors.Requirements;
import io.crate.operation.projectors.RowReceiver;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Merge rows of multiple upstreams ordered by a positional unique long. Rows are emitted as
 * soon as possible. Every upstream gets its own {@link RowReceiver}, this handle is
 * not operating thread-safe, but the merger itself is thread-safe. The main purpose of this
 * implementation is merging ordered shard rows on a node.
 */
public class PositionalRowMerger implements RowDownstream, RowUpstream {

    private final RowReceiver downstream;
    private final AtomicInteger upstreamsRemaining = new AtomicInteger(0);
    private final List<UpstreamBuffer> upstreamBuffers = new ArrayList<>();
    private final int orderingColumnIndex;
    private volatile int outputCursor = 0;
    private volatile int leastUpstreamBufferCursor = -1;
    private volatile int leastUpstreamBufferId = -1;
    private final AtomicBoolean consumeRows = new AtomicBoolean(true);

    public PositionalRowMerger(RowReceiver downstream, int orderingColumnIndex) {
        this.downstream = downstream;
        downstream.setUpstream(this);
        this.orderingColumnIndex = orderingColumnIndex;
    }

    private synchronized boolean emitRows() {
        if (leastUpstreamBufferId == -1 || leastUpstreamBufferCursor != outputCursor) {
            findLeastUpstreamBufferId();
        }

        while (leastUpstreamBufferCursor == outputCursor && leastUpstreamBufferId != -1) {
            if (!emitRow(upstreamBuffers.get(leastUpstreamBufferId).poll())) {
                return false;
            }
            if (upstreamsRemaining.get() > 0) {
                findLeastUpstreamBufferId();
            }
        }

        return true;
    }

    private void findLeastUpstreamBufferId() {
        for (int i = 0; i < upstreamBuffers.size(); i++) {
            UpstreamBuffer upstreamBuffer = upstreamBuffers.get(i);
            if (upstreamBuffer.size() == 0) {
                continue;
            }
            try {
                Row row = upstreamBuffer.first();
                int orderingValue = (int)row.get(orderingColumnIndex);
                if (orderingValue == outputCursor) {
                    leastUpstreamBufferCursor = orderingValue;
                    leastUpstreamBufferId = i;
                    return;
                } else if (orderingValue <= leastUpstreamBufferCursor) {
                    leastUpstreamBufferCursor = orderingValue;
                    leastUpstreamBufferId = i;
                }
            } catch (NoSuchElementException e) {
                // continue
            }
        }
    }

    private synchronized boolean emitRow(Row row) {
        outputCursor++;
        return downstream.setNextRow(row);
    }


    @Override
    public RowReceiver newRowReceiver() {
        upstreamsRemaining.incrementAndGet();
        UpstreamBuffer upstreamBuffer = new UpstreamBuffer(this);
        upstreamBuffers.add(upstreamBuffer);
        return upstreamBuffer;
    }

    private void finish() {
        if (upstreamsRemaining.decrementAndGet() <= 0) {
            downstream.finish();
        }
    }

    private void fail(Throwable throwable) {
        consumeRows.set(false);
        downstream.fail(throwable);
    }

    @Override
    public void pause() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resume(boolean async) {
        throw new UnsupportedOperationException();
    }

    /**
     * tells the RowUpstream that it should push all rows again
     */
    @Override
    public void repeat() {
        throw new UnsupportedOperationException();
    }

    static class UpstreamBuffer implements RowReceiver {

        private final LinkedList<Row> rows = new LinkedList<>();
        private final PositionalRowMerger merger;
        private final Object lock = new Object();

        public UpstreamBuffer(PositionalRowMerger merger) {
            this.merger = merger;
        }

        @Override
        public boolean setNextRow(Row row) {
            if (!merger.consumeRows.get()) {
                return false;
            }
            if ((int)row.get(merger.orderingColumnIndex) == merger.outputCursor) {
                if (!merger.emitRow(row)) {
                    return false;
                }
            } else {
                synchronized (lock) {
                    rows.add(row);
                }
            }
            return merger.emitRows();
        }

        @Override
        public void finish() {
            merger.finish();
        }

        @Override
        public void fail(Throwable throwable) {
            merger.fail(throwable);
        }

        public Row poll() {
            synchronized (lock) {
                return rows.poll();
            }
        }

        public Row first() {
            synchronized (lock) {
                return rows.getFirst();
            }
        }

        public int size() {
            synchronized (lock) {
                return rows.size();
            }
        }

        @Override
        public void prepare(ExecutionState executionState) {

        }

        @Override
        public Set<Requirement> requirements() {
            return Requirements.NO_REQUIREMENTS;
        }

        @Override
        public void setUpstream(RowUpstream rowUpstream) {

        }
    }
}
