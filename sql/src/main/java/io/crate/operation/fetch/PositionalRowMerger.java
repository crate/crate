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
import io.crate.operation.RowDownstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.RowUpstream;
import io.crate.operation.projectors.Projector;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Merge rows of multiple upstreams ordered by a positional unique long.
 * Every upstream gets its own {@link RowDownstreamHandle},
 * this handle is not operating thread-safe, but the merger itself is thread-safe.
 * Rows are emitted as soon as possible. Main purpose of this implementation is merging ordered
 * shard rows on a node.
 */
public class PositionalRowMerger implements Projector, RowDownstreamHandle {

    private RowDownstreamHandle downstream;
    private final AtomicInteger upstreamsRemaining = new AtomicInteger(0);
    private final List<UpstreamBuffer> upstreamBuffers = new ArrayList<>();
    private final int orderingColumnIndex;
    private volatile long outputCursor = 0;
    private volatile long leastUpstreamBufferCursor = -1;
    private volatile int leastUpstreamBufferId = -1;
    private final AtomicBoolean consumeRows = new AtomicBoolean(true);

    public PositionalRowMerger(RowDownstream downstream, int orderingColumnIndex) {
        downstream(downstream);
        this.orderingColumnIndex = orderingColumnIndex;
    }

    @Override
    public boolean setNextRow(Row row) {
        throw new UnsupportedOperationException("PositionalRowMerger does not support setNextRow");
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
            try {
                Row row = upstreamBuffers.get(i).first();
                Long orderingValue = (Long)row.get(orderingColumnIndex);
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
    public void startProjection() {
    }

    @Override
    public void downstream(RowDownstream downstream) {
        this.downstream = downstream.registerUpstream(this);
    }

    @Override
    public RowDownstreamHandle registerUpstream(RowUpstream upstream) {
        upstreamsRemaining.incrementAndGet();
        UpstreamBuffer upstreamBuffer = new UpstreamBuffer(this);
        upstreamBuffers.add(upstreamBuffer);
        return upstreamBuffer;
    }

    @Override
    public void finish() {
        if (upstreamsRemaining.decrementAndGet() <= 0) {
            downstream.finish();
        }
    }

    @Override
    public void fail(Throwable throwable) {
        consumeRows.set(false);
        downstream.fail(throwable);
    }

    static class UpstreamBuffer implements RowDownstreamHandle {

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
            if (row.get(merger.orderingColumnIndex) == merger.outputCursor) {
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
    }
}
