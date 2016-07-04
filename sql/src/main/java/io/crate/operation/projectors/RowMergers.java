/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.projectors;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import io.crate.core.collections.ArrayRow;
import io.crate.core.collections.Row;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowUpstream;
import io.crate.operation.collect.collectors.TopRowUpstream;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class RowMergers {

    private RowMergers() {}

    public static RowDownstream passThroughRowMerger(RowReceiver delegate) {
        return new MultiUpstreamRowReceiver(delegate);
    }

    /**
     * Acts as a bridge from multiple RowUpstreams to a single RowReceiver
     *
     *
     * <pre>
     *      +----+     +----+
     *      | U1 |     | U2 |
     *      +----+     +----+
     *          \        /
     *           \      /
     *          +-----------+
     *          | RowMerger |
     *          +-----------+
     *                |
     *                |
     *          +-------------+
     *          | RowReceiver |
     *          +-------------+
     * </pre>
     */
    static class MultiUpstreamRowReceiver implements RowReceiver, RowMerger {

        private static final ESLogger LOGGER = Loggers.getLogger(MultiUpstreamRowReceiver.class);

        final RowReceiver delegate;
        final Set<RowUpstream> rowUpstreams = Sets.newConcurrentHashSet();
        private final AtomicInteger activeUpstreams = new AtomicInteger(0);
        private final AtomicBoolean prepared = new AtomicBoolean(false);
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final Object lock = new Object();

        final Queue<Object[]> pauseFifo = new LinkedList<>();
        final ArrayRow sharedRow = new ArrayRow();

        volatile boolean paused = false;
        private boolean downstreamFinished = false;

        /**
         * Used in {@link #resume(boolean)} when rows are emitted from the pauseFifo.
         * This is used to have proper pause/resume locking
         */
        final TopRowUpstream topRowUpstream = new TopRowUpstream(MoreExecutors.directExecutor(),
            new Runnable() { // resumeRunnable
                @Override
                public void run() {
                    delegate.setUpstream(MultiUpstreamRowReceiver.this);
                    resume(false);
                }
            },
            new Runnable() { // repeatRunnable
                @Override
                public void run() {
                    // this can only happen after finish in which case the MultiUpstreamRowReceiver itself should be the
                    // upstream and not the topRowUpstream
                    throw new AssertionError("repeat shouldn't be called on TopRowUpstream");
                }
            }
        );

        MultiUpstreamRowReceiver(RowReceiver delegate) {
            delegate.setUpstream(this);
            this.delegate = delegate;
        }

        @Override
        public final boolean setNextRow(Row row) {
            if (downstreamFinished) {
                return false;
            }
            synchronized (lock) {
                boolean wantMore = synchronizedSetNextRow(row);
                if (!wantMore) {
                    pauseFifo.clear();
                    return false;
                }
                return true;
            }
        }

        /**
         * pause handling is tricky:
         *
         *
         * <pre>
         * u1:                                              u2:
         *  rw.setNextRow(r1)                                rw.setNextRow(r2)
         *      synchronized:                                   synchronized: // < blocked until u1 is done
         *          [...]
         *          delegate.setNextRow()
         *                  [...]
         *                  upstream.pause()
         *                  [...]
         *                  return
         *      return                                         // unblocks but already *within* setNextRow
         *  u1 pauses                                          if (paused) {
         *                                                         pauseFifo.add(row.materialize())
         *                                                         return
         *                                                     }
         *                                                     u2 pauses
         *
         *  </pre>
         */
        boolean synchronizedSetNextRow(Row row) {
            if (paused) {
                pauseFifo.add(row.materialize());
                return true;
            } else {
                assert pauseFifo.isEmpty()
                    : "resume should consume pauseFifo first before delegating resume to upstreams";
                return delegate.setNextRow(row);
            }
        }

        @Override
        public final void finish() {
            countdown();
        }

        @Override
        public final void fail(Throwable throwable) {
            failure.set(throwable);
            countdown();
        }

        /**
         * triggered if the last remaining upstream finished or failed
         */
        void onFinish() {
            assert !paused : "must not receive a finish call if upstream should be paused";
            assert pauseFifo.isEmpty() : "pauseFifo should be clear already";
            delegate.finish();
        }

        /**
         * triggered if the last remaining upstream finished or failed
         */
        void onFail(Throwable t) {
            delegate.fail(t);
        }

        @Override
        public void prepare() {
            if (prepared.compareAndSet(false, true)) {
                delegate.prepare();
            }
        }

        @Override
        public void kill(Throwable throwable) {
            delegate.kill(throwable);
        }

        @Override
        public Set<Requirement> requirements() {
            return delegate.requirements();
        }

        @Override
        public void setUpstream(RowUpstream rowUpstream) {
            if (!rowUpstreams.add(rowUpstream)) {
                LOGGER.debug("Upstream {} registered itself twice", rowUpstream);
            }
        }

        @Override
        public void pause() {
            paused = true;
            for (RowUpstream rowUpstream : rowUpstreams) {
                rowUpstream.pause();
            }
        }

        @Override
        public void resume(boolean async) {
            synchronized (lock) {
                paused = false;
                if (!pauseFifo.isEmpty()) {
                    // clear pauseFifo first, otherwise it could grow very large
                    delegate.setUpstream(topRowUpstream);
                    Object[] row;
                    while ((row = pauseFifo.poll()) != null) {
                        sharedRow.cells(row);
                        boolean wantMore = delegate.setNextRow(sharedRow);
                        if (topRowUpstream.shouldPause()) {
                            delegate.setUpstream(this);
                            topRowUpstream.pauseProcessed();
                            return;
                        }
                        if (!wantMore) {
                            downstreamFinished = true;
                            pauseFifo.clear();
                            // resume upstreams anyway so that they can finish and cleanup resources
                            // they'll receive false on the next setNextRow call
                            break;
                        }
                    }
                    delegate.setUpstream(this);
                }
            }
            for (RowUpstream rowUpstream : rowUpstreams) {
                rowUpstream.resume(async);
            }
        }

        private void countdown() {
            int remainingUpstreams = activeUpstreams.decrementAndGet();
            assert remainingUpstreams >= 0 : "activeUpstreams must not get negative: " + remainingUpstreams;
            if (remainingUpstreams == 0) {
                Throwable t = failure.get();
                if (t == null) {
                    onFinish();
                } else {
                    onFail(t);
                }
            }
        }

        public void repeat() {
            if (activeUpstreams.compareAndSet(0, rowUpstreams.size())) {
                pauseFifo.clear();
                for (RowUpstream rowUpstream : rowUpstreams) {
                    rowUpstream.repeat();
                }
            } else {
                throw new IllegalStateException("Can't repeat if there are still active upstreams");
            }
        }

        @Override
        public RowReceiver newRowReceiver() {
            activeUpstreams.incrementAndGet();
            return this;
        }
    }
}
