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

import com.amazonaws.annotation.ThreadSafe;
import com.google.common.collect.ImmutableList;
import io.crate.core.collections.ArrayRow;
import io.crate.core.collections.Row;
import io.crate.operation.RowDownstream;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * RowDownstream/RowReceiver that acts as a bridge from multiple upstreams to a single RowReceiver.
 *
 * Can handle upstreams which emit to this RowReceiver concurrently.
 *
 *      +----+     +----+
 *      | U1 |     | U2 |
 *      +----+     +----+
 *          \        /
 *           \      /
 *      +---------------------------+
 *      | synchronized              |
 *      | MultiUpstreamRowReceiver  |
 *      +---------------------------+
 *                |
 *                |
 *          +-------------+
 *          | RowReceiver |
 *          +-------------+
 */
@ThreadSafe
class MultiUpstreamRowReceiver implements RowReceiver, RowDownstream {

    private static final ESLogger LOGGER = Loggers.getLogger(MultiUpstreamRowReceiver.class);

    final RowReceiver delegate;
    private final List<ResumeHandle> resumeHandles = Collections.synchronizedList(new ArrayList<ResumeHandle>());
    private final AtomicInteger activeUpstreams = new AtomicInteger(0);
    private final AtomicBoolean prepared = new AtomicBoolean(false);
    private final AtomicBoolean pauseTriggered = new AtomicBoolean(false);
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final Object lock = new Object();

    private final Queue<Object[]> pauseFifo = new LinkedList<>();
    private final ArrayRow sharedRow = new ArrayRow();

    private boolean downstreamFinished = false;
    private boolean paused = false;

    MultiUpstreamRowReceiver(RowReceiver delegate) {
        this.delegate = delegate;
    }

    @Override
    public final Result setNextRow(Row row) {
        if (downstreamFinished) {
            return Result.STOP;
        }
        synchronized (lock) {
            Result result = synchronizedSetNextRow(row);
            switch (result) {
                case CONTINUE:
                    return result;
                case PAUSE:
                    paused = true;
                    // delegate.pauseProcessed is called in pauseProcessed of the RowMerger
                    return result;
                case STOP:
                    downstreamFinished = true;
                    pauseFifo.clear();
                    return result;
            }
            throw new AssertionError("Unrecognized setNextRow result: " + result);
        }
    }

    @Override
    public void pauseProcessed(ResumeHandle resumeable) {
        resumeHandles.add(resumeable);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("pauseProcessed num={}/{}", resumeHandles.size(), activeUpstreams.get());
        }
        if (resumeHandles.size() == activeUpstreams.get()) {
            triggerPauseProcessed();
        }
    }

    private void triggerPauseProcessed() {
        if (pauseTriggered.compareAndSet(false, true)) {
            final ImmutableList<ResumeHandle> resumeHandles = ImmutableList.copyOf(this.resumeHandles);
            this.resumeHandles.clear();
            delegate.pauseProcessed(new MultiResumeHandle(resumeHandles));
        }
    }

    /**
     * pause handling is tricky:
     *
     *
     * u1:                                              u2:
     *  rw.setNextRow(r1)                                rw.setNextRow(r2)
     *      synchronized:                                   synchronized: // < blocked until u1 is done
     *          [...]
     *          delegate.setNextRow()
     *             [...]
     *             return PAUSE
     *      return                                         // unblocks but already *within* setNextRow
     *  u1 pauses                                          if (paused) {
     *                                                         pauseFifo.add(row.materialize())
     *                                                         return
     *                                                     }
     *                                                     u2 pauses
     */
    private Result synchronizedSetNextRow(Row row) {
        if (paused) {
            pauseFifo.add(row.materialize());
            return Result.PAUSE;
        } else {
            assert pauseFifo.size() == 0
                : "resume should consume pauseFifo first before delegating resume to upstreams";
            return delegate.setNextRow(row);
        }
    }

    @Override
    public final void finish(RepeatHandle repeatHandle) {
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
    private void onFinish() {
        assert !paused : "must not receive a finish call if upstream should be paused";
        assert pauseFifo.isEmpty() : "pauseFifo should be clear already";

        /*
         * Repeat wouldn't emit the rows in the same order because the upstreams run multi-threaded
         * E.g. 2 shards with 2 rows each:
         *
         *  S1        S2
         *  1          3
         *  2          4
         *
         *  The first run could emit
         *
         *  3 - 1 - 2 - 4
         *
         *  and a second run could emit
         *
         *  1 - 2 - 3 - 4
         *
         *  and a third:
         *
         *  3 - 4 - 1 - 2
         */
        delegate.finish(RepeatHandle.UNSUPPORTED);
    }

    /**
     * triggered if the last remaining upstream finished or failed
     */
    private void onFail(Throwable t) {
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
        } else if (paused && remainingUpstreams == resumeHandles.size()) {
            triggerPauseProcessed();
        }
    }

    @Override
    public RowReceiver newRowReceiver() {
        activeUpstreams.incrementAndGet();
        return this;
    }

    private class MultiResumeHandle implements ResumeHandle {
        private final ImmutableList<ResumeHandle> resumeHandles;

        MultiResumeHandle(ImmutableList<ResumeHandle> resumeHandles) {
            this.resumeHandles = resumeHandles;
        }

        @Override
        public void resume(boolean async) {
            if (!pauseFifo.isEmpty()) {
                Object[] row;
                loop:
                while ((row = pauseFifo.poll()) != null) {
                    sharedRow.cells(row);
                    Result result = delegate.setNextRow(sharedRow);
                    switch (result) {
                        case CONTINUE:
                            continue;
                        case PAUSE:
                            // this -> MultiResumableUpstream again, not the MultiUpstreamRowReceiver
                            delegate.pauseProcessed(this);

                            // don't resume upstreams,
                            // instead this resumable will be called again and it will continue using the pauseFifo
                            return;
                        case STOP:
                            downstreamFinished = true;
                            pauseFifo.clear();
                            break loop; // pass through resume to rowMerger-upstreams to process STOP and finish
                    }
                    throw new AssertionError("Unrecognized setNextRow result: " + result);
                }
            }
            paused = false;
            pauseTriggered.set(false);
            for (ResumeHandle resumeHandle : resumeHandles) {
                resumeHandle.resume(async);
            }
        }
    }
}
