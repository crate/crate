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

package io.crate.operation.collect.collectors;

import com.google.common.collect.ImmutableList;
import io.crate.concurrent.CompletionListenable;
import io.crate.concurrent.CompletionListener;
import io.crate.concurrent.CompletionMultiListener;
import io.crate.concurrent.CompletionState;
import io.crate.core.collections.Row;
import io.crate.operation.RowDownstream;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.projectors.RepeatHandle;
import io.crate.operation.projectors.Requirement;
import io.crate.operation.projectors.ResumeHandle;
import io.crate.operation.projectors.RowReceiver;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Collector that wraps 1+ other collectors.
 * <p>
 * This is useful to execute multiple collectors non-concurrent/sequentially.
 * <p>
 * CC: CompositeCollector
 * C1: CrateCollector Shard 1
 * C2: CrateCollector Shard 1
 * RR: RowReceiver
 * <p>
 * +----------------------------------+
 * |               CC                 |
 * |       C1               C2        |
 * +----------------------------------+
 * \              /
 * \            /
 * CC-RowMerger
 * |
 * RR
 * <p>
 * Flow is like this:
 * <p>
 * CC.doCollect()
 * C1.doCollect()
 * CC-RR.setNextRow()
 * RR.setNextRow()
 * (...)
 * CC-RR.finish()
 * CC.completionListener -> doCollect
 * C2.doCollect()
 * CC-RR.setNextRow()
 * RR.setNextRow()
 * (...)
 * CC-RR.finish()
 * all finished -> RR.finish
 */
public class CompositeCollector implements CrateCollector {

    private static final ESLogger LOGGER = Loggers.getLogger(CompositeCollector.class);

    private final Iterator<? extends CrateCollector> collectorsIt;

    public CompositeCollector(Collection<? extends Builder> builders, RowReceiver rowReceiver) {
        MultiRowReceiver multiRowReceiver = new MultiRowReceiver(rowReceiver);
        List<CrateCollector> collectors = new ArrayList<>(builders.size());
        for (Builder builder : builders) {
            collectors.add(builder.build(multiRowReceiver.newRowReceiver()));
        }
        multiRowReceiver.addListener(new CompletionListener() {
            @Override
            public void onSuccess(@Nullable CompletionState result) {
                doCollect();
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                kill(t);
            }
        });
        collectorsIt = collectors.iterator();
        // without at least one collector there is no way to inform someone about anything - this would just be a useless no-op class
        assert this.collectorsIt.hasNext() : "need at least one collector";
    }

    @Override
    public void doCollect() {
        if (collectorsIt.hasNext()) {
            CrateCollector collector = collectorsIt.next();
            LOGGER.trace("doCollect collector={}", collector);
            collector.doCollect();
        }
    }

    @Override
    public void kill(@Nullable Throwable throwable) {
        // no while loop because one kill will cause completionListener.onFailure to be killed which calls kill here again
        if (collectorsIt.hasNext()) {
            CrateCollector collector = collectorsIt.next();
            collector.kill(throwable);
        }
    }

    private static class MultiRowReceiver implements RowDownstream, RowReceiver, CompletionListenable {

        private final RowReceiver delegate;
        private final AtomicInteger activeUpstreams = new AtomicInteger(0);
        private final List<RepeatHandle> repeatHandles = new ArrayList<>();
        private Throwable failure = null;
        private boolean prepared = false;
        private CompletionListener listener = CompletionListener.NO_OP;

        MultiRowReceiver(RowReceiver delegate) {
            this.delegate = delegate;
        }

        private void countdown() {
            int numUpstreams = activeUpstreams.decrementAndGet();
            LOGGER.trace("countdown numUpstreams={} failure={}", numUpstreams, failure);
            if (numUpstreams == 0) {
                if (failure == null) {
                    final ImmutableList<RepeatHandle> repeatHandles = ImmutableList.copyOf(this.repeatHandles);
                    this.repeatHandles.clear();
                    delegate.finish(new RepeatHandle() {
                        @Override
                        public void repeat() {
                            if (MultiRowReceiver.this.activeUpstreams.compareAndSet(0, repeatHandles.size())) {
                                for (RepeatHandle repeatHandle : repeatHandles) {
                                    repeatHandle.repeat();
                                }
                            } else {
                                throw new IllegalStateException("Repeat called without all upstreams being finished");
                            }
                        }
                    });
                } else {
                    delegate.fail(failure);
                }
            }
        }

        @Override
        public RowReceiver newRowReceiver() {
            int numUpstreams = activeUpstreams.incrementAndGet();
            LOGGER.trace("newRowReceiver activeUpstreams={}", numUpstreams);
            return this;
        }

        @Override
        public Result setNextRow(Row row) {
            return delegate.setNextRow(row);
        }

        @Override
        public void pauseProcessed(ResumeHandle resumeable) {
            delegate.pauseProcessed(resumeable);
        }

        @Override
        public void finish(RepeatHandle repeatable) {
            this.repeatHandles.add(repeatable);
            countdown();
            listener.onSuccess(null);
        }

        @Override
        public void fail(Throwable throwable) {
            this.failure = throwable;
            countdown();
            listener.onFailure(throwable);
        }

        @Override
        public void kill(Throwable throwable) {
            listener.onFailure(throwable);
            delegate.kill(throwable);
        }

        @Override
        public void prepare() {
            if (!prepared) {
                prepared = true;
                delegate.prepare();
            }
        }

        @Override
        public Set<Requirement> requirements() {
            return delegate.requirements();
        }

        @Override
        public void addListener(CompletionListener listener) {
            this.listener = CompletionMultiListener.merge(this.listener, listener);
        }
    }
}
