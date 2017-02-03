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

import io.crate.data.Row;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.projectors.RepeatHandle;
import io.crate.operation.projectors.Requirement;
import io.crate.operation.projectors.ResumeHandle;
import io.crate.operation.projectors.RowReceiver;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
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
 *
 * Note: As this collector combines multiple collectors in 1 thread, due to current resume/repeat architecture,
 * number of collectors (shards) is limited by the configured thread stack size (default: 1024k on 64bit).
 */
public class CompositeCollector implements CrateCollector {

    private final List<CrateCollector> collectors;
    private final Receiver receiver;
    private final RowReceiver downstream;
    private boolean isDownstreamFinished = false;

    public CompositeCollector(Collection<? extends Builder> builders, RowReceiver rowReceiver) {
        assert builders.size() > 1 : "CompositeCollector must not be called with less than 2 collectors";
        receiver = new Receiver(builders.size());
        collectors = new ArrayList<>(builders.size());
        downstream = rowReceiver;
        for (Builder builder : builders) {
            collectors.add(builder.build(receiver));
        }
    }

    @Override
    public void doCollect() {
        collectors.get(0).doCollect();
    }

    @Override
    public void kill(@Nullable Throwable throwable) {
        receiver.killFromParent(throwable);
    }

    private class Receiver implements RowReceiver, RepeatHandle {

        private final AtomicInteger numFinishCalls = new AtomicInteger(0);
        private final RepeatHandle[] repeatHandles;

        private Receiver(int size) {
            repeatHandles = new RepeatHandle[size];
        }

        @Override
        public Result setNextRow(Row row) {
            Result result = downstream.setNextRow(row);
            if (result == Result.STOP) {
                isDownstreamFinished = true;
            }
            return result;
        }

        @Override
        public void pauseProcessed(ResumeHandle resumeable) {
            downstream.pauseProcessed(resumeable);
        }

        public void repeat() {
            int calls = numFinishCalls.get();
            if (calls == -1) return;
            assert calls % collectors.size() == 0 : "Repeat called without all upstreams being finished";
            repeatHandles[0].repeat();
        }

        @Override
        public void finish(RepeatHandle repeatable) {
            int calls = numFinishCalls.incrementAndGet();
            if (calls == 0) {
                // numFinishedCalls was -1 so killed
                numFinishCalls.set(-1);
                return;
            }
            repeatHandles[(calls - 1) % repeatHandles.length] = repeatable;
            if (!isDownstreamFinished && calls < repeatHandles.length) {
                // in first collect loop
                collectors.get(calls).doCollect();
            } else if (isDownstreamFinished || calls % repeatHandles.length == 0) {
                // a loop over upstreams is finished or downstream returned Result.STOP
                downstream.finish(this);
            } else {
                // in a repeat loop
                repeatHandles[calls % repeatHandles.length].repeat();
            }
        }

        @Override
        public void fail(Throwable throwable) {
            downstream.fail(throwable);
        }

        private void killFromParent(Throwable throwable) {
            int calls = numFinishCalls.getAndSet(-1);
            if (calls >= 0) {
                collectors.get(calls % repeatHandles.length).kill(throwable);
            }
            downstream.kill(throwable);
        }

        @Override
        public void kill(Throwable throwable) {
            if (numFinishCalls.get() != -1) {
                downstream.kill(throwable);
            }
        }

        @Override
        public Set<Requirement> requirements() {
            return downstream.requirements();
        }
    }
}
