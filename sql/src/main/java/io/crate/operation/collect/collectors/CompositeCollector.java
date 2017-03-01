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

import io.crate.data.BatchConsumer;
import io.crate.data.BatchIterator;
import io.crate.data.CompositeBatchIterator;
import io.crate.data.Killable;
import io.crate.operation.collect.CrateCollector;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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

    public CompositeCollector(Collection<? extends Builder> builders, BatchConsumer finalConsumer, Killable killable) {
        assert builders.size() > 1 : "CompositeCollector must not be called with less than 2 collectors";

        collectors = new ArrayList<>(builders.size());
        MultiConsumer multiConsumer = new MultiConsumer(builders.size(), finalConsumer);
        for (Builder builder : builders) {
            collectors.add(builder.build(multiConsumer, killable));
        }
    }

    @Override
    public void doCollect() {
        for (CrateCollector collector : collectors) {
            collector.doCollect();
        }
    }

    @Override
    public void kill(@Nullable Throwable throwable) {
        for (CrateCollector collector : collectors) {
            collector.kill(throwable);
        }
    }

    static class MultiConsumer implements BatchConsumer {

        private final BatchConsumer consumer;
        private final BatchIterator[] iterators;

        private AtomicInteger remainingAccepts;
        private Throwable lastFailure;

        MultiConsumer(int numAccepts, BatchConsumer consumer) {
            this.remainingAccepts = new AtomicInteger(numAccepts);
            this.iterators = new BatchIterator[numAccepts];
            this.consumer = consumer;
        }

        @Override
        public void accept(BatchIterator iterator, @Nullable Throwable failure) {
            int remaining = remainingAccepts.decrementAndGet();
            if (failure != null) {
                lastFailure = failure;
            }
            synchronized (iterators) {
                iterators[remaining] = iterator;
            }
            if (remaining == 0) {
                CompositeBatchIterator compositeBatchIterator = new CompositeBatchIterator(iterators);
                consumer.accept(compositeBatchIterator, lastFailure);
            }
        }
    }
}
