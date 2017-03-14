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
import io.crate.operation.collect.CrateCollector;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Collector that wraps 1+ other collectors.
 * <p>
 * This is useful to execute multiple collectors non-concurrent/sequentially.
 * <p>
 */
public class CompositeCollector implements CrateCollector {

    private final List<CrateCollector> collectors;

    /**
     * Create a BatchConsumer which accepts multiple {@link BatchConsumer#accept(BatchIterator, Throwable)} calls before
     * it uses {@code compositeBatchIteratorFactory} to create a BatchIterator which will be passed to {@code finalConsumer}
     */
    public static BatchConsumer newMultiConsumer(int numAccepts,
                                                 BatchConsumer finalConsumer,
                                                 Function<BatchIterator[], BatchIterator> compositeBatchIteratorFactory) {
        return new MultiConsumer(numAccepts, finalConsumer, compositeBatchIteratorFactory);
    }

    public static CompositeCollector syncCompositeCollector(Collection<? extends Builder> builders,
                                                            BatchConsumer consumer) {
        return new CompositeCollector(
            builders,
            new MultiConsumer(builders.size(), consumer, CompositeBatchIterator::new)
        );
    }

    private CompositeCollector(Collection<? extends Builder> builders, MultiConsumer multiConsumer) {
        assert builders.size() > 1 : "CompositeCollector must not be called with less than 2 collectors";

        collectors = new ArrayList<>(builders.size());
        for (Builder builder : builders) {
            collectors.add(builder.build(multiConsumer));
        }
    }

    @Override
    public void doCollect() {
        for (CrateCollector collector : collectors) {
            collector.doCollect();
        }
    }

    @Override
    public void kill(Throwable throwable) {
        for (CrateCollector collector : collectors) {
            collector.kill(throwable);
        }
    }

    static class MultiConsumer implements BatchConsumer {

        private final BatchIterator[] iterators;
        private final BatchConsumer consumer;
        private final Function<BatchIterator[], BatchIterator> compositeBatchIteratorFactory;

        private AtomicInteger remainingAccepts;
        private Throwable lastFailure;

        private MultiConsumer(int numAccepts,
                              BatchConsumer consumer,
                              Function<BatchIterator[], BatchIterator> compositeBatchIteratorFactory) {
            this.remainingAccepts = new AtomicInteger(numAccepts);
            this.iterators = new BatchIterator[numAccepts];
            this.consumer = consumer;
            this.compositeBatchIteratorFactory = compositeBatchIteratorFactory;
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
                consumer.accept(compositeBatchIteratorFactory.apply(iterators), lastFailure);
            }
        }

        @Override
        public boolean requiresScroll() {
            return consumer.requiresScroll();
        }
    }
}
