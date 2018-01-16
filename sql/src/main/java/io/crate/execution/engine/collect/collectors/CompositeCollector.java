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

package io.crate.execution.engine.collect.collectors;

import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.engine.collect.CrateCollector;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

/**
 * Collector that wraps 1+ other collectors.
 */
public class CompositeCollector implements CrateCollector {

    private final List<CrateCollector> collectors;

    /**
     * Create a BatchConsumer which accepts multiple {@link RowConsumer#accept(BatchIterator, Throwable)} calls before
     * it uses {@code compositeBatchIteratorFactory} to create a BatchIterator which will be passed to {@code finalConsumer}
     */
    public CompositeCollector(Collection<? extends Builder> builders,
                              RowConsumer finalConsumer,
                              Function<BatchIterator<Row>[], BatchIterator<Row>> compositeBatchIteratorFactory) {
        assert builders.size() > 1 : "CompositeCollector must not be called with less than 2 collectors";

        MultiConsumer multiConsumer = new MultiConsumer(builders.size(), finalConsumer, compositeBatchIteratorFactory);
        collectors = new ArrayList<>(builders.size());
        for (Builder builder : builders) {
            collectors.add(builder.build(builder.applyProjections(multiConsumer)));
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

    static class MultiConsumer implements RowConsumer {

        private final BatchIterator<Row>[] iterators;
        private final RowConsumer consumer;
        private final Function<BatchIterator<Row>[], BatchIterator<Row>> compositeBatchIteratorFactory;

        private int remainingAccepts;
        private Throwable lastFailure;

        MultiConsumer(int numAccepts,
                      RowConsumer consumer,
                      Function<BatchIterator<Row>[], BatchIterator<Row>> compositeBatchIteratorFactory) {
            this.remainingAccepts = numAccepts;
            this.iterators = new BatchIterator[numAccepts];
            this.consumer = consumer;
            this.compositeBatchIteratorFactory = compositeBatchIteratorFactory;
        }

        @Override
        public void accept(BatchIterator<Row> iterator, @Nullable Throwable failure) {
            int remaining;
            synchronized (iterators) {
                remainingAccepts--;
                remaining = remainingAccepts;
                if (failure != null) {
                    lastFailure = failure;
                }
                iterators[remaining] = iterator;
            }
            if (remaining == 0) {
                // null checks to avoid using the factory with potential null-entries within the iterators
                if (lastFailure == null) {
                    consumer.accept(compositeBatchIteratorFactory.apply(iterators), null);
                } else {
                    consumer.accept(null, lastFailure);
                }
            }
        }

        @Override
        public boolean requiresScroll() {
            return consumer.requiresScroll();
        }
    }
}
