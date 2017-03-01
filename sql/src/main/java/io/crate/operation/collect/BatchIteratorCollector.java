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

package io.crate.operation.collect;

import io.crate.data.BatchConsumer;
import io.crate.data.BatchIterator;
import io.crate.data.Killable;

import javax.annotation.Nullable;

/**
 * Collector adapter to a BatchIterator.
 * {@link #doCollect()} results in a {@link BatchConsumer#accept(BatchIterator, Throwable)} call on the consumer.
 */
public class BatchIteratorCollector implements CrateCollector {

    private final BatchIterator batchIterator;
    private final BatchConsumer consumer;
    private final Killable killable;

    /**
     * @param batchIterator providing the data
     * @param consumer receiving the data
     * @param killable to be used when {@link #kill(Throwable)} is called
     */
    public BatchIteratorCollector(BatchIterator batchIterator, BatchConsumer consumer, Killable killable) {
        this.batchIterator = batchIterator;
        this.consumer = consumer;
        this.killable = killable;
    }

    @Override
    public void doCollect() {
        consumer.accept(batchIterator, null);
    }

    @Override
    public void kill(@Nullable Throwable throwable) {
        killable.kill(throwable);
    }
}
