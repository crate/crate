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

import io.crate.data.BatchIterator;
import io.crate.operation.projectors.BatchConsumerToRowReceiver;
import io.crate.operation.projectors.RowReceiver;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class BatchIteratorCollector implements CrateCollector {

    private final Supplier<CompletableFuture<BatchIterator>> batchIteratorFuture;
    private final BatchConsumerToRowReceiver consumer;
    private final RowReceiver rowReceiver;

    public BatchIteratorCollector(Supplier<CompletableFuture<BatchIterator>> batchIteratorFuture,
                                  RowReceiver rowReceiver) {
        this.batchIteratorFuture = batchIteratorFuture;
        this.consumer = new BatchConsumerToRowReceiver(rowReceiver);
        this.rowReceiver = rowReceiver;
    }

    @Override
    public void doCollect() {
        batchIteratorFuture.get().whenComplete(consumer);
    }

    @Override
    public void kill(@Nullable Throwable throwable) {
        rowReceiver.kill(throwable);
    }
}
