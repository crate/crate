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

package io.crate.action.job;

import io.crate.data.BatchConsumer;
import io.crate.data.Row;
import io.crate.data.ListenableBatchIterator;
import io.crate.operation.projectors.*;

import javax.annotation.Nullable;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Dummy RowReceiver that must not be used as RowReceiver but as {@link BatchConsumer} using {@link #asConsumer()}
 *
 * This is temporary until {@link RowReceiver} is removed.
 */
public class BatchConsumerRowReceiverAdapter implements RowReceiver {

    private final BatchConsumer batchConsumer;
    private final CompletableFuture<Void> completionFuture = new CompletableFuture<>();

    BatchConsumerRowReceiverAdapter(BatchConsumer batchConsumer) {
        this.batchConsumer = batchConsumer;
    }

    @Override
    public CompletableFuture<?> completionFuture() {
        return completionFuture;
    }

    @Override
    public Result setNextRow(Row row) {
        return Result.STOP;
    }

    @Override
    public void pauseProcessed(ResumeHandle resumeable) {
        throw new UnsupportedOperationException("DON'T USE THIS");
    }

    @Override
    public void finish(RepeatHandle repeatable) {
        throw new UnsupportedOperationException("DON'T USE THIS");
    }

    @Override
    public void fail(Throwable throwable) {
        throw new UnsupportedOperationException("DON'T USE THIS");
    }

    @Override
    public void kill(Throwable throwable) {
        throw new UnsupportedOperationException("DON'T USE THIS");
    }

    @Override
    public Set<Requirement> requirements() {
        if (batchConsumer.requiresScroll()) {
            return EnumSet.of(Requirement.REPEAT);
        }
        return Requirements.NO_REQUIREMENTS;
    }

    @Nullable
    @Override
    public BatchConsumer asConsumer() {
        return (bi, failure) -> {
            batchConsumer.accept(new ListenableBatchIterator(bi, completionFuture), failure);
        };
    }
}
