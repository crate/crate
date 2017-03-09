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

import io.crate.data.BatchConsumer;
import io.crate.data.BatchIterator;
import io.crate.exceptions.SQLExceptions;

import java.util.Objects;

public class BatchConsumerToRowReceiver implements BatchConsumer {

    private final RowReceiver rowReceiver;

    public BatchConsumerToRowReceiver(RowReceiver rowReceiver) {
        Objects.requireNonNull(rowReceiver, "RowReceiver cannot be null");
        this.rowReceiver = rowReceiver;
    }

    @Override
    public void accept(BatchIterator iterator, Throwable failure) {
        rowReceiver.completionFuture().whenComplete((ignored, t) -> iterator.close());
        if (failure == null) {
            safeConsumeIterator(iterator);
        } else {
            rowReceiver.fail(failure);
        }
    }

    private void safeConsumeIterator(BatchIterator it) {
        try {
            consumeIterator(it);
        } catch (IllegalStateException e) {
            if (!rowReceiver.completionFuture().isDone()) {
                throw e;
            }
            // swallow exception; rowReceiver got killed from outside which triggered the cursor-close callback
        }
    }
    private void consumeIterator(BatchIterator iterator) {
        try {
            while (iterator.moveNext()) {
                RowReceiver.Result result = rowReceiver.setNextRow(iterator.currentRow());
                switch (result) {
                    case CONTINUE:
                        break;
                    case STOP:
                        rowReceiver.finish(getRepeatHandle(iterator));
                        return;
                    case PAUSE:
                        rowReceiver.pauseProcessed(async -> consumeIterator(iterator));
                        return;
                }
            }
        } catch (Exception e) {
            rowReceiver.fail(e);
            return;
        }

        if (iterator.allLoaded()) {
            rowReceiver.finish(getRepeatHandle(iterator));
        } else {
            iterator.loadNextBatch().whenComplete(
                (r, e) -> {
                    if (e != null) {
                        rowReceiver.fail(SQLExceptions.unwrap(e));
                    } else {
                        safeConsumeIterator(iterator);
                    }
                }
            );
        }
    }

    private RepeatHandle getRepeatHandle(BatchIterator iterator) {
        return () -> {
            iterator.moveToStart();
            consumeIterator(iterator);
        };
    }
}
