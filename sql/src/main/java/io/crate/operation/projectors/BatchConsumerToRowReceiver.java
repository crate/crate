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

import io.crate.data.*;
import io.crate.exceptions.SQLExceptions;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * BatchConsumer which consumes a BatchIterator, feeding the data into a {@link RowReceiver}.
 *
 * If the {@link RowReceiver} is a {@link Projector} it will try to use {@link BatchIteratorProjector} implementations
 * by using {@link Projector#asProjector()}}.
 *
 * In case {@link RowReceiver#asConsumer()} returns a consumer this consumer will be used instead of the rowReceiver,
 * thus this BatchConsumer effectively becomes a proxy.
 */
public class BatchConsumerToRowReceiver implements BatchConsumer, Killable {

    private RowReceiver rowReceiver;
    private boolean running = false;

    public BatchConsumerToRowReceiver(RowReceiver rowReceiver) {
        Objects.requireNonNull(rowReceiver, "RowReceiver cannot be null");
        this.rowReceiver = rowReceiver;
    }

    private BatchIterator applyProjections(BatchIterator iterator) {
        RowReceiver receiver = rowReceiver;
        while (receiver instanceof Projector) {
            Projector projector = (Projector) receiver;
            BatchIteratorProjector bip = projector.asProjector();
            if (bip == null) {
                break;
            }
            receiver =  projector.downstream();
            iterator = bip.apply(iterator);
        }
        rowReceiver = receiver;
        return iterator;
    }

    @Override
    public synchronized void accept(BatchIterator it, Throwable failure) {
        assert running == false : "Accept must only be called once";
        running = true;
        final BatchIterator iterator = applyProjections(it);
        BatchConsumer batchConsumer = rowReceiver.asConsumer();
        if (batchConsumer == null) {
            rowReceiver.completionFuture().whenComplete((ignored, t) -> {
                if (iterator != null) {
                    iterator.close();
                }
            });
            if (failure == null) {
                safeConsumeIterator(iterator);
            } else {
                rowReceiver.fail(failure);
            }
        } else {
            batchConsumer.accept(iterator, failure);
        }
    }

    @Override
    public boolean requiresScroll() {
        return rowReceiver.requirements().contains(Requirement.REPEAT);
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
        assert iterator.rowData() != null: "rowData is null of iterator: " + iterator;
        final Row row = RowBridging.toRow(iterator.rowData());
        try {
            while (iterator.moveNext()) {
                RowReceiver.Result result = rowReceiver.setNextRow(row);
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
        } catch (Throwable e) {
            // catch Throwables to capture assertion errors
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

    @Override
    public synchronized void kill(@Nonnull Throwable throwable) {
        rowReceiver.kill(throwable);
        if (!running) {
            // some RowReceiver kill implementations only set a internal "stop" flag and wait for the next
            // row to process it - in order for that to work it's necessary to trigger the consumption
            accept(null, throwable);
        }
    }
}
