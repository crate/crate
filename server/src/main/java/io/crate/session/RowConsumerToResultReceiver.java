/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.session;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.exceptions.SQLExceptions;

public class RowConsumerToResultReceiver implements RowConsumer {

    private static final Logger LOGGER = LogManager.getLogger(RowConsumerToResultReceiver.class);

    private final CompletableFuture<?> completionFuture = new CompletableFuture<>();
    private ResultReceiver<?> resultReceiver;
    private int maxRows;

    /**
     * Reset per suspend/execute
     */
    private int rowCount = 0;
    private CompletableFuture<BatchIterator<Row>> suspendedIt = new CompletableFuture<>();
    private boolean waitingForWrite = false;

    public RowConsumerToResultReceiver(ResultReceiver<?> resultReceiver, int maxRows, Consumer<Throwable> onCompletion) {
        this.resultReceiver = resultReceiver;
        this.maxRows = maxRows;
        completionFuture.whenComplete((_, err) -> {
            onCompletion.accept(err);
        });
    }

    @Override
    public void accept(BatchIterator<Row> iterator, @Nullable Throwable failure) {
        if (failure == null) {
            consumeIt(iterator);
        } else {
            if (iterator != null) {
                iterator.close();
            }
            completionFuture.completeExceptionally(failure);
            resultReceiver.fail(failure);
        }
    }

    @Override
    public CompletableFuture<?> completionFuture() {
        return completionFuture;
    }

    private void consumeIt(BatchIterator<Row> iterator) {
        while (true) {
            try {
                while (iterator.moveNext()) {
                    if (rowCount > 0 && maxRows > 0 && rowCount % maxRows == 0) {
                        suspendedIt.complete(iterator);
                        resultReceiver.batchFinished();
                        return; // resumed via postgres protocol, close is done later
                    }
                    rowCount++;
                    CompletableFuture<Void> writeFuture = resultReceiver.setNextRow(iterator.currentElement());
                    if (writeFuture != null) {
                        LOGGER.trace("Suspended execution after {} rows as the receiver is not writable anymore", rowCount);
                        waitingForWrite = true;
                        writeFuture.thenRun(() -> {
                            LOGGER.trace("Resume execution after {} rows", rowCount);
                            waitingForWrite = false;
                            rowCount = 0;
                            consumeIt(iterator);
                        });
                        return;
                    }
                }
                if (iterator.allLoaded()) {
                    completionFuture.complete(null);
                    iterator.close();
                    resultReceiver.allFinished();
                    return;
                } else {
                    var nextBatch = iterator.loadNextBatch().toCompletableFuture();
                    if (nextBatch.isDone()) {
                        if (nextBatch.isCompletedExceptionally()) {
                            // trigger exception
                            nextBatch.join();
                        }
                        continue;
                    }
                    nextBatch.whenComplete((_, f) -> {
                        if (f == null) {
                            consumeIt(iterator);
                        } else {
                            Throwable t = SQLExceptions.unwrap(f);
                            iterator.close();
                            completionFuture.completeExceptionally(t);
                            resultReceiver.fail(t);
                        }
                    });
                    return;
                }
            } catch (Throwable t) {
                iterator.close();
                completionFuture.completeExceptionally(t);
                resultReceiver.fail(t);
                return;
            }
        }
    }

    /**
     * If this consumer suspended itself (due to {@code maxRows} being > 0, it will close the BatchIterator
     * and finish the ResultReceiver
     */
    public void closeAndFinishIfSuspended() {
        suspendedIt.whenComplete((it, _) -> {
            it.close();
            completionFuture.complete(null);
            // resultReceiver is left untouched:
            // - A previous .batchCompleted() call already flushed out pending messages
            // - Calling failure/allFinished would lead to extra messages, including  sentCommandComplete, to the client, which can lead to issues on the client.
        });
    }

    public boolean suspended() {
        return suspendedIt.isDone();
    }

    @VisibleForTesting
    public boolean waitingForWrite() {
        return waitingForWrite;
    }

    public void replaceResultReceiver(ResultReceiver<?> resultReceiver, int maxRows) {
        this.rowCount = 0;
        this.resultReceiver = resultReceiver;
        this.maxRows = maxRows;
    }

    public void resume() {
        assert suspended() : "resume must only be called if suspended() returned true";
        BatchIterator<Row> it = null;
        try {
            it = suspendedIt.join();
            suspendedIt = new CompletableFuture<>();
            resultReceiver.setNextRow(it.currentElement());
            rowCount++;
            consumeIt(it);
        } catch (Throwable t) {
            if (it != null) {
                it.close();
            }
            completionFuture.completeExceptionally(t);
            resultReceiver.fail(t);
        }
    }

    @Override
    public String toString() {
        return "RowConsumerToResultReceiver{" +
               "resultReceiver=" + resultReceiver +
               ", maxRows=" + maxRows +
               ", rowCount=" + rowCount +
               ", activeIt=" + suspendedIt +
               '}';
    }
}
