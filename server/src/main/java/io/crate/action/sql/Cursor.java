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

package io.crate.action.sql;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.crate.data.BatchIterator;
import io.crate.data.ForwardingBatchIterator;
import io.crate.data.LimitingBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.expression.symbol.Symbol;
import io.crate.sql.tree.Declare.Hold;
import io.crate.sql.tree.Fetch.ScrollMode;

public final class Cursor implements AutoCloseable {

    private final Hold hold;
    private final CompletableFuture<BatchIterator<Row>> queryIterator;
    private final List<Symbol> outputs;

    public Cursor(Hold hold,
                  CompletableFuture<BatchIterator<Row>> queryIterator,
                  List<Symbol> outputs) {
        this.hold = hold;
        this.queryIterator = queryIterator;
        this.outputs = outputs;
    }

    public Hold hold() {
        return hold;
    }

    public void fetch(RowConsumer consumer, ScrollMode scrollMode, long count) {
        if (scrollMode == ScrollMode.ABSOLUTE) {
            consumer.accept(null, new UnsupportedOperationException(
                "Scrolling to an absolute position is not supported"));
            return;
        }
        if (count < 0) {
            consumer.accept(null, new UnsupportedOperationException("Cannot scroll backwards"));
            return;
        }
        if (count == 0) {
            consumer.accept(null, new UnsupportedOperationException("Scroll must move forward"));
            return;
        }
        if (queryIterator.isDone()) {
            try {
                BatchIterator<Row> bi = queryIterator.join();
                triggerConsumer(consumer, bi, count);
            } catch (Throwable t) {
                consumer.accept(null, t);
                return;
            }
        } else {
            queryIterator.whenComplete((bi, err) -> {
                if (err == null) {
                    triggerConsumer(consumer, bi, count);
                } else {
                    consumer.accept(null, err);
                }
            });
        }
    }

    private void triggerConsumer(RowConsumer consumer, BatchIterator<Row> fullResult, long count) {
        // Alternative implementations:
        // - Could extend RowConsumer to have native scroll support
        //   e.g. accept(bi, scrollMode, count)?
        //
        // - Could also have a "ScrollingBatchIterator"

        BatchIterator<Row> batchIterator;
        if (count <= Integer.MAX_VALUE) {
            batchIterator = LimitingBatchIterator.newInstance(fullResult, (int) count);
        } else {
            batchIterator = fullResult;
        }
        var nonClosingBi = new ForwardingBatchIterator<Row>() {

            @Override
            protected BatchIterator<Row> delegate() {
                return batchIterator;
            }

            @Override
            public void close() {
                // Close must be deferred to when the cursor gets closed
            }
        };
        consumer.accept(nonClosingBi, null);
    }

    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public void close() {
        if (queryIterator.isDone() && !queryIterator.isCompletedExceptionally()) {
            queryIterator.join().close();
        } else {
            queryIterator.whenComplete((bi, err) -> {
                if (bi != null) {
                    bi.close();
                }
            });
        }
    }
}
