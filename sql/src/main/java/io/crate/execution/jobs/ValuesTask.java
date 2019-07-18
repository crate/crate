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

package io.crate.execution.jobs;

import com.google.common.collect.Lists;
import io.crate.analyze.SymbolEvaluator;
import io.crate.data.BatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.ListenableRowConsumer;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.data.RowN;
import io.crate.data.SentinelRow;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Functions;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.SubQueryResults;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class ValuesTask implements Task {

    private final int phaseId;
    private final ListenableRowConsumer consumer;
    private final List<RowN> rows;
    private final AtomicBoolean started = new AtomicBoolean(false);

    public ValuesTask(int phaseId,
                      RowConsumer rowConsumer,
                      List<List<Symbol>> rows,
                      Functions functions,
                      TransactionContext txnCtx) {
        this.phaseId = phaseId;
        this.consumer = new ListenableRowConsumer(rowConsumer);

        final int numColumns = rows.get(0).size();
        final Object[] cells = new Object[numColumns];
        final RowN rowN = new RowN(cells);
        //noinspection StaticPseudoFunctionalStyleMethod - this should be lazy and repeatable
        this.rows = Lists.transform(rows, row -> {
            assert row != null : "Rows must not be null";
            for (int i = 0; i < numColumns; i++) {
                cells[i] = SymbolEvaluator.evaluate(
                    txnCtx,
                    functions,
                    row.get(i),
                    Row.EMPTY,
                    SubQueryResults.EMPTY
                );
            }
            return rowN;
        });
    }

    @Override
    public void prepare() throws Exception {
    }

    @Override
    public void start() {
        if (started.compareAndSet(false, true)) {
            BatchIterator<Row> bi = InMemoryBatchIterator.of(rows, SentinelRow.SENTINEL);
            consumer.accept(bi, null);
        }
    }

    @Override
    public String name() {
        return "VALUES";
    }

    @Override
    public int id() {
        return phaseId;
    }

    @Override
    public CompletableFuture<CompletionState> completionFuture() {
        return consumer.completionFuture().thenApply(ignored -> new CompletionState());
    }

    @Override
    public void kill(@Nonnull Throwable throwable) {
        if (started.compareAndSet(false, true)) {
            consumer.accept(null, throwable);
        }
    }
}
