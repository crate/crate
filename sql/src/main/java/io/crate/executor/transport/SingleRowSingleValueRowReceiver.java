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

package io.crate.executor.transport;

import io.crate.analyze.symbol.SelectSymbol;
import io.crate.data.Row;
import io.crate.operation.projectors.*;
import io.crate.planner.Plan;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * RowReceiver expects to receive only one row and triggers a future with the value once completed
 */
class SingleRowSingleValueRowReceiver implements RowReceiver {

    private final CompletableFuture<Object> completionFuture = new CompletableFuture<>();
    private final static Object SENTINEL = new Object();
    private final SubSelectSymbolReplacer replacer;
    private Object value = SENTINEL;

    SingleRowSingleValueRowReceiver(Plan rootPlan, SelectSymbol selectSymbolToReplace) {
        replacer = new SubSelectSymbolReplacer(rootPlan, selectSymbolToReplace);
    }

    @Override
    public Result setNextRow(Row row) {
        if (this.value == SENTINEL) {
            this.value = row.get(0);
        } else {
            throw new UnsupportedOperationException("Subquery returned more than 1 row");
        }
        return Result.CONTINUE;
    }

    @Override
    public void pauseProcessed(ResumeHandle resumeable) {
    }

    @Override
    public void finish(RepeatHandle repeatable) {
        try {
            Object value = this.value == SENTINEL ? null : this.value;
            replacer.onSuccess(value);
        } catch (Throwable e) {
            completionFuture.completeExceptionally(e);
            return;
        }
        completionFuture.complete(value);
    }

    @Override
    public void fail(Throwable throwable) {
        completionFuture.completeExceptionally(throwable);
    }

    @Override
    public void kill(Throwable throwable) {
        completionFuture.completeExceptionally(throwable);
    }

    @Override
    public Set<Requirement> requirements() {
        return Requirements.NO_REQUIREMENTS;
    }

    public CompletableFuture<?> completionFuture() {
        return completionFuture;
    }
}
