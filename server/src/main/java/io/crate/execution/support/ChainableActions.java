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

package io.crate.execution.support;

import io.crate.common.exceptions.Exceptions;
import io.crate.exceptions.MultiException;

import org.jetbrains.annotations.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public class ChainableActions {

    /**
     * Runs the given list of chainable actions sequentially.
     *
     * If one fails, all previous actions will be rolled back by calling the {@link ChainableAction#undo()}
     * callable of each action, including the failing one, in reverse order.
     */
    public static <R> CompletableFuture<R> run(List<? extends ChainableAction<R>> actions) {
        assert actions.size() > 0 : "Empty list of ChainableActions";

        List<ChainableAction<R>> previousActions = new ArrayList<>(actions.size());
        ChainableAction<R> lastAction = actions.get(0);
        CompletableFuture<R> future = lastAction.doIt();

        Result<R> result = new Result<>(null, null);
        if (actions.size() > 1) {
            for (int i = 1; i < actions.size(); i++) {
                ChainableAction<R> action = actions.get(i);
                previousActions.add(lastAction);
                lastAction = action;
                future = future.handle(result::addResultAndError)
                    .thenCompose(r -> runOrRollbackOnErrors(r, action, List.copyOf(previousActions)));
            }
        } else {
            // we'll want to undo the action in case it fails
            previousActions.add(lastAction);
        }

        return future.handle(result::addResultAndError)
            .thenCompose(r -> rollbackOnErrors(r, previousActions));
    }

    private static <R> CompletableFuture<R> runOrRollbackOnErrors(Result<R> result,
                                                                  ChainableAction<R> action,
                                                                  List<ChainableAction<R>> previousActions) {
        if (result.error != null) {
            return rollbackOnErrors(result, previousActions);
        }
        return action.doIt();
    }

    /**
     * Rollback every previous action if the given result contains an error.
     *
     * Note: This method is called for every action, even if a previous one failed and doing a rollback on every
     * chain action was done already. So the implementation must ensure that the rollback is only done once.
     */
    private static <R> CompletableFuture<R> rollbackOnErrors(Result<R> result,
                                                             List<ChainableAction<R>> previousActions) {
        if (result.error != null) {
            // Only undo the chain once
            if (result.undoDone) {
                Exceptions.rethrowUnchecked(result.error);
                return CompletableFuture.failedFuture(result.error);
            }
            int previousActionsSize = previousActions.size();
            assert previousActionsSize > 0 : "previous actions cannot be empty. " +
                                             "if only one action was executed it should be part of the previous actions in order to undo it";
            CompletableFuture<R> previousActionUndo = previousActions.get(previousActionsSize - 1).undo();
            for (int i = previousActionsSize - 2; i >= 0; i--) {
                ChainableAction<R> previousAction = previousActions.get(i);
                previousActionUndo = previousActionUndo
                    .handle(result::addResultAndError)
                    .thenCompose(r -> {
                        if (r.errorOnUndo) {
                            // last undo also throws an exception. throw it, will stop execution
                            // (no further undo actions are executed)
                            Exceptions.rethrowUnchecked(r.error);
                        }
                        return previousAction.undo();
                    });
            }
            result.undoDone = true;
            return previousActionUndo
                .handle(result::addResultAndError)
                .thenCompose(r -> {
                    Exceptions.rethrowUnchecked(result.error);
                    return CompletableFuture.failedFuture(result.error);
                });
        }
        return CompletableFuture.completedFuture(result.result);
    }

    private static class Result<R> {

        @Nullable
        private R result;
        @Nullable
        private Throwable error;
        private boolean errorOnUndo = false;
        private boolean undoDone = false;

        public Result(@Nullable R result, @Nullable Throwable error) {
            this.result = result;
            this.error = error;
        }

        Result<R> addResultAndError(@Nullable R result, @Nullable Throwable t) {
            if (result != null) {
                this.result = result;
            }
            t = unwrap(t);
            if (t != null) {
                if (error != null && error != t) {
                    error = MultiException.of(error, t);
                    // if an error was already set, current error must resulted due to on undo operation
                    errorOnUndo = true;
                } else {
                    error = t;
                }
            }
            return this;
        }
    }

    private static Throwable unwrap(@Nullable Throwable t) {
        if (t instanceof CompletionException) {
            return t.getCause();
        }
        return t;
    }
}
