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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ChainableActionsTest {

    private static class TrackedChainableAction extends ChainableAction<Integer> {

        private final Integer idx;
        private final List<Integer> doCalls;
        private final List<Integer> undoCalls;

        TrackedChainableAction(Integer idx,
                               List<Integer> doCalls,
                               List<Integer> undoCalls,
                               Supplier<CompletableFuture<Integer>> doSupplier,
                               Supplier<CompletableFuture<Integer>> undoSupplier) {
            super(doSupplier, undoSupplier);
            this.idx = idx;
            this.doCalls = doCalls;
            this.undoCalls = undoCalls;
        }

        @Override
        CompletableFuture<Integer> doIt() {
            doCalls.add(idx);
            return super.doIt();
        }

        @Override
        CompletableFuture<Integer> undo() {
            undoCalls.add(idx);
            return super.undo();
        }
    }

    @Test
    public void testRun() throws Exception {
        int numActions = 3;
        List<TrackedChainableAction> actions = new ArrayList<>(numActions);
        List<Integer> doCalls = new ArrayList<>(numActions);
        List<Integer> undoCalls = new ArrayList<>(numActions);

        for (int i = 0; i < numActions; i++) {
            actions.add(new TrackedChainableAction(
                i,
                doCalls,
                undoCalls,
                () -> CompletableFuture.completedFuture(0),
                () -> CompletableFuture.completedFuture(0)));
        }

        CompletableFuture<Integer> result = ChainableActions.run(actions);
        assertThat(result.get(1, TimeUnit.SECONDS), is(0));

        assertThat(doCalls, contains(0, 1, 2));
        assertThat(undoCalls, empty());
    }

    @Test
    public void testRollbackOnError() {
        int numActions = 3;
        List<TrackedChainableAction> actions = new ArrayList<>(numActions);
        List<Integer> doCalls = new ArrayList<>(numActions);
        List<Integer> undoCalls = new ArrayList<>(numActions);

        for (int i = 0; i < numActions - 1; i++) {
            actions.add(new TrackedChainableAction(
                i,
                doCalls,
                undoCalls,
                () -> CompletableFuture.completedFuture(0),
                () -> CompletableFuture.completedFuture(0)));
        }

        // create last one which will throw an error, undo() on all previous actions must be called in reverse order
        CompletableFuture<Integer> failingFuture = new CompletableFuture<>();
        TrackedChainableAction failingAction = new TrackedChainableAction(
            numActions - 1,
            doCalls,
            undoCalls,
            () -> failingFuture,
            () -> CompletableFuture.completedFuture(0));
        actions.add(failingAction);

        CompletableFuture<Integer> result = ChainableActions.run(actions);
        failingFuture.completeExceptionally(new RuntimeException("future failed"));

        assertThat(result.isCompletedExceptionally(), is(true));

        assertThat(doCalls, contains(0, 1, 2));
        assertThat(undoCalls, contains(1, 0));
    }
}
