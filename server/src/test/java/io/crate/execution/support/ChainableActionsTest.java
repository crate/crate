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

import io.crate.exceptions.MultiException;
import io.crate.exceptions.SQLExceptions;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
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
        public CompletableFuture<Integer> doIt() {
            doCalls.add(idx);
            return super.doIt();
        }

        @Override
        public CompletableFuture<Integer> undo() {
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
        actions.add(new TrackedChainableAction(
            numActions - 1,
            doCalls,
            undoCalls,
            () -> CompletableFuture.failedFuture(new RuntimeException("do operation failed")),
            () -> CompletableFuture.completedFuture(0)));

        CompletableFuture<Integer> result = ChainableActions.run(actions);

        assertThat(result.isCompletedExceptionally(), is(true));

        assertThat(doCalls, contains(0, 1, 2));
        assertThat(undoCalls, contains(1, 0));
    }

    @Test
    public void testRollbackOnOneAction() throws Exception {
        int numActions = 1;
        List<TrackedChainableAction> actions = new ArrayList<>(numActions);
        List<Integer> doCalls = new ArrayList<>(numActions);
        List<Integer> undoCalls = new ArrayList<>(numActions);

        actions.add(new TrackedChainableAction(
            numActions - 1,
            doCalls,
            undoCalls,
            () -> CompletableFuture.failedFuture(new RuntimeException("do operation failed")),
            () -> CompletableFuture.completedFuture(0)));

        CompletableFuture<Integer> result = ChainableActions.run(actions);
        assertThat(result.isCompletedExceptionally(), is(true));
        try {
            result.get();
        } catch (ExecutionException e) {
            assertThat(e.getCause().getMessage(), is("do operation failed"));
        }

        assertThat(doCalls, contains(0));
        assertThat(undoCalls, contains(0));
    }

    @Test
    public void testRollbackOnErrorWhenFirstActionFails() throws Exception {
        int numActions = 3;
        List<TrackedChainableAction> actions = new ArrayList<>(numActions);
        List<Integer> doCalls = new ArrayList<>(numActions);
        List<Integer> undoCalls = new ArrayList<>(numActions);

        actions.add(new TrackedChainableAction(
            numActions - 1,
            doCalls,
            undoCalls,
            () -> CompletableFuture.failedFuture(new RuntimeException("do operation failed")),
            () -> CompletableFuture.completedFuture(0)));

        for (int i = 0; i < numActions - 1; i++) {
            actions.add(new TrackedChainableAction(
                i,
                doCalls,
                undoCalls,
                () -> CompletableFuture.completedFuture(0),
                () -> CompletableFuture.completedFuture(0)));
        }

        CompletableFuture<Integer> result = ChainableActions.run(actions);

        assertThat(result.isCompletedExceptionally(), is(true));
        try {
            result.get();
        } catch (ExecutionException e) {
            assertThat(e.getCause().getMessage(), is("do operation failed"));
        }

        assertThat(doCalls, contains(2));
        assertThat(undoCalls, contains(2));
    }

    @Test
    public void testRollbackErrorsAreChainedToRootCause() {
        int numActions = 3;
        List<TrackedChainableAction> actions = new ArrayList<>(numActions);
        List<Integer> doCalls = new ArrayList<>(numActions);
        List<Integer> undoCalls = new ArrayList<>(numActions);

        actions.add(new TrackedChainableAction(
            0,
            doCalls,
            undoCalls,
            () -> CompletableFuture.completedFuture(0),
            () -> CompletableFuture.completedFuture(0)));

        // 2nd one will throw an error on rollback
        actions.add(new TrackedChainableAction(
            1,
            doCalls,
            undoCalls,
            () -> CompletableFuture.completedFuture(0),
            () -> CompletableFuture.failedFuture(new RuntimeException("the undo operation failed"))));

        // last one which will throw an error, undo() on all previous actions must be called in reverse order
        actions.add(new TrackedChainableAction(
            numActions - 1,
            doCalls,
            undoCalls,
            () -> CompletableFuture.failedFuture(new RuntimeException("the do operation failed")),
            () -> CompletableFuture.completedFuture(0)));

        CompletableFuture<Integer> result = ChainableActions.run(actions);

        assertThat(result.isCompletedExceptionally(), is(true));
        assertThat(doCalls, contains(0, 1, 2));
        // Undo was only called on action 1 as 2 failed and this undo action failed also,
        // so no other action was rolled back.
        assertThat(undoCalls, contains(1));

        try {
            result.get();
        } catch (Throwable t) {
            t = SQLExceptions.unwrap(t);
            assertThat(t, instanceOf(MultiException.class));

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            t.printStackTrace(new PrintStream(out));
            assertThat(new String(out.toByteArray()), allOf(
                containsString("do operation failed"),
                containsString("undo operation failed")));
        }
    }

    @Test
    public void testRollbackIsOnlyDoneOnce() {
        int numActions = 3;
        List<TrackedChainableAction> actions = new ArrayList<>(numActions);
        List<Integer> doCalls = new ArrayList<>(numActions);
        List<Integer> undoCalls = new ArrayList<>(numActions);

        actions.add(new TrackedChainableAction(
            0,
            doCalls,
            undoCalls,
            () -> CompletableFuture.failedFuture(new RuntimeException("the first do operation failed")),
            () -> CompletableFuture.completedFuture(0)));

        // 2nd one will throw an error on rollback
        actions.add(new TrackedChainableAction(
            1,
            doCalls,
            undoCalls,
            () -> CompletableFuture.completedFuture(0),
            () -> CompletableFuture.failedFuture(new RuntimeException("the undo operation failed"))));

        // last one which will throw an error, undo() on all previous actions must be called in reverse order
        actions.add(new TrackedChainableAction(
            2,
            doCalls,
            undoCalls,
            () -> CompletableFuture.failedFuture(new RuntimeException("the do operation failed")),
            () -> CompletableFuture.completedFuture(0)));

        CompletableFuture<Integer> result = ChainableActions.run(actions);

        assertThat(result.isCompletedExceptionally(), is(true));
        assertThat(doCalls, contains(0));
        // undo was only called on action 0 as it failed, so rollback only action 0
        assertThat(undoCalls, contains(0));

        try {
            result.get();
        } catch (Throwable t) {
            t = SQLExceptions.unwrap(t);
            assertThat(t, not(instanceOf(MultiException.class)));

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            t.printStackTrace(new PrintStream(out));
            assertThat(new String(out.toByteArray()), containsString("the first do operation failed"));
            assertThat(new String(out.toByteArray()), allOf(
                not(containsString("the do operation failed")),
                not(containsString("the undo operation failed"))));
        }
    }
}
