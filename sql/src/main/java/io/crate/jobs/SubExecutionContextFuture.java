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

package io.crate.jobs;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

@ParametersAreNonnullByDefault
public class SubExecutionContextFuture extends CompletableFuture<CompletionState> {

    private final CompletableFuture<CompletionState> internalFuture = new CompletableFuture<>();
    private final AtomicBoolean closeCalled = new AtomicBoolean(false);
    private final CompletionState state = new CompletionState();

    /**
     * @return true if this is the first call to this method
     */
    boolean firstClose() {
        return !closeCalled.getAndSet(true);
    }

    /**
     * @return true if in the process of closing or done
     */
    public synchronized boolean closed() {
        return closeCalled.get() || internalFuture.isDone();
    }

    public void bytesUsed(long bytes) {
        state.bytesUsed(bytes);
    }

    public boolean close(@Nullable Throwable t) {
        if (t == null) {
            return complete(state);
        } else {
            return completeExceptionally(t);
        }
    }

    @Override
    public CompletionState get() throws InterruptedException, ExecutionException {
        return internalFuture.get();
    }

    @Override
    public CompletionState get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return internalFuture.get(timeout, unit);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return internalFuture.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return internalFuture.isCancelled();
    }

    @Override
    public boolean isDone() {
        return closed();
    }

    @Override
    public CompletionState join() {
        return internalFuture.join();
    }

    @Override
    public CompletionState getNow(CompletionState valueIfAbsent) {
        return internalFuture.getNow(valueIfAbsent);
    }

    @Override
    public boolean complete(CompletionState value) {
        super.complete(value);
        return internalFuture.complete(value);
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
        super.completeExceptionally(ex);
        return internalFuture.completeExceptionally(ex);
    }

    @Override
    public <U> CompletableFuture<U> thenApply(Function<? super CompletionState, ? extends U> fn) {
        return internalFuture.thenApply(fn);
    }

    @Override
    public <U> CompletableFuture<U> thenApplyAsync(Function<? super CompletionState, ? extends U> fn) {
        return internalFuture.thenApplyAsync(fn);
    }

    @Override
    public <U> CompletableFuture<U> thenApplyAsync(Function<? super CompletionState, ? extends U> fn, Executor executor) {
        return internalFuture.thenApplyAsync(fn, executor);
    }

    @Override
    public CompletableFuture<Void> thenAccept(Consumer<? super CompletionState> action) {
        return internalFuture.thenAccept(action);
    }

    @Override
    public CompletableFuture<Void> thenAcceptAsync(Consumer<? super CompletionState> action) {
        return internalFuture.thenAcceptAsync(action);
    }

    @Override
    public CompletableFuture<Void> thenAcceptAsync(Consumer<? super CompletionState> action, Executor executor) {
        return internalFuture.thenAcceptAsync(action, executor);
    }

    @Override
    public CompletableFuture<Void> thenRun(Runnable action) {
        return internalFuture.thenRun(action);
    }

    @Override
    public CompletableFuture<Void> thenRunAsync(Runnable action) {
        return internalFuture.thenRunAsync(action);
    }

    @Override
    public CompletableFuture<Void> thenRunAsync(Runnable action, Executor executor) {
        return internalFuture.thenRunAsync(action, executor);
    }

    @Override
    public <U, V> CompletableFuture<V> thenCombine(CompletionStage<? extends U> other, BiFunction<? super CompletionState, ? super U, ? extends V> fn) {
        return internalFuture.thenCombine(other, fn);
    }

    @Override
    public <U, V> CompletableFuture<V> thenCombineAsync(CompletionStage<? extends U> other, BiFunction<? super CompletionState, ? super U, ? extends V> fn) {
        return internalFuture.thenCombineAsync(other, fn);
    }

    @Override
    public <U, V> CompletableFuture<V> thenCombineAsync(CompletionStage<? extends U> other, BiFunction<? super CompletionState, ? super U, ? extends V> fn, Executor executor) {
        return internalFuture.thenCombineAsync(other, fn, executor);
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBoth(CompletionStage<? extends U> other, BiConsumer<? super CompletionState, ? super U> action) {
        return internalFuture.thenAcceptBoth(other, action);
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super CompletionState, ? super U> action) {
        return internalFuture.thenAcceptBothAsync(other, action);
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super CompletionState, ? super U> action, Executor executor) {
        return internalFuture.thenAcceptBothAsync(other, action, executor);
    }

    @Override
    public CompletableFuture<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
        return internalFuture.runAfterBoth(other, action);
    }

    @Override
    public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
        return internalFuture.runAfterBothAsync(other, action);
    }

    @Override
    public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return internalFuture.runAfterBothAsync(other, action, executor);
    }

    @Override
    public <U> CompletableFuture<U> applyToEither(CompletionStage<? extends CompletionState> other, Function<? super CompletionState, U> fn) {
        return internalFuture.applyToEither(other, fn);
    }

    @Override
    public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends CompletionState> other, Function<? super CompletionState, U> fn) {
        return internalFuture.applyToEitherAsync(other, fn);
    }

    @Override
    public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends CompletionState> other, Function<? super CompletionState, U> fn, Executor executor) {
        return internalFuture.applyToEitherAsync(other, fn, executor);
    }

    @Override
    public CompletableFuture<Void> acceptEither(CompletionStage<? extends CompletionState> other, Consumer<? super CompletionState> action) {
        return internalFuture.acceptEither(other, action);
    }

    @Override
    public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends CompletionState> other, Consumer<? super CompletionState> action) {
        return internalFuture.acceptEitherAsync(other, action);
    }

    @Override
    public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends CompletionState> other, Consumer<? super CompletionState> action, Executor executor) {
        return internalFuture.acceptEitherAsync(other, action, executor);
    }

    @Override
    public CompletableFuture<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
        return internalFuture.runAfterEither(other, action);
    }

    @Override
    public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
        return internalFuture.runAfterEitherAsync(other, action);
    }

    @Override
    public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return internalFuture.runAfterEitherAsync(other, action, executor);
    }

    @Override
    public <U> CompletableFuture<U> thenCompose(Function<? super CompletionState, ? extends CompletionStage<U>> fn) {
        return internalFuture.thenCompose(fn);
    }

    @Override
    public <U> CompletableFuture<U> thenComposeAsync(Function<? super CompletionState, ? extends CompletionStage<U>> fn) {
        return internalFuture.thenComposeAsync(fn);
    }

    @Override
    public <U> CompletableFuture<U> thenComposeAsync(Function<? super CompletionState, ? extends CompletionStage<U>> fn, Executor executor) {
        return internalFuture.thenComposeAsync(fn, executor);
    }

    @Override
    public CompletableFuture<CompletionState> whenComplete(BiConsumer<? super CompletionState, ? super Throwable> action) {
        return internalFuture.whenComplete(action);
    }

    @Override
    public CompletableFuture<CompletionState> whenCompleteAsync(BiConsumer<? super CompletionState, ? super Throwable> action) {
        return internalFuture.whenCompleteAsync(action);
    }

    @Override
    public CompletableFuture<CompletionState> whenCompleteAsync(BiConsumer<? super CompletionState, ? super Throwable> action, Executor executor) {
        return internalFuture.whenCompleteAsync(action, executor);
    }

    @Override
    public <U> CompletableFuture<U> handle(BiFunction<? super CompletionState, Throwable, ? extends U> fn) {
        return internalFuture.handle(fn);
    }

    @Override
    public <U> CompletableFuture<U> handleAsync(BiFunction<? super CompletionState, Throwable, ? extends U> fn) {
        return internalFuture.handleAsync(fn);
    }

    @Override
    public <U> CompletableFuture<U> handleAsync(BiFunction<? super CompletionState, Throwable, ? extends U> fn, Executor executor) {
        return internalFuture.handleAsync(fn, executor);
    }

    @Override
    public CompletableFuture<CompletionState> toCompletableFuture() {
        return internalFuture.toCompletableFuture();
    }

    @Override
    public CompletableFuture<CompletionState> exceptionally(Function<Throwable, ? extends CompletionState> fn) {
        return internalFuture.exceptionally(fn);
    }

    @Override
    public boolean isCompletedExceptionally() {
        return internalFuture.isCompletedExceptionally();
    }

    @Override
    public void obtrudeValue(CompletionState value) {
        internalFuture.obtrudeValue(value);
    }

    @Override
    public void obtrudeException(Throwable ex) {
        internalFuture.obtrudeException(ex);
    }

    @Override
    public int getNumberOfDependents() {
        return internalFuture.getNumberOfDependents();
    }
}
