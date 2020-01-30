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

package io.crate.concurrent;

import io.crate.common.SuppressForbidden;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public final class CompletableFutures {

    private CompletableFutures() {
    }

    public static <T> CompletableFuture<List<T>> allAsList(Collection<? extends CompletableFuture<? extends T>> futures) {
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(aVoid -> {
                ArrayList<T> results = new ArrayList<>(futures.size());
                for (CompletableFuture<? extends T> future : futures) {
                    results.add(future.join());
                }
                return results;
            });
    }

    @SuppressForbidden
    public static <T> CompletableFuture<T> supplyAsync(Supplier<T> supplier,
                                                       Executor executor) {
        try {
            return CompletableFuture.supplyAsync(supplier, executor);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    public static <T> CompletableFuture<T> runBeforeCompleteExceptional(CompletableFuture<T> delegate,
                                                                        Consumer<Throwable> beforeExceptional) {
        return new ExceptionalInterceptingFuture<>(delegate, beforeExceptional);
    }


    private static class ExceptionalInterceptingFuture<T> extends CompletableFuture<T> {

        private final CompletableFuture<T> delegate;
        private final Consumer<Throwable> beforeExceptional;

        private ExceptionalInterceptingFuture(CompletableFuture<T> delegate, Consumer<Throwable> beforeExceptional) {
            this.delegate = delegate;
            this.beforeExceptional = beforeExceptional;
        }

        @Override
        public boolean completeExceptionally(Throwable ex) {
            beforeExceptional.accept(ex);
            return delegate.completeExceptionally(ex);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return delegate.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isDone() {
            return delegate.isDone();
        }

        @Override
        public T get() throws InterruptedException, ExecutionException {
            return delegate.get();
        }

        @Override
        public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return delegate.get(timeout, unit);
        }

        @Override
        public T join() {
            return delegate.join();
        }

        @Override
        public T getNow(T valueIfAbsent) {
            return delegate.getNow(valueIfAbsent);
        }

        @Override
        public boolean complete(T value) {
            return delegate.complete(value);
        }

        @Override
        public <U> CompletableFuture<U> thenApply(Function<? super T, ? extends U> fn) {
            return delegate.thenApply(fn);
        }

        @Override
        public <U> CompletableFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn) {
            return delegate.thenApplyAsync(fn);
        }

        @Override
        public <U> CompletableFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn,
                                                       Executor executor) {
            return delegate.thenApplyAsync(fn, executor);
        }

        @Override
        public CompletableFuture<Void> thenAccept(Consumer<? super T> action) {
            return delegate.thenAccept(action);
        }

        @Override
        public CompletableFuture<Void> thenAcceptAsync(Consumer<? super T> action) {
            return delegate.thenAcceptAsync(action);
        }

        @Override
        public CompletableFuture<Void> thenAcceptAsync(Consumer<? super T> action, Executor executor) {
            return delegate.thenAcceptAsync(action, executor);
        }

        @Override
        public CompletableFuture<Void> thenRun(Runnable action) {
            return delegate.thenRun(action);
        }

        @Override
        public CompletableFuture<Void> thenRunAsync(Runnable action) {
            return delegate.thenRunAsync(action);
        }

        @Override
        public CompletableFuture<Void> thenRunAsync(Runnable action, Executor executor) {
            return delegate.thenRunAsync(action, executor);
        }

        @Override
        public <U, V> CompletableFuture<V> thenCombine(CompletionStage<? extends U> other,
                                                       BiFunction<? super T, ? super U, ? extends V> fn) {
            return delegate.thenCombine(other, fn);
        }

        @Override
        public <U, V> CompletableFuture<V> thenCombineAsync(CompletionStage<? extends U> other,
                                                            BiFunction<? super T, ? super U, ? extends V> fn) {
            return delegate.thenCombineAsync(other, fn);
        }

        @Override
        public <U, V> CompletableFuture<V> thenCombineAsync(CompletionStage<? extends U> other,
                                                            BiFunction<? super T, ? super U, ? extends V> fn,
                                                            Executor executor) {
            return delegate.thenCombineAsync(other, fn, executor);
        }

        @Override
        public <U> CompletableFuture<Void> thenAcceptBoth(CompletionStage<? extends U> other,
                                                          BiConsumer<? super T, ? super U> action) {
            return delegate.thenAcceptBoth(other, action);
        }

        @Override
        public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other,
                                                               BiConsumer<? super T, ? super U> action) {
            return delegate.thenAcceptBothAsync(other, action);
        }

        @Override
        public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other,
                                                               BiConsumer<? super T, ? super U> action,
                                                               Executor executor) {
            return delegate.thenAcceptBothAsync(other, action, executor);
        }

        @Override
        public CompletableFuture<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
            return delegate.runAfterBoth(other, action);
        }

        @Override
        public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other,
                                                         Runnable action) {
            return delegate.runAfterBothAsync(other, action);
        }

        @Override
        public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other,
                                                         Runnable action, Executor executor) {
            return delegate.runAfterBothAsync(other, action, executor);
        }

        @Override
        public <U> CompletableFuture<U> applyToEither(CompletionStage<? extends T> other,
                                                      Function<? super T, U> fn) {
            return delegate.applyToEither(other, fn);
        }

        @Override
        public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends T> other,
                                                           Function<? super T, U> fn) {
            return delegate.applyToEitherAsync(other, fn);
        }

        @Override
        public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends T> other,
                                                           Function<? super T, U> fn,
                                                           Executor executor) {
            return delegate.applyToEitherAsync(other, fn, executor);
        }

        @Override
        public CompletableFuture<Void> acceptEither(CompletionStage<? extends T> other,
                                                    Consumer<? super T> action) {
            return delegate.acceptEither(other, action);
        }

        @Override
        public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends T> other,
                                                         Consumer<? super T> action) {
            return delegate.acceptEitherAsync(other, action);
        }

        @Override
        public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends T> other,
                                                         Consumer<? super T> action, Executor executor) {
            return delegate.acceptEitherAsync(other, action, executor);
        }

        @Override
        public CompletableFuture<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
            return delegate.runAfterEither(other, action);
        }

        @Override
        public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other,
                                                           Runnable action) {
            return delegate.runAfterEitherAsync(other, action);
        }

        @Override
        public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other,
                                                           Runnable action, Executor executor) {
            return delegate.runAfterEitherAsync(other, action, executor);
        }

        @Override
        public <U> CompletableFuture<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn) {
            return delegate.thenCompose(fn);
        }

        @Override
        public <U> CompletableFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn) {
            return delegate.thenComposeAsync(fn);
        }

        @Override
        public <U> CompletableFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn,
                                                         Executor executor) {
            return delegate.thenComposeAsync(fn, executor);
        }

        @Override
        public CompletableFuture<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
            return delegate.whenComplete(action);
        }

        @Override
        public CompletableFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action) {
            return delegate.whenCompleteAsync(action);
        }

        @Override
        public CompletableFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action,
                                                      Executor executor) {
            return delegate.whenCompleteAsync(action, executor);
        }

        @Override
        public <U> CompletableFuture<U> handle(BiFunction<? super T, Throwable, ? extends U> fn) {
            return delegate.handle(fn);
        }

        @Override
        public <U> CompletableFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn) {
            return delegate.handleAsync(fn);
        }

        @Override
        public <U> CompletableFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn,
                                                    Executor executor) {
            return delegate.handleAsync(fn, executor);
        }

        @Override
        public CompletableFuture<T> toCompletableFuture() {
            return delegate.toCompletableFuture();
        }

        @Override
        public CompletableFuture<T> exceptionally(Function<Throwable, ? extends T> fn) {
            return delegate.exceptionally(fn);
        }

        @Override
        public boolean isCancelled() {
            return delegate.isCancelled();
        }

        @Override
        public boolean isCompletedExceptionally() {
            return delegate.isCompletedExceptionally();
        }

        @Override
        public void obtrudeValue(T value) {
            delegate.obtrudeValue(value);
        }

        @Override
        public void obtrudeException(Throwable ex) {
            delegate.obtrudeException(ex);
        }

        @Override
        public int getNumberOfDependents() {
            return delegate.getNumberOfDependents();
        }

        @Override
        public String toString() {
            return delegate.toString();
        }

        @Override
        public <U> CompletableFuture<U> newIncompleteFuture() {
            return delegate.newIncompleteFuture();
        }

        @Override
        public Executor defaultExecutor() {
            return delegate.defaultExecutor();
        }

        @Override
        public CompletableFuture<T> copy() {
            return delegate.copy();
        }

        @Override
        public CompletionStage<T> minimalCompletionStage() {
            return delegate.minimalCompletionStage();
        }

        @Override
        public CompletableFuture<T> completeAsync(Supplier<? extends T> supplier, Executor executor) {
            return delegate.completeAsync(supplier, executor);
        }

        @Override
        public CompletableFuture<T> completeAsync(Supplier<? extends T> supplier) {
            return delegate.completeAsync(supplier);
        }

        @Override
        public CompletableFuture<T> orTimeout(long timeout, TimeUnit unit) {
            return delegate.orTimeout(timeout, unit);
        }

        @Override
        public CompletableFuture<T> completeOnTimeout(T value, long timeout, TimeUnit unit) {
            return delegate.completeOnTimeout(value, timeout, unit);
        }
    }

}
