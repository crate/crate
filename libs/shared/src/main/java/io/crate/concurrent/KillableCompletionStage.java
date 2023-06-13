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

package io.crate.concurrent;

import io.crate.data.Killable;

import org.jetbrains.annotations.NotNull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public final class KillableCompletionStage<T> implements CompletionStage<T>, Killable {

    public static <T> KillableCompletionStage<T> whenKilled(CompletionStage<T> delegate,
                                                            Consumer<Throwable> onKill) {
        return new KillableCompletionStage<>(delegate, onKill);
    }

    public static <T> KillableCompletionStage<T> failed(Throwable t) {
        return new KillableCompletionStage<>(CompletableFuture.failedFuture(t), ignored -> {});
    }

    private final CompletionStage<T> delegate;
    private final Consumer<Throwable> onKill;

    private KillableCompletionStage(CompletionStage<T> delegate, Consumer<Throwable> onKill) {
        this.delegate = delegate;
        this.onKill = onKill;
    }

    @Override
    public void kill(@NotNull Throwable throwable) {
        onKill.accept(throwable);
    }

    @Override
    public <U> KillableCompletionStage<U> thenApply(Function<? super T, ? extends U> fn) {
        return whenKilled(delegate.thenApply(fn), onKill);
    }

    @Override
    public <U> KillableCompletionStage<U> thenApplyAsync(Function<? super T, ? extends U> fn) {
        return whenKilled(delegate.thenApplyAsync(fn), onKill);
    }

    @Override
    public <U> KillableCompletionStage<U> thenApplyAsync(Function<? super T, ? extends U> fn,
                                                         Executor executor) {
        return whenKilled(delegate.thenApplyAsync(fn, executor), onKill);
    }

    @Override
    public KillableCompletionStage<Void> thenAccept(Consumer<? super T> action) {
        return whenKilled(delegate.thenAccept(action), onKill);
    }

    @Override
    public KillableCompletionStage<Void> thenAcceptAsync(Consumer<? super T> action) {
        return whenKilled(delegate.thenAcceptAsync(action), onKill);
    }

    @Override
    public KillableCompletionStage<Void> thenAcceptAsync(Consumer<? super T> action, Executor executor) {
        return whenKilled(delegate.thenAcceptAsync(action, executor), onKill);
    }

    @Override
    public KillableCompletionStage<Void> thenRun(Runnable action) {
        return whenKilled(delegate.thenRun(action), onKill);
    }

    @Override
    public KillableCompletionStage<Void> thenRunAsync(Runnable action) {
        return whenKilled(delegate.thenRunAsync(action), onKill);
    }

    @Override
    public KillableCompletionStage<Void> thenRunAsync(Runnable action, Executor executor) {
        return whenKilled(delegate.thenRunAsync(action, executor), onKill);
    }

    @Override
    public <U, V> KillableCompletionStage<V> thenCombine(CompletionStage<? extends U> other,
                                                         BiFunction<? super T, ? super U, ? extends V> fn) {
        return whenKilled(delegate.thenCombine(other, fn), onKill);
    }

    @Override
    public <U, V> KillableCompletionStage<V> thenCombineAsync(CompletionStage<? extends U> other,
                                                              BiFunction<? super T, ? super U, ? extends V> fn) {
        return whenKilled(delegate.thenCombineAsync(other, fn), onKill);
    }

    @Override
    public <U, V> KillableCompletionStage<V> thenCombineAsync(CompletionStage<? extends U> other,
                                                              BiFunction<? super T, ? super U, ? extends V> fn,
                                                              Executor executor) {
        return whenKilled(delegate.thenCombineAsync(other, fn, executor), onKill);
    }

    @Override
    public <U> KillableCompletionStage<Void> thenAcceptBoth(CompletionStage<? extends U> other,
                                                            BiConsumer<? super T, ? super U> action) {
        return whenKilled(delegate.thenAcceptBoth(other, action), onKill);
    }

    @Override
    public <U> KillableCompletionStage<Void> thenAcceptBothAsync(CompletionStage<? extends U> other,
                                                                 BiConsumer<? super T, ? super U> action) {
        return whenKilled(delegate.thenAcceptBothAsync(other, action), onKill);
    }

    @Override
    public <U> KillableCompletionStage<Void> thenAcceptBothAsync(CompletionStage<? extends U> other,
                                                                 BiConsumer<? super T, ? super U> action,
                                                                 Executor executor) {
        return whenKilled(delegate.thenAcceptBothAsync(other, action, executor), onKill);
    }

    @Override
    public KillableCompletionStage<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
        return whenKilled(delegate.runAfterBoth(other, action), onKill);
    }

    @Override
    public KillableCompletionStage<Void> runAfterBothAsync(CompletionStage<?> other,
                                                           Runnable action) {
        return whenKilled(delegate.runAfterBothAsync(other, action), onKill);
    }

    @Override
    public KillableCompletionStage<Void> runAfterBothAsync(CompletionStage<?> other,
                                                           Runnable action, Executor executor) {
        return whenKilled(delegate.runAfterBothAsync(other, action, executor), onKill);
    }

    @Override
    public <U> KillableCompletionStage<U> applyToEither(CompletionStage<? extends T> other,
                                                        Function<? super T, U> fn) {
        return whenKilled(delegate.applyToEither(other, fn), onKill);
    }

    @Override
    public <U> KillableCompletionStage<U> applyToEitherAsync(CompletionStage<? extends T> other,
                                                             Function<? super T, U> fn) {
        return whenKilled(delegate.applyToEitherAsync(other, fn), onKill);
    }

    @Override
    public <U> KillableCompletionStage<U> applyToEitherAsync(CompletionStage<? extends T> other,
                                                             Function<? super T, U> fn,
                                                             Executor executor) {
        return whenKilled(delegate.applyToEitherAsync(other, fn, executor), onKill);
    }

    @Override
    public KillableCompletionStage<Void> acceptEither(CompletionStage<? extends T> other,
                                                      Consumer<? super T> action) {
        return whenKilled(delegate.acceptEither(other, action), onKill);
    }

    @Override
    public KillableCompletionStage<Void> acceptEitherAsync(CompletionStage<? extends T> other,
                                                           Consumer<? super T> action) {
        return whenKilled(delegate.acceptEitherAsync(other, action), onKill);
    }

    @Override
    public KillableCompletionStage<Void> acceptEitherAsync(CompletionStage<? extends T> other,
                                                           Consumer<? super T> action, Executor executor) {
        return whenKilled(delegate.acceptEitherAsync(other, action, executor), onKill);
    }

    @Override
    public KillableCompletionStage<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
        return whenKilled(delegate.runAfterEither(other, action), onKill);
    }

    @Override
    public KillableCompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other,
                                                             Runnable action) {
        return whenKilled(delegate.runAfterEitherAsync(other, action), onKill);
    }

    @Override
    public KillableCompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other,
                                                             Runnable action, Executor executor) {
        return whenKilled(delegate.runAfterEitherAsync(other, action, executor), onKill);
    }

    @Override
    public <U> KillableCompletionStage<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn) {
        return whenKilled(delegate.thenCompose(fn), onKill);
    }

    @Override
    public <U> KillableCompletionStage<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn) {
        return whenKilled(delegate.thenComposeAsync(fn), onKill);
    }

    @Override
    public <U> KillableCompletionStage<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn,
                                                           Executor executor) {
        return whenKilled(delegate.thenComposeAsync(fn, executor), onKill);
    }

    @Override
    public KillableCompletionStage<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
        return whenKilled(delegate.whenComplete(action), onKill);
    }

    @Override
    public KillableCompletionStage<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action) {
        return whenKilled(delegate.whenCompleteAsync(action), onKill);
    }

    @Override
    public KillableCompletionStage<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action,
                                                        Executor executor) {
        return whenKilled(delegate.whenCompleteAsync(action, executor), onKill);
    }

    @Override
    public <U> KillableCompletionStage<U> handle(BiFunction<? super T, Throwable, ? extends U> fn) {
        return whenKilled(delegate.handle(fn), onKill);
    }

    @Override
    public <U> KillableCompletionStage<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn) {
        return whenKilled(delegate.handleAsync(fn), onKill);
    }

    @Override
    public <U> KillableCompletionStage<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn,
                                                      Executor executor) {
        return whenKilled(delegate.handleAsync(fn, executor), onKill);
    }

    @Override
    public CompletableFuture<T> toCompletableFuture() {
        return delegate.toCompletableFuture();
    }

    @Override
    public KillableCompletionStage<T> exceptionally(Function<Throwable, ? extends T> fn) {
        return whenKilled(delegate.exceptionally(fn), onKill);
    }
}
