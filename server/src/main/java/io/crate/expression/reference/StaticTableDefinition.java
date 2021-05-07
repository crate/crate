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

package io.crate.expression.reference;

import io.crate.user.User;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.expressions.RowCollectExpressionFactory;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class StaticTableDefinition<T> {

    private final BiFunction<TransactionContext, User, CompletableFuture<? extends Iterable<T>>> recordsForUser;
    private final StaticTableReferenceResolver<T> referenceResolver;
    private final boolean involvesIO;

    public StaticTableDefinition(Supplier<CompletableFuture<? extends Iterable<T>>> iterable,
                                 Map<ColumnIdent, ? extends RowCollectExpressionFactory<T>> expressionFactories,
                                 boolean involvesIO) {
        this.recordsForUser = (t, u) -> iterable.get();
        this.referenceResolver = new StaticTableReferenceResolver<>(expressionFactories);
        this.involvesIO = involvesIO;
    }

    public StaticTableDefinition(Supplier<? extends Iterable<T>> iterable,
                                 Map<ColumnIdent, ? extends RowCollectExpressionFactory<T>> expressionFactories,
                                 BiFunction<TransactionContext, T, T> applyContext) {
        this.recordsForUser = (txnCtx, u) -> completedFuture(() ->
            StreamSupport.stream(iterable.get().spliterator(), false)
                .map(record -> applyContext.apply(txnCtx, record))
                .iterator());
        this.referenceResolver = new StaticTableReferenceResolver<>(expressionFactories);
        this.involvesIO = true;
    }

    public StaticTableDefinition(Supplier<? extends Iterable<T>> iterable,
                                 BiPredicate<User, T> predicate,
                                 Map<ColumnIdent, ? extends RowCollectExpressionFactory<T>> expressionFactories) {
        this.recordsForUser = (txnCtx, u) -> completedFuture(() -> StreamSupport.stream(iterable.get().spliterator(), false)
            .filter(t -> u == null || predicate.test(u, t)).iterator());
        this.referenceResolver = new StaticTableReferenceResolver<>(expressionFactories);
        this.involvesIO = true;
    }

    public StaticTableDefinition(Supplier<CompletableFuture<? extends Iterable<T>>> futureRecords,
                                 Map<ColumnIdent, ? extends RowCollectExpressionFactory<T>> expressionFactories,
                                 BiPredicate<User, T> predicate,
                                 boolean involvesIO) {
        this.recordsForUser = (txnCtx, user) ->
            futureRecords.get().thenApply((records) ->
                StreamSupport.stream(records.spliterator(), false)
                .filter(record -> user == null || predicate.test(user, record))
                ::iterator
            );
        this.referenceResolver = new StaticTableReferenceResolver<>(expressionFactories);
        this.involvesIO = involvesIO;
    }

    public CompletableFuture<? extends Iterable<T>> retrieveRecords(TransactionContext txnCtx, @Nullable User user) {
        return recordsForUser.apply(txnCtx, user);
    }

    public StaticTableReferenceResolver<T> getReferenceResolver() {
        return referenceResolver;
    }

    public boolean involvesIO() {
        return involvesIO;
    }
}
