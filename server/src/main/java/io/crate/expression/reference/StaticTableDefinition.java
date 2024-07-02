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

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiPredicate;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

import org.jetbrains.annotations.Nullable;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.role.Role;

public class StaticTableDefinition<T> {

    private final GetRecords<T> getRecords;
    private final StaticTableReferenceResolver<T> referenceResolver;
    private final boolean involvesIO;

    @FunctionalInterface
    public interface GetRecords<T> {
        CompletableFuture<? extends Iterable<T>> get(TransactionContext txtContext, Role user);
    }


    public StaticTableDefinition(Supplier<CompletableFuture<? extends Iterable<T>>> iterable,
                                 Map<ColumnIdent, ? extends RowCollectExpressionFactory<T>> expressionFactories,
                                 boolean involvesIO) {
        this.getRecords = (t, u) -> iterable.get();
        this.referenceResolver = new StaticTableReferenceResolver<>(expressionFactories);
        this.involvesIO = involvesIO;
    }

    public StaticTableDefinition(Supplier<? extends Iterable<T>> iterable,
                                 BiPredicate<Role, T> predicate,
                                 Map<ColumnIdent, ? extends RowCollectExpressionFactory<T>> expressionFactories) {
        this.getRecords = (txnCtx, u) -> completedFuture(() -> StreamSupport.stream(iterable.get().spliterator(), false)
            .filter(t -> u == null || predicate.test(u, t)).iterator());
        this.referenceResolver = new StaticTableReferenceResolver<>(expressionFactories);
        this.involvesIO = true;
    }

    public StaticTableDefinition(Supplier<CompletableFuture<? extends Iterable<T>>> futureRecords,
                                 Map<ColumnIdent, ? extends RowCollectExpressionFactory<T>> expressionFactories,
                                 BiPredicate<Role, T> predicate,
                                 boolean involvesIO) {
        this.getRecords = (txnCtx, user) ->
            futureRecords.get().thenApply(records ->
                StreamSupport.stream(records.spliterator(), false)
                .filter(r -> user == null || predicate.test(user, r))
                ::iterator
            );
        this.referenceResolver = new StaticTableReferenceResolver<>(expressionFactories);
        this.involvesIO = involvesIO;
    }

    public StaticTableDefinition(GetRecords<T> getRecords,
                                 Map<ColumnIdent, ? extends RowCollectExpressionFactory<T>> expressionFactories,
                                 boolean involvesIO) {
        this.getRecords = getRecords;
        this.referenceResolver = new StaticTableReferenceResolver<>(expressionFactories);
        this.involvesIO = involvesIO;
    }

    public CompletableFuture<? extends Iterable<T>> retrieveRecords(TransactionContext txnCtx, @Nullable Role user) {
        return getRecords.get(txnCtx, user);
    }

    public StaticTableReferenceResolver<T> getReferenceResolver() {
        return referenceResolver;
    }

    public boolean involvesIO() {
        return involvesIO;
    }
}
