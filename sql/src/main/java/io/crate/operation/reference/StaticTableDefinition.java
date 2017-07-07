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

package io.crate.operation.reference;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.expressions.RowCollectExpressionFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class StaticTableDefinition<T> {

    private final Supplier<CompletableFuture<? extends Iterable<T>>> iterable;
    private final StaticTableReferenceResolver<T> referenceResolver;

    public StaticTableDefinition(Supplier<CompletableFuture<? extends Iterable<T>>> iterable,
                                 Map<ColumnIdent, ? extends RowCollectExpressionFactory<T>> expressionFactories) {
        this.iterable = iterable;
        this.referenceResolver = new StaticTableReferenceResolver<>(expressionFactories);
    }

    public Supplier<CompletableFuture<? extends Iterable<T>>> getIterable() {
        return iterable;
    }

    public StaticTableReferenceResolver<T> getReferenceResolver() {
        return referenceResolver;
    }
}
