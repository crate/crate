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

package io.crate.analyze;

import java.util.function.Consumer;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.sql.tree.Query;
import io.crate.role.Role;

public final class CreateViewStmt implements AnalyzedStatement {

    private final RelationName name;
    private final AnalyzedRelation analyzedQuery;
    private final Query query;
    private final boolean replaceExisting;
    @Nullable
    private final Role owner;

    CreateViewStmt(RelationName name,
                   AnalyzedRelation analyzedQuery,
                   Query query,
                   boolean replaceExisting,
                   @Nullable Role owner) {
        this.name = name;
        this.analyzedQuery = analyzedQuery;
        this.query = query;
        this.replaceExisting = replaceExisting;
        this.owner = owner;
    }

    public Query query() {
        return query;
    }

    public RelationName name() {
        return name;
    }

    @VisibleForTesting
    public AnalyzedRelation analyzedQuery() {
        return analyzedQuery;
    }

    public boolean replaceExisting() {
        return replaceExisting;
    }

    @Nullable
    public Role owner() {
        return owner;
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
        analyzedQuery.visitSymbols(consumer);
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> visitor, C context) {
        return visitor.visitCreateViewStmt(this, context);
    }

    @Override
    public boolean isWriteOperation() {
        return true;
    }
}
