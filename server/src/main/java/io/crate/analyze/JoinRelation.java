/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.exceptions.AmbiguousColumnException;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.table.Operation;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.sql.tree.JoinCriteria;

public class JoinRelation implements AnalyzedRelation {

    private final AnalyzedRelation left;
    private final AnalyzedRelation right;
    private final List<Symbol> outputs;
    private final Optional<JoinCriteria> joinCriteria;
    private final JoinType joinType;

    public JoinRelation(AnalyzedRelation left,
                        AnalyzedRelation right,
                        List<Symbol> outputs,
                        Optional<JoinCriteria> joinCriteria,
                        JoinType joinType) {
        this.outputs = outputs;
        this.left = left;
        this.right = right;
        this.joinCriteria = joinCriteria;
        this.joinType = joinType;
    }

    public AnalyzedRelation left() {
        return left;
    }

    public AnalyzedRelation right() {
        return right;
    }

    public Optional<JoinCriteria> joinCriteria() {
        return joinCriteria;
    }

    public JoinType joinType() {
        return joinType;
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitJoinRelation(this, context);
    }

    @Nullable
    @Override
    public Symbol getField(ColumnIdent column,
                           Operation operation,
                           boolean errorOnUnknownObjectKey) throws AmbiguousColumnException, ColumnUnknownException, UnsupportedOperationException {
        throw new UnsupportedOperationException("getField is not supported on JoinRelation");
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
        for (Symbol output : outputs) {
            consumer.accept(output);
        }
        left.visitSymbols(consumer);
        right.visitSymbols(consumer);
    }

    @Override
    public RelationName relationName() {
        throw new UnsupportedOperationException("relationName is not supported on JoinRelation");
    }

    @Nonnull
    @Override
    public List<Symbol> outputs() {
        return outputs;
    }
}
