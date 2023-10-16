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

import static io.crate.types.DataTypes.merge;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.elasticsearch.common.UUIDs;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.common.collections.Lists;
import io.crate.exceptions.AmbiguousColumnException;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.JoinType;

/**
 * Join Relation represents a Join containing its lhs and rhs as {@link AnalyzedRelation'}.
 */
public class JoinRelation implements AnalyzedRelation {

    private final AnalyzedRelation left;
    private final AnalyzedRelation right;
    private final List<ScopedSymbol> outputs;
    private final JoinType joinType;
    private final Symbol joinCondition;
    private final RelationName name;

    public JoinRelation(AnalyzedRelation left,
                        AnalyzedRelation right,
                        JoinType joinType,
                        @Nullable Symbol joinCondition) {
        this.left = left;
        this.right = right;
        this.joinType = joinType;
        this.joinCondition = joinCondition;
        this.name = new RelationName(null, UUIDs.dirtyUUID().toString());
        ArrayList<ScopedSymbol> outputs = new ArrayList<>();
        for (int i = 0; i < left.outputs().size(); i++) {
            var l = left.outputs().get(i);
            outputs.add(new ScopedSymbol(name, Symbols.pathFromSymbol(l), merge(l.valueType(), l.valueType())));

        }
        for (int i = 0; i < right.outputs().size(); i++) {
            var r = right.outputs().get(i);
            outputs.add(new ScopedSymbol(name, Symbols.pathFromSymbol(r), merge(r.valueType(), r.valueType())));
        }
        this.outputs = List.copyOf(outputs);
    }

    public AnalyzedRelation left() {
        return left;
    }

    public AnalyzedRelation right() {
        return right;
    }

    public JoinType joinType() {
        return joinType;
    }

    @Nullable
    public Symbol joinCondition() {
        return joinCondition;
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitJoinRelation(this, context);
    }

    @Override
    public Symbol getField(ColumnIdent column, Operation operation, boolean errorOnUnknownObjectKey) throws AmbiguousColumnException, ColumnUnknownException, UnsupportedOperationException {
        for (var output : outputs) {
            if (output.column().equals(column)) {
                return output;
            }
        }
        return null;
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
        for (Symbol output : outputs) {
            consumer.accept(output);
        }
        left.visitSymbols(consumer);
        right.visitSymbols(consumer);
        if (joinCondition != null) {
            consumer.accept(joinCondition);
        }
    }

    @Override
    public RelationName relationName() {
        return name;
    }

    @NotNull
    @Override
    public List<Symbol> outputs() {
        return (List<Symbol>)(List) outputs;
    }

}
