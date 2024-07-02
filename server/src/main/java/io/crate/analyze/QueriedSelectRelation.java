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

import java.util.List;
import java.util.function.Consumer;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.JoinPair;
import io.crate.common.collections.Lists;
import io.crate.exceptions.AmbiguousColumnException;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.AliasSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.table.Operation;

public class QueriedSelectRelation implements AnalyzedRelation {

    private final List<AnalyzedRelation> from;
    private final List<JoinPair> joinPairs;
    private final boolean isDistinct;
    private final List<Symbol> outputs;
    private final Symbol whereClause;
    private final List<Symbol> groupBy;
    @Nullable
    private final Symbol having;
    @Nullable
    private final OrderBy orderBy;
    @Nullable
    private final Symbol offset;
    @Nullable
    private final Symbol limit;

    public QueriedSelectRelation(boolean isDistinct,
                                 List<AnalyzedRelation> from,
                                 List<JoinPair> joinPairs,
                                 List<Symbol> outputs,
                                 Symbol whereClause,
                                 List<Symbol> groupBy,
                                 @Nullable Symbol having,
                                 @Nullable OrderBy orderBy,
                                 @Nullable Symbol limit,
                                 @Nullable Symbol offset) {
        this.outputs = outputs;
        this.whereClause = whereClause;
        this.groupBy = groupBy;
        this.having = having;
        this.orderBy = orderBy;
        this.offset = offset;
        this.limit = limit;
        assert from.size() >= 1 : "QueriedSelectRelation must have at least 1 relation in FROM";
        this.isDistinct = isDistinct;
        this.from = from;
        this.joinPairs = joinPairs;
    }

    public List<AnalyzedRelation> from() {
        return from;
    }

    public Symbol getField(ColumnIdent column, Operation operation, boolean errorOnUnknownObjectKey) throws AmbiguousColumnException, ColumnUnknownException, UnsupportedOperationException {
        Symbol match = null;
        for (Symbol output : outputs()) {
            ColumnIdent outputName = output.toColumn();
            if (outputName.equals(column)) {
                if (match != null) {
                    throw new AmbiguousColumnException(column, output);
                }
                match = output;
            }
        }
        if (match != null || column.isRoot()) {
            return match;
        }
        ColumnIdent root = column.getRoot();

        // Try to optimize child-column access to use a Reference instead of a subscript function
        //
        // E.g.
        //
        //    SELECT obj['x'] FROM (select...)
        //
        // Should use Reference (obj.x) instead of Function subscript(obj, 'x')
        // Unless an alias shadows the sub-relation column:
        //
        //      SELECT obj['x'] FROM (SELECT unnest(obj) as obj FROM ...)
        //
        // -> Resolve both root field and child-field from source again.
        //    If the root field matches output -> it's not shadowed
        for (AnalyzedRelation source : from) {
            Symbol field = source.getField(column, operation, errorOnUnknownObjectKey);
            if (field != null) {
                if (match != null) {
                    throw new AmbiguousColumnException(column, match);
                }
                Symbol rootField = source.getField(root, operation, errorOnUnknownObjectKey);
                for (Symbol output : outputs()) {
                    Symbol symbol = output;
                    while (symbol instanceof AliasSymbol alias) {
                        symbol = alias.symbol();
                    }
                    if (symbol.equals(rootField) || output.equals(rootField)) {
                        match = field;
                        break;
                    }
                }
            }
        }
        return match;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitQueriedSelectRelation(this, context);
    }

    @Override
    public RelationName relationName() {
        throw new UnsupportedOperationException(
            "QueriedSelectRelation has no name. It must be beneath an aliased-relation to be addressable by name");
    }

    @NotNull
    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    public Symbol where() {
        return whereClause;
    }

    public List<Symbol> groupBy() {
        return groupBy;
    }

    @Nullable
    public Symbol having() {
        return having;
    }

    @Nullable
    public OrderBy orderBy() {
        return orderBy;
    }

    @Nullable
    public Symbol limit() {
        return limit;
    }

    @Nullable
    public Symbol offset() {
        return offset;
    }

    @Override
    public String toString() {
        return "SELECT "
               + Lists.joinOn(", ", outputs(), x -> x.toColumn().sqlFqn())
               + " FROM ("
               + Lists.joinOn(", ", from, x -> x.relationName().toString())
               + ')';
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
        for (Symbol output : outputs) {
            consumer.accept(output);
        }
        consumer.accept(whereClause);
        for (Symbol groupKey : groupBy) {
            consumer.accept(groupKey);
        }
        if (having != null) {
            consumer.accept(having);
        }
        if (orderBy != null) {
            orderBy.accept(consumer);
        }
        if (limit != null) {
            consumer.accept(limit);
        }
        if (offset != null) {
            consumer.accept(offset);
        }
        for (var joinPair : joinPairs) {
            if (joinPair.condition() != null) {
                consumer.accept(joinPair.condition());
            }
        }
    }

    public List<JoinPair> joinPairs() {
        return joinPairs;
    }
}
