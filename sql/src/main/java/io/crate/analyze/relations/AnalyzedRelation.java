/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.analyze.relations;

import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.AnalyzedStatementVisitor;
import io.crate.analyze.HavingClause;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Consumer;

public interface AnalyzedRelation extends AnalyzedStatement {

    <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context);

    Field getField(ColumnIdent path, Operation operation) throws UnsupportedOperationException, ColumnUnknownException;

    QualifiedName getQualifiedName();

    /** * @return The outputs of the relation */
    List<Symbol> outputs();

    /**
     * @return WHERE clause of the relation.
     *         This is {@link WhereClause#MATCH_ALL} if there was no WhereClause in the statement
     */
    WhereClause where();

    /**
     * @return The GROUP BY keys. Empty if there are none.
     */
    List<Symbol> groupBy();

    /**
     * @return The HAVING clause or null
     */
    @Nullable
    HavingClause having();

    /**
     * @return ORDER BY clause or null if not present
     */
    @Nullable
    OrderBy orderBy();

    @Nullable
    Symbol limit();

    @Nullable
    Symbol offset();

    boolean hasAggregates();

    /**
     * Calls the consumer for each top-level symbol in the relation
     * (Arguments/children of function symbols are not visited)
     */
    @Override
    default void visitSymbols(Consumer<? super Symbol> consumer) {
        for (Symbol output : outputs()) {
            consumer.accept(output);
        }
        where().accept(consumer);
        for (Symbol groupKey : groupBy()) {
            consumer.accept(groupKey);
        }
        HavingClause having = having();
        if (having != null) {
            having.accept(consumer);
        }
        OrderBy orderBy = orderBy();
        if (orderBy != null) {
            orderBy.accept(consumer);
        }
        Symbol limit = limit();
        if (limit != null) {
            consumer.accept(limit);
        }
        Symbol offset = offset();
        if (offset != null) {
            consumer.accept(offset);
        }
    }

    @Override
    default <C, R> R accept(AnalyzedStatementVisitor<C, R> visitor, C context) {
        return visitor.visitSelectStatement(this, context);
    }

    @Override
    default boolean isWriteOperation() {
        return false;
    }

    @Override
    default boolean isUnboundPlanningSupported() {
        return true;
    }

    boolean isDistinct();
}
