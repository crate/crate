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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Consumer;

import org.jspecify.annotations.Nullable;

import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;

public final class AnalyzedUpdateStatement implements AnalyzedStatement {

    private final AbstractTableRelation<?> table;
    private final LinkedHashMap<Reference, Symbol> assignmentByTargetCol;
    private final Symbol query;

    /**
     * List of values or expressions used to be retrieved from the updated rows.
     */
    @Nullable
    private final List<Symbol> returnValues;

    public AnalyzedUpdateStatement(AbstractTableRelation<?> table,
                                   LinkedHashMap<Reference, Symbol> assignmentByTargetCol,
                                   Symbol query,
                                   @Nullable List<Symbol> returnValues) {
        this.table = table;
        this.assignmentByTargetCol = assignmentByTargetCol;
        this.query = query;
        this.returnValues = returnValues;
    }

    public AbstractTableRelation<?> table() {
        return table;
    }

    public LinkedHashMap<Reference, Symbol> assignmentByTargetCol() {
        return assignmentByTargetCol;
    }

    public Symbol query() {
        return query;
    }

    @Nullable
    @Override
    public List<Symbol> outputs() {
        return returnValues;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> visitor, C context) {
        return visitor.visitAnalyzedUpdateStatement(this, context);
    }

    @Override
    public boolean isWriteOperation() {
        return true;
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
        consumer.accept(query);
        for (Symbol sourceExpr : assignmentByTargetCol.values()) {
            consumer.accept(sourceExpr);
        }
        if (returnValues != null) {
            for (Symbol returningSymbol : returnValues) {
                consumer.accept(returningSymbol);
            }
        }
    }
}
