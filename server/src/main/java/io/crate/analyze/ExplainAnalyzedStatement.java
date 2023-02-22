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

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.exceptions.scoped.table.AmbiguousColumnException;
import io.crate.exceptions.scoped.table.ColumnUnknownException;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.table.Operation;
import io.crate.profile.ProfilingContext;
import io.crate.types.DataTypes;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

public class ExplainAnalyzedStatement implements AnalyzedStatement, AnalyzedRelation {

    final AnalyzedStatement statement;
    private final ProfilingContext context;
    private final List<Symbol> outputs;
    private final RelationName relationName;

    ExplainAnalyzedStatement(String columnName, AnalyzedStatement statement, @Nullable ProfilingContext context) {
        relationName = new RelationName(null, "explain");
        ScopedSymbol field = new ScopedSymbol(
            relationName,
            new ColumnIdent(columnName),
            context == null ? DataTypes.STRING : DataTypes.UNTYPED_OBJECT
        );
        this.statement = statement;
        this.context = context;
        this.outputs = List.of(field);
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitExplainStatement(this, context);
    }

    public AnalyzedStatement statement() {
        return statement;
    }

    @Nullable
    public ProfilingContext context() {
        return context;
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitExplain(this, context);
    }

    @Override
    public Symbol getField(ColumnIdent column, Operation operation, boolean errorOnUnknownObjectKey) throws AmbiguousColumnException, ColumnUnknownException, UnsupportedOperationException {
        throw new UnsupportedOperationException("Cannot use getField on " + getClass().getSimpleName());
    }

    @Override
    public boolean isWriteOperation() {
        return false;
    }

    @Override
    public RelationName relationName() {
        return relationName;
    }

    @Nonnull
    @Override
    public List<Symbol> outputs() {
        return outputs;
    }
}
