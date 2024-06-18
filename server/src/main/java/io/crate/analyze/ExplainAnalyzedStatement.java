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

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.exceptions.AmbiguousColumnException;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.table.Operation;
import io.crate.profile.ProfilingContext;
import io.crate.sql.tree.Explain;
import io.crate.types.DataTypes;

public class ExplainAnalyzedStatement implements AnalyzedRelation {

    final AnalyzedStatement statement;
    private final ProfilingContext context;
    private final List<Symbol> outputs;
    private final RelationName relationName;
    private final EnumSet<Explain.Option> options;

    private static final String PLAN_COLUMN_NAME = "QUERY PLAN";
    private static final String STEP_COLUMN_NAME = "STEP";

    ExplainAnalyzedStatement(AnalyzedStatement statement,
                             @Nullable ProfilingContext context,
                             EnumSet<Explain.Option> options) {
        relationName = new RelationName(null, "explain");
        this.statement = statement;
        this.context = context;
        this.outputs = new ArrayList<>();
        this.options = options;
        if (options.contains(Explain.Option.VERBOSE)) {
            ScopedSymbol stepField = new ScopedSymbol(
                relationName,
                ColumnIdent.of(STEP_COLUMN_NAME),
                DataTypes.STRING
            );
            outputs.add(stepField);
        }
        ScopedSymbol queryPlanField = new ScopedSymbol(
            relationName,
            ColumnIdent.of(PLAN_COLUMN_NAME),
            context == null ? DataTypes.STRING : DataTypes.UNTYPED_OBJECT
        );
        outputs.add(queryPlanField);
    }

    public boolean showCosts() {
        return options.contains(Explain.Option.COSTS);
    }

    public boolean verbose() {
        return options.contains(Explain.Option.VERBOSE);
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

    @NotNull
    @Override
    public List<Symbol> outputs() {
        return outputs;
    }
}
