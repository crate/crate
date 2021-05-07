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

package io.crate.analyze.relations;

import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.table.Operation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * A view is a stored SELECT statement.
 *
 * This is semantically equivalent to {@code <relation> AS <viewName>} with the
 * addition of special privilege behavior (Users can be granted privileges on a view)
 **/
public final class AnalyzedView implements AnalyzedRelation, FieldResolver {

    private final RelationName name;
    private final String owner;
    private final AnalyzedRelation relation;
    private final List<Symbol> outputSymbols;

    public AnalyzedView(RelationName name, String owner, AnalyzedRelation relation) {
        this.name = name;
        this.owner = owner;
        this.relation = relation;
        var childOutputs = relation.outputs();
        ArrayList<Symbol> outputs = new ArrayList<>(childOutputs.size());
        for (int i = 0; i < childOutputs.size(); i++) {
            var output = childOutputs.get(i);
            var column = Symbols.pathFromSymbol(output);
            outputs.add(new ScopedSymbol(name, column, output.valueType()));
        }
        this.outputSymbols = List.copyOf(outputs);
    }

    public String owner() {
        return owner;
    }

    public AnalyzedRelation relation() {
        return relation;
    }

    public RelationName name() {
        return name;
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitView(this, context);
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
        relation.visitSymbols(consumer);
    }

    @Override
    public Symbol getField(ColumnIdent column, Operation operation) throws UnsupportedOperationException, ColumnUnknownException {
        Symbol field = relation.getField(column, operation);
        if (field == null) {
            return null;
        }
        ScopedSymbol scopedSymbol = new ScopedSymbol(name, column, field.valueType());
        int i = outputSymbols.indexOf(scopedSymbol);
        if (i >= 0) {
            return outputSymbols.get(i);
        }
        return scopedSymbol;
    }

    @Override
    public RelationName relationName() {
        return name;
    }

    @Nonnull
    @Override
    public List<Symbol> outputs() {
        return outputSymbols;
    }

    @Override
    public String toString() {
        return "AnalyzedView{" + "qualifiedName=" + name +
               ", relation=" + relation +
               '}';
    }

    @Nullable
    @Override
    public Symbol resolveField(ScopedSymbol field) {
        if (!field.relation().equals(name)) {
            throw new IllegalArgumentException(field + " does not belong to " + name);
        }
        return relation.getField(field.column(), Operation.READ);
    }
}
