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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <pre>{@code <relation> AS alias}</pre>
 *
 * This relation only provides a different name for an inner relation.
 * The {@link #outputs()} of the aliased-relation are {@link ScopedSymbol}s,
 * a 1:1 mapping of the outputs of the inner relation but associated with the aliased-relation.;
 */
public class AliasedAnalyzedRelation implements AnalyzedRelation, FieldResolver {

    private final AnalyzedRelation relation;
    private final RelationName alias;
    private final Map<ColumnIdent, ColumnIdent> aliasToColumnMapping;
    private final ArrayList<Symbol> outputs;
    private final ArrayList<ScopedSymbol> scopedSymbols;

    public AliasedAnalyzedRelation(AnalyzedRelation relation, RelationName alias) {
        this(relation, alias, List.of());
    }

    AliasedAnalyzedRelation(AnalyzedRelation relation, RelationName alias, List<String> columnAliases) {
        this.relation = relation;
        this.alias = alias;
        aliasToColumnMapping = new HashMap<>(columnAliases.size());
        this.outputs = new ArrayList<>(relation.outputs().size());
        this.scopedSymbols = new ArrayList<>(relation.outputs().size());
        for (int i = 0; i < relation.outputs().size(); i++) {
            Symbol childOutput = relation.outputs().get(i);
            ColumnIdent childColumn = Symbols.pathFromSymbol(childOutput);
            ColumnIdent columnAlias = childColumn;
            if (i < columnAliases.size()) {
                columnAlias = new ColumnIdent(columnAliases.get(i));
            }
            aliasToColumnMapping.put(columnAlias, childColumn);
            var scopedSymbol = new ScopedSymbol(alias, columnAlias, childOutput.valueType());
            outputs.add(scopedSymbol);
            scopedSymbols.add(scopedSymbol);
        }
    }

    @Override
    public Symbol getField(ColumnIdent column, Operation operation) throws UnsupportedOperationException, ColumnUnknownException {
        if (operation != Operation.READ) {
            throw new UnsupportedOperationException(operation + " is not supported on " + alias);
        }
        ColumnIdent childColumnName = aliasToColumnMapping.get(column);
        if (childColumnName == null) {
            if (column.isTopLevel()) {
                return null;
            }
            childColumnName = aliasToColumnMapping.get(column.getRoot());
            if (childColumnName == null) {
                // The column ident maybe a quoted subscript which points to an alias of a sub relation.
                // Aliases are always strings but due to the support for quoted subscript expressions,
                // the select column ident may already be expanded to a subscript.
                var maybeQuotedSubscriptColumnAlias = new ColumnIdent(column.sqlFqn());
                childColumnName = aliasToColumnMapping.get(maybeQuotedSubscriptColumnAlias);
                if (childColumnName == null) {
                    return null;
                }
                column = maybeQuotedSubscriptColumnAlias;
            } else {
                childColumnName = new ColumnIdent(childColumnName.name(), column.path());
            }
        }
        Symbol field = relation.getField(childColumnName, operation);
        if (field == null) {
            return null;
        }
        ScopedSymbol scopedSymbol = new ScopedSymbol(alias, column, field.valueType());

        // If the scopedSymbol exists already, return that instance.
        // Otherwise (e.g. lazy-loaded subscript expression) it must be stored to comply with
        // IdentityHashMap constraints.
        int i = scopedSymbols.indexOf(scopedSymbol);
        if (i >= 0) {
            return scopedSymbols.get(i);
        }
        scopedSymbols.add(scopedSymbol);
        return scopedSymbol;
    }

    public AnalyzedRelation relation() {
        return relation;
    }

    @Override
    public RelationName relationName() {
        return alias;
    }

    @Nonnull
    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public String toString() {
        return relation + " AS " + alias;
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitAliasedAnalyzedRelation(this, context);
    }

    @Nullable
    @Override
    public Symbol resolveField(ScopedSymbol field) {
        if (!field.relation().equals(alias)) {
            throw new IllegalArgumentException(field + " does not belong to " + relationName());
        }
        ColumnIdent column = field.column();
        ColumnIdent childColumnName = aliasToColumnMapping.get(column);
        if (childColumnName == null && !column.isTopLevel()) {
            var childCol = aliasToColumnMapping.get(column.getRoot());
            childColumnName = new ColumnIdent(childCol.name(), column.path());
        }
        assert childColumnName != null
            : "If a ScopedSymbol has been retrieved via `getField`, it must be possible to get the columnIdent";
        var result = relation.getField(childColumnName, Operation.READ);
        if (result == null) {
            throw new IllegalArgumentException(field + " does not belong to " + relationName());
        }
        return result;
    }
}
