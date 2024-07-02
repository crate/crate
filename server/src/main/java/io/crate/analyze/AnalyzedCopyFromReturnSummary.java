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
import io.crate.exceptions.AmbiguousColumnException;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.Table;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

import org.jetbrains.annotations.NotNull;
import java.util.List;

public class AnalyzedCopyFromReturnSummary extends AnalyzedCopyFrom implements AnalyzedRelation {

    private final List<ScopedSymbol> fields;

    AnalyzedCopyFromReturnSummary(DocTableInfo tableInfo,
                                  List<String> targetColumns,
                                  Table<Symbol> table,
                                  GenericProperties<Symbol> properties,
                                  Symbol uri) {
        super(tableInfo, targetColumns, table, properties, uri);
        this.fields = List.of(
            new ScopedSymbol(tableInfo.ident(), ColumnIdent.of("node"), ObjectType.builder()
                .setInnerType("id", DataTypes.STRING)
                .setInnerType("name", DataTypes.STRING)
                .build()),
            new ScopedSymbol(tableInfo.ident(), ColumnIdent.of("uri"), DataTypes.STRING),
            new ScopedSymbol(tableInfo.ident(), ColumnIdent.of("success_count"), DataTypes.LONG),
            new ScopedSymbol(tableInfo.ident(), ColumnIdent.of("error_count"), DataTypes.LONG),
            new ScopedSymbol(tableInfo.ident(), ColumnIdent.of("errors"), DataTypes.UNTYPED_OBJECT)
        );
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        throw new UnsupportedOperationException(
            getClass().getCanonicalName() + " is virtual relation, visiting it is unsupported");
    }

    @Override
    public Symbol getField(ColumnIdent column, Operation operation, boolean errorOnUnknownObjectKey) throws AmbiguousColumnException, ColumnUnknownException, UnsupportedOperationException {
        throw new UnsupportedOperationException("Cannot use getField on " + getClass().getSimpleName());
    }

    @Override
    public RelationName relationName() {
        return tableInfo().ident();
    }

    @NotNull
    @Override
    public List<Symbol> outputs() {
        return List.copyOf(fields);
    }
}
