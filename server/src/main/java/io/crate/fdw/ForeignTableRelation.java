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

package io.crate.fdw;

import java.util.List;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.exceptions.AmbiguousColumnException;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.table.Operation;

public class ForeignTableRelation implements AnalyzedRelation {

    private final ForeignTable table;

    public ForeignTableRelation(ForeignTable table) {
        this.table = table;
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitForeignTable(this, context);
    }

    @Override
    @Nullable
    public Symbol getField(ColumnIdent column,
                           Operation operation,
                           boolean errorOnUnknownObjectKey)
            throws AmbiguousColumnException, ColumnUnknownException, UnsupportedOperationException {

        if (operation != Operation.READ) {
            throw new UnsupportedOperationException("Cannot write or delete on foreign tables");
        }
        Reference reference = table.references().get(column);
        if (reference == null) {
            throw new ColumnUnknownException(column, table.name());
        }
        return reference;
    }

    @Override
    public RelationName relationName() {
        return table.name();
    }

    @Override
    public @NotNull List<Symbol> outputs() {
        return List.copyOf(table.references().values());
    }
}
