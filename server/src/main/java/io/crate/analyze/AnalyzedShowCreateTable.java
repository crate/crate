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
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.types.DataTypes;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;

public class AnalyzedShowCreateTable implements AnalyzedStatement, AnalyzedRelation {

    private final DocTableInfo tableInfo;
    private final List<ScopedSymbol> fields;
    private final RelationName relationName;

    public AnalyzedShowCreateTable(DocTableInfo tableInfo) {
        String columnName = "SHOW CREATE TABLE " + tableInfo.ident().fqn();
        relationName = new RelationName(null, "SHOW CREATE TABLE");
        this.fields = Collections.singletonList(new ScopedSymbol(relationName, new ColumnIdent(columnName), DataTypes.STRING));
        this.tableInfo = tableInfo;
    }

    public DocTableInfo tableInfo() {
        return tableInfo;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitShowCreateTableAnalyzedStatement(this, context);
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitShowCreateTable(this, context);
    }

    @Override
    public Symbol getField(ColumnIdent column, Operation operation) throws UnsupportedOperationException, ColumnUnknownException {
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
        return List.copyOf(fields);
    }
}
