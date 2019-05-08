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

package io.crate.analyze;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.OutputName;
import io.crate.metadata.Path;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.DataTypes;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

public class ShowCreateTableAnalyzedStatement implements AnalyzedStatement, AnalyzedRelation {

    private final DocTableInfo tableInfo;
    private final List<Field> fields;

    public ShowCreateTableAnalyzedStatement(DocTableInfo tableInfo) {
        String columnName = String.format(Locale.ENGLISH, "SHOW CREATE TABLE %s", tableInfo.ident().fqn());
        this.fields = Collections.singletonList(new Field(this, new OutputName(columnName), DataTypes.STRING));
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
        return visitor.process(this, context);
    }

    @Override
    public Field getField(Path path, Operation operation) throws UnsupportedOperationException, ColumnUnknownException {
        throw new UnsupportedOperationException("getWritableField() is not supported on ShowCreateTableAnalyzedStatement");
    }

    @Override
    public List<Field> fields() {
        return fields;
    }

    @Override
    public boolean isWriteOperation() {
        return false;
    }

    @Override
    public QualifiedName getQualifiedName() {
        throw new UnsupportedOperationException("method not supported");
    }

    @Override
    public void setQualifiedName(@Nonnull QualifiedName qualifiedName) {
        throw new UnsupportedOperationException("method not supported");
    }

    @Override
    public List<Symbol> outputs() {
        return List.copyOf(fields);
    }

    @Override
    public WhereClause where() {
        return WhereClause.MATCH_ALL;
    }

    @Override
    public List<Symbol> groupBy() {
        return List.of();
    }

    @Nullable
    @Override
    public HavingClause having() {
        return null;
    }

    @Nullable
    @Override
    public OrderBy orderBy() {
        return null;
    }

    @Nullable
    @Override
    public Symbol limit() {
        return null;
    }

    @Nullable
    @Override
    public Symbol offset() {
        return null;
    }

    @Override
    public boolean hasAggregates() {
        return false;
    }

    @Override
    public boolean isUnboundPlanningSupported() {
        return true;
    }

    @Override
    public boolean isDistinct() {
        return false;
    }
}
