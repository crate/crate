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

import io.crate.expression.symbol.Symbol;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.Table;

import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Consumer;

public class AnalyzedCopyTo implements AnalyzedStatement {

    private final TableInfo tableInfo;
    private final Table<Symbol> table;
    private final Symbol uri;
    private final GenericProperties<Symbol> properties;
    private final List<Symbol> columns;
    @Nullable
    private final Symbol whereClause;
    private boolean waitForCompletion;

    AnalyzedCopyTo(TableInfo tableInfo,
                   Table<Symbol> table,
                   Symbol uri,
                   GenericProperties<Symbol> properties,
                   List<Symbol> columns,
                   @Nullable Symbol whereClause) {
        this.tableInfo = tableInfo;
        this.table = table;
        this.uri = uri;
        this.properties = properties;
        this.columns = columns;
        this.whereClause = whereClause;
    }

    public TableInfo tableInfo() {
        return tableInfo;
    }

    public Table<Symbol> table() {
        return table;
    }

    public Symbol uri() {
        return uri;
    }

    public GenericProperties<Symbol> properties() {
        return properties;
    }

    public List<Symbol> columns() {
        return columns;
    }

    @Nullable
    public Symbol whereClause() {
        return whereClause;
    }

    public boolean waitForCompletion() {
        return waitForCompletion;
    }

    public void setWaitForCompletion(boolean waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitCopyToStatement(this, context);
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
        for (var partitionProperty : table.partitionProperties()) {
            consumer.accept(partitionProperty.columnName());
            partitionProperty.expressions().forEach(consumer);
        }
        columns.forEach(consumer);
        if (whereClause != null) {
            consumer.accept(whereClause);
        }
        consumer.accept(uri);
        properties.properties().values().forEach(consumer);
    }

    @Override
    public boolean isWriteOperation() {
        return false;
    }
}
