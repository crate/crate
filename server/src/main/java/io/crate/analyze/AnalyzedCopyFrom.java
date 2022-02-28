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
import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.Table;
import io.crate.types.DataType;

import java.util.List;
import java.util.function.Consumer;

public class AnalyzedCopyFrom implements AnalyzedStatement {

    private final DocTableInfo tableInfo;
    private final List<String> targetColumns;
    private final Table<Symbol> table;
    private final GenericProperties<Symbol> properties;
    private final Symbol uri;

    AnalyzedCopyFrom(DocTableInfo tableInfo,
                     List<String> targetColumns,
                     Table<Symbol> table,
                     GenericProperties<Symbol> properties,
                     Symbol uri) {
        this.tableInfo = tableInfo;
        this.targetColumns = targetColumns;
        this.table = table;
        this.properties = properties;
        this.uri = uri;
    }

    public DocTableInfo tableInfo() {
        return tableInfo;
    }

    public List<String> targetColumns() {
        return targetColumns;
    }

    public GenericProperties<Symbol> properties() {
        return properties;
    }

    public Table<Symbol> table() {
        return table;
    }

    public Symbol uri() {
        return uri;
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
        for (var partitionProperty : table.partitionProperties()) {
            partitionProperty.expressions().forEach(consumer);
        }
        properties.properties().values().forEach(consumer);
        consumer.accept(uri);
    }

    @Override
    public boolean isWriteOperation() {
        return true;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitCopyFromStatement(this, context);
    }

    public static IllegalArgumentException raiseInvalidType(DataType dataType) {
        throw new IllegalArgumentException("fileUri must be of type STRING or STRING ARRAY. Got " + dataType);
    }
}
