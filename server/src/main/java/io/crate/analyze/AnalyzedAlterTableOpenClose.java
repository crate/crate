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
import io.crate.sql.tree.Table;

import java.util.function.Consumer;

public class AnalyzedAlterTableOpenClose implements DDLStatement {


    private final DocTableInfo tableInfo;
    private final Table<Symbol> table;
    private final boolean openTable;


    AnalyzedAlterTableOpenClose(DocTableInfo tableInfo,
                                Table<Symbol> table,
                                boolean openTable) {
        this.tableInfo = tableInfo;
        this.table = table;
        this.openTable = openTable;
    }

    public DocTableInfo tableInfo() {
        return tableInfo;
    }

    public Table<Symbol> table() {
        return table;
    }

    public boolean isOpenTable() {
        return openTable;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitAnalyzedAlterTableOpenClose(this, context);
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
        for (var partitionProperty : table.partitionProperties()) {
            consumer.accept(partitionProperty.columnName());
            partitionProperty.expressions().forEach(consumer);
        }
    }
}
