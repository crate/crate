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

import java.util.function.Consumer;

import org.jetbrains.annotations.Nullable;

import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.metadata.table.TableInfo;

public final class AnalyzedDropTable<T extends TableInfo> implements DDLStatement {

    private final boolean dropIfExists;

    @Nullable
    private final T tableInfo;

    private final RelationName tableName;

    AnalyzedDropTable(@Nullable T tableInfo, boolean dropIfExists, RelationName tableName) {
        this.tableInfo = tableInfo;
        this.dropIfExists = dropIfExists;
        this.tableName = tableName;
    }

    @Nullable
    public T table() {
        return tableInfo;
    }

    public boolean dropIfExists() {
        return dropIfExists;
    }

    public RelationName tableName() {
        return tableName;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> visitor, C context) {
        return visitor.visitDropTable(this, context);
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
    }
}
