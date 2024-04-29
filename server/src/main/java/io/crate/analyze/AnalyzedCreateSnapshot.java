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

import java.util.List;
import java.util.function.Consumer;

import io.crate.expression.symbol.Symbol;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.Table;

public class AnalyzedCreateSnapshot implements DDLStatement {

    public static final List<String> ALL_INDICES = List.of("*", "-.blob_*");

    private final String repositoryName;
    private final String snapshotName;
    private final List<Table<Symbol>> tables;
    private final GenericProperties<Symbol> properties;

    AnalyzedCreateSnapshot(String repositoryName,
                           String snapshotName,
                           List<Table<Symbol>> tables,
                           GenericProperties<Symbol> properties) {
        this.repositoryName = repositoryName;
        this.snapshotName = snapshotName;
        this.tables = tables;
        this.properties = properties;
    }

    public String repositoryName() {
        return repositoryName;
    }

    public String snapshotName() {
        return snapshotName;
    }


    public GenericProperties<Symbol> properties() {
        return properties;
    }

    public List<Table<Symbol>> tables() {
        return tables;
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
        for (var table : tables) {
            for (Assignment<Symbol> partitionProperty : table.partitionProperties()) {
                partitionProperty.expressions().forEach(consumer);
            }
        }
        properties.forValues(consumer);
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitCreateSnapshotAnalyzedStatement(this, context);
    }
}
