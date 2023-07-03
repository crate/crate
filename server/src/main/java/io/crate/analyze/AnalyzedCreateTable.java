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

import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.CreateTable;
import io.crate.sql.tree.TableElement;

public class AnalyzedCreateTable implements DDLStatement {

    private final RelationName relationName;
    private final CreateTable<Symbol> createTable;
    private final AnalyzedTableElements<Symbol> analyzedTableElements;

    public AnalyzedCreateTable(RelationName relationName,
                               CreateTable<Symbol> createTable,
                               AnalyzedTableElements<Symbol> analyzedTableElements) {
        this.relationName = relationName;
        this.createTable = createTable;
        this.analyzedTableElements = analyzedTableElements;
    }

    public CreateTable<Symbol> createTable() {
        return createTable;
    }

    public RelationName relationName() {
        return relationName;
    }

    public AnalyzedTableElements<Symbol> analyzedTableElements() {
        return analyzedTableElements;
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
        for (Assignment<Symbol> partitionProperty : createTable.name().partitionProperties()) {
            consumer.accept(partitionProperty.expression());
            partitionProperty.expressions().forEach(consumer);
        }
        for (TableElement<Symbol> tableElement : createTable.tableElements()) {
            tableElement.visit(consumer);
        }
        createTable.clusteredBy().ifPresent(x -> {
            x.column().ifPresent(consumer);
            x.numberOfShards().ifPresent(consumer);
        });
        createTable.partitionedBy().ifPresent(x -> x.columns().forEach(consumer));
        createTable.properties().properties().values().forEach(consumer);
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> visitor, C context) {
        return visitor.visitCreateTable(this, context);
    }
}
