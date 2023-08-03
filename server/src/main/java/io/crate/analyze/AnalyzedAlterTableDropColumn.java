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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.crate.data.Row;
import io.crate.execution.ddl.tables.DropColumnRequest;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.operators.SubQueryAndParamBinder;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.DropColumnDefinition;

public record AnalyzedAlterTableDropColumn(
    DocTableInfo table,
    Map<ColumnIdent, TableElementsAnalyzer.RefBuilder> columns) implements DDLStatement {

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> visitor, C context) {
        return visitor.visitAlterTableDropColumn(this, context);
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
    }

    public DropColumnRequest bind(NodeContext nodeCtx,
                                 CoordinatorTxnCtx txnCtx,
                                 Row params,
                                 SubQueryResults subQueryResults) {
        SubQueryAndParamBinder bindParameter = new SubQueryAndParamBinder(params, subQueryResults);
        Function<Symbol, Object> toValue = new SymbolEvaluator(txnCtx, nodeCtx, subQueryResults).bind(params);
        List<Reference> dropColumns = new ArrayList<>(columns.size());
        for (var refBuilder : columns.values()) {
            dropColumns.add(refBuilder.build(columns, table.ident(), bindParameter, toValue));
        }

        validate(table, dropColumns);
        return new DropColumnRequest(table.ident(), dropColumns);
    }

    /**
     * Validate that columns can be dropped. Non-existent columns with `IF EXISTS` are removed in
     * {@link TableElementsAnalyzer.PeekColumns#visitDropColumnDefinition(DropColumnDefinition, Void)}.
     */
    private static void validate(DocTableInfo tableInfo, List<Reference> dropColumns) {
        var generatedColRefs = new HashSet<>();
        for (var genRef : tableInfo.generatedColumns()) {
            generatedColRefs.addAll(genRef.referencedReferences());
        }
        var leftOverCols = tableInfo.columns().stream().map(Reference::column).collect(Collectors.toSet());

        for (int i = 0 ; i < dropColumns.size(); i++) {
            var refToDrop = dropColumns.get(i);
            var colToDrop = refToDrop.column();

            if (tableInfo.primaryKey().contains(colToDrop)) {
                throw new UnsupportedOperationException("Dropping column: " + colToDrop.sqlFqn() + " which " +
                                                        "is part of the PRIMARY KEY is not allowed");
            }

            if (tableInfo.clusteredBy().equals(colToDrop)) {
                throw new UnsupportedOperationException("Dropping column: " + colToDrop.sqlFqn() + " which " +
                                                        "is used in 'CLUSTERED BY' is not allowed");
            }

            if (tableInfo.isPartitioned() && tableInfo.partitionedBy().contains(colToDrop)) {
                throw new UnsupportedOperationException("Dropping column: " + colToDrop.sqlFqn() + " which " +
                                                        "is part of the 'PARTITIONED BY' columns is not allowed");
            }

            if (generatedColRefs.contains(refToDrop)) {
                throw new UnsupportedOperationException(
                    "Dropping column: " + colToDrop.sqlFqn() + " which is used to produce values for " +
                    "generated column is not allowed");
            }
            leftOverCols.remove(colToDrop);
        }

        if (leftOverCols.isEmpty()) {
            throw new UnsupportedOperationException("Dropping all columns of a table is not allowed");
        }
    }
}
