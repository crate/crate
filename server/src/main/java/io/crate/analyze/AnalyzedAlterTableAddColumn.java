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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import com.carrotsearch.hppc.IntArrayList;

import io.crate.analyze.TableElementsAnalyzer.RefBuilder;
import io.crate.data.Row;
import io.crate.execution.ddl.tables.AddColumnRequest;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.operators.SubQueryAndParamBinder;
import io.crate.planner.operators.SubQueryResults;

public record AnalyzedAlterTableAddColumn(
        DocTableInfo table,
        /**
         * In order of definition
         */
        Map<ColumnIdent, RefBuilder> columns,
        /**
         * By constraint name; In order of definition
         **/
        Map<String, AnalyzedCheck> checks) implements DDLStatement {

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> visitor, C context) {
        return visitor.visitAlterTableAddColumn(this, context);
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
    }

    public AddColumnRequest bind(NodeContext nodeCtx,
                                 CoordinatorTxnCtx txnCtx,
                                 Row params,
                                 SubQueryResults subQueryResults) {
        SubQueryAndParamBinder bindParameter = new SubQueryAndParamBinder(params, subQueryResults);
        Function<Symbol, Object> toValue = new SymbolEvaluator(txnCtx, nodeCtx, subQueryResults).bind(params);
        List<Reference> newColumns = new ArrayList<>(columns.size());
        LinkedHashSet<Reference> primaryKeys = new LinkedHashSet<>();
        for (var refBuilder : columns.values()) {
            Reference reference = refBuilder.build(columns, table.ident(), bindParameter, toValue);
            if (refBuilder.isPrimaryKey()) {
                primaryKeys.add(reference);
            }
            newColumns.add(reference);
        }
        Map<String, String> checkConstraints = new LinkedHashMap<>();
        for (var entry : checks.entrySet()) {
            String constraintName = entry.getKey();
            AnalyzedCheck check = entry.getValue();
            checkConstraints.put(constraintName, check.expression());
        }
        IntArrayList pkIndices = new IntArrayList(primaryKeys.size());
        for (Reference pk : primaryKeys) {
            int idx = Reference.indexOf(newColumns, pk.column());
            pkIndices.add(idx);
        }
        return new AddColumnRequest(
            table.ident(),
            newColumns,
            checkConstraints,
            pkIndices
        );
    }
}
