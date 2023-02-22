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

package io.crate.planner.node.ddl;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import com.carrotsearch.hppc.IntArrayList;

import io.crate.analyze.AnalyzedAlterTableAddColumn;
import io.crate.analyze.AnalyzedColumnDefinition;
import io.crate.analyze.AnalyzedTableElements;
import io.crate.analyze.SymbolEvaluator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.ddl.tables.AddColumnRequest;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;

public class AlterTableAddColumnPlan implements Plan {

    private final AnalyzedAlterTableAddColumn alterTable;

    public AlterTableAddColumnPlan(AnalyzedAlterTableAddColumn alterTable) {
        this.alterTable = alterTable;
    }

    @Override
    public StatementType type() {
        return StatementType.DDL;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) throws Exception {
        var addColumnRequest = createRequest(alterTable, dependencies.nodeContext(), plannerContext,
                                                              params, subQueryResults, dependencies.fulltextAnalyzerResolver());

        dependencies.alterTableOperation().executeAlterTableAddColumn(addColumnRequest)
            .whenComplete(new OneRowActionListener<>(consumer, rCount -> new Row1(rCount == null ? -1 : rCount)));
    }

    public static AddColumnRequest createRequest(AnalyzedAlterTableAddColumn alterTable,
                                                 NodeContext nodeContext,
                                                 PlannerContext plannerContext,
                                                 Row params,
                                                 SubQueryResults subQueryResults,
                                                 FulltextAnalyzerResolver fulltextAnalyzerResolver) {
        var tableElements = validate(
            alterTable,
            plannerContext.transactionContext(),
            nodeContext,
            params,
            subQueryResults,
            fulltextAnalyzerResolver
        );

        // We can add multiple object columns via ALTER TABLE ADD COLUMN.
        // Those columns can have overlapping paths, for example we can add columns o['a']['b'] and o['a']['c'].
        // For every added column AnalyzedColumnDefinition provides not only leaf but also path to the root.
        // For o['a']['b'] and o['a']['c'] we can end up having Ref(Ident(o)) and Ref(Ident(a)) twice.

        // We need a Map to compare References by FQN.
        // Regular Set cannot be used as it would use Reference.position along with other fields when calling equals() and
        // position is resolved to -1 and -2 for all parts of the path in the case above so overlapping parts will be "different".

        // pKeyIndices is constructed based on insertion order, so we need a LinkedHashMap.

        LinkedHashMap<ColumnIdent, Reference> references = new LinkedHashMap<>();
        IntArrayList pKeysIndices = new IntArrayList();
        tableElements.collectReferences(alterTable.tableInfo().ident(), references, pKeysIndices, true);

        return new AddColumnRequest(
            alterTable.tableInfo().ident(),
            new ArrayList<>(references.values()), // We don't use Map in the request itself since we need directly indexed structure referred by pKeysIndices.
            tableElements.getCheckConstraints(),
            pKeysIndices
        );
    }

    /**
     * Validates statement, resolves generated and default expressions.
     */
    public static AnalyzedTableElements<Object> validate(AnalyzedAlterTableAddColumn alterTable,
                                                         CoordinatorTxnCtx txnCtx,
                                                         NodeContext nodeCtx,
                                                         Row params,
                                                         SubQueryResults subQueryResults,
                                                         FulltextAnalyzerResolver fulltextAnalyzerResolver) {
        Function<? super Symbol, Object> eval = x -> SymbolEvaluator.evaluate(
            txnCtx,
            nodeCtx,
            x,
            params,
            subQueryResults
        );
        DocTableInfo tableInfo = alterTable.tableInfo();
        AnalyzedTableElements<Object> tableElements = alterTable.analyzedTableElements().map(eval);

        for (AnalyzedColumnDefinition<Object> column : tableElements.columns()) {
            ensureColumnLeafsAreNew(column, tableInfo);
        }

        ensureNoIndexDefinitions(tableElements.columns());

        AnalyzedTableElements.validateAndBuildSettings(tableElements, fulltextAnalyzerResolver);

        AnalyzedTableElements.finalizeAndValidate(
            alterTable.tableInfo().ident(),
            alterTable.analyzedTableElementsWithExpressions(),
            tableElements
        );

        return tableElements;
    }



    private static void ensureColumnLeafsAreNew(AnalyzedColumnDefinition<Object> column, TableInfo tableInfo) {
        if ((!column.isParentColumn() || !column.hasChildren()) && tableInfo.getReference(column.ident()) != null) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                                             "The table %s already has a column named %s",
                                                             tableInfo.ident().sqlFqn(),
                                                             column.ident().sqlFqn()));
        }
        for (AnalyzedColumnDefinition<Object> child : column.children()) {
            ensureColumnLeafsAreNew(child, tableInfo);
        }
    }

    private static void ensureNoIndexDefinitions(List<AnalyzedColumnDefinition<Object>> columns) {
        for (AnalyzedColumnDefinition<Object> column : columns) {
            if (column.isIndexColumn()) {
                throw new UnsupportedOperationException(
                    "Adding an index using ALTER TABLE ADD COLUMN is not supported");
            }
            ensureNoIndexDefinitions(column.children());
        }
    }
}
