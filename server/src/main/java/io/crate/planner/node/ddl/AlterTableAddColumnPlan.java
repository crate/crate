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

import com.carrotsearch.hppc.IntArrayList;

import io.crate.analyze.AnalyzedAlterTableAddColumn;
import io.crate.analyze.AnalyzedColumnDefinition;
import io.crate.analyze.AnalyzedTableElements;
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
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryAndParamBinder;
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
        var tableElements = validate(
            alterTable,
            plannerContext.transactionContext(),
            dependencies.nodeContext(),
            params,
            subQueryResults,
            dependencies.fulltextAnalyzerResolver()
        );
        var addColumnRequest = createRequest(tableElements, alterTable.tableInfo().ident());

        dependencies.alterTableOperation().executeAlterTableAddColumn(addColumnRequest)
            .whenComplete(new OneRowActionListener<>(consumer, rCount -> new Row1(rCount == null ? -1 : rCount)));
    }

    /**
     * @param tableElements has to be finalized and validated before passing to this method.
     * collectReferences is called with bound = true meaning that it expects analyzer, geo properties to be resolved at this point.
     */
    public static AddColumnRequest createRequest(AnalyzedTableElements<Symbol> tableElements, RelationName relationName) {
        LinkedHashMap<ColumnIdent, Reference> references = new LinkedHashMap<>();
        IntArrayList pKeysIndices = new IntArrayList();
        tableElements.collectReferences(relationName, references, pKeysIndices, true);

        return new AddColumnRequest(
            relationName,
            new ArrayList<>(references.values()),
            tableElements.getCheckConstraints(),
            pKeysIndices
        );
    }


    /**
     * Validates statement, resolves generated and default expressions.
     */
    public static AnalyzedTableElements<Symbol> validate(AnalyzedAlterTableAddColumn alterTable,
                                                         CoordinatorTxnCtx txnCtx,
                                                         NodeContext nodeCtx,
                                                         Row params,
                                                         SubQueryResults subQueryResults,
                                                         FulltextAnalyzerResolver fulltextAnalyzerResolver) {
        SubQueryAndParamBinder paramBinder = new SubQueryAndParamBinder(params, subQueryResults);
        DocTableInfo tableInfo = alterTable.tableInfo();
        AnalyzedTableElements<Symbol> tableElements = alterTable.analyzedTableElements().map(paramBinder);

        for (AnalyzedColumnDefinition<Symbol> column : tableElements.columns()) {
            ensureColumnLeafsAreNew(column, tableInfo);
        }

        ensureNoIndexDefinitions(tableElements.columns());

        AnalyzedTableElements.validateAndBuildSettings(tableElements, fulltextAnalyzerResolver);
        AnalyzedTableElements.finalizeAndValidate(alterTable.tableInfo().ident(), tableElements);
        return tableElements;
    }



    private static <T> void ensureColumnLeafsAreNew(AnalyzedColumnDefinition<T> column, TableInfo tableInfo) {
        if ((!column.isParentColumn() || !column.hasChildren()) && tableInfo.getReference(column.ident()) != null) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                                             "The table %s already has a column named %s",
                                                             tableInfo.ident().sqlFqn(),
                                                             column.ident().sqlFqn()));
        }
        for (AnalyzedColumnDefinition<T> child : column.children()) {
            ensureColumnLeafsAreNew(child, tableInfo);
        }
    }

    private static <T> void ensureNoIndexDefinitions(List<AnalyzedColumnDefinition<T>> columns) {
        for (AnalyzedColumnDefinition<T> column : columns) {
            if (column.isIndexColumn()) {
                throw new UnsupportedOperationException(
                    "Adding an index using ALTER TABLE ADD COLUMN is not supported");
            }
            ensureNoIndexDefinitions(column.children());
        }
    }
}
