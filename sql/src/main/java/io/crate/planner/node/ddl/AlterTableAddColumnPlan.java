/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner.node.ddl;

import com.google.common.annotations.VisibleForTesting;
import io.crate.analyze.AddColumnAnalyzedStatement;
import io.crate.analyze.AnalyzedAlterTableAddColumn;
import io.crate.analyze.AnalyzedColumnDefinition;
import io.crate.analyze.AnalyzedTableElements;
import io.crate.analyze.SymbolEvaluator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.types.ArrayType;
import org.elasticsearch.common.settings.Settings;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

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
        AddColumnAnalyzedStatement stmt = createStatement(
            alterTable,
            plannerContext.transactionContext(),
            plannerContext.functions(),
            params,
            subQueryResults,
            dependencies.fulltextAnalyzerResolver());

        dependencies.alterTableOperation().executeAlterTableAddColumn(stmt)
            .whenComplete(new OneRowActionListener<>(consumer, rCount -> new Row1(rCount == null ? -1 : rCount)));
    }

    @VisibleForTesting
    public static AddColumnAnalyzedStatement createStatement(AnalyzedAlterTableAddColumn alterTable,
                                                             CoordinatorTxnCtx txnCtx,
                                                             Functions functions,
                                                             Row params,
                                                             SubQueryResults subQueryResults,
                                                             FulltextAnalyzerResolver fulltextAnalyzerResolver) {
        Function<? super Symbol, Object> eval = x -> SymbolEvaluator.evaluate(
            txnCtx,
            functions,
            x,
            params,
            subQueryResults
        );
        DocTableInfo tableInfo = alterTable.tableInfo();

        AnalyzedTableElements<Symbol> tableElementsUnbound = alterTable.analyzedTableElements();
        AnalyzedTableElements<Object> tableElementsBound = tableElementsUnbound.map(eval);

        for (AnalyzedColumnDefinition<Object> column : tableElementsBound.columns()) {
            ensureColumnLeafsAreNew(column, tableInfo);
        }
        addExistingPrimaryKeys(tableInfo, tableElementsBound);
        ensureNoIndexDefinitions(tableElementsBound.columns());
        // validate table elements
        AnalyzedTableElements<Symbol> tableElementsUnboundWithExpressions = alterTable.analyzedTableElementsWithExpressions();
        Map<String, Object> mapping = AnalyzedTableElements.finalizeAndValidate(
            tableInfo.ident(),
            tableElementsUnboundWithExpressions,
            tableElementsBound,
            functions);


        int numCurrentPks = tableInfo.primaryKey().size();
        if (tableInfo.primaryKey().contains(DocSysColumns.ID)) {
            numCurrentPks -= 1;
        }

        Settings tableSettings = AnalyzedTableElements.validateAndBuildSettings(
            tableElementsBound, fulltextAnalyzerResolver);

        boolean hasNewPrimaryKeys = AnalyzedTableElements.primaryKeys(tableElementsBound).size() > numCurrentPks;
        boolean hasGeneratedColumns = tableElementsUnboundWithExpressions.hasGeneratedColumns();
        return new AddColumnAnalyzedStatement(
            tableInfo,
            tableElementsBound,
            tableSettings,
            mapping,
            hasNewPrimaryKeys,
            hasGeneratedColumns
        );
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

    private static void addExistingPrimaryKeys(DocTableInfo tableInfo, AnalyzedTableElements<Object> tableElements) {
        for (ColumnIdent pkIdent : tableInfo.primaryKey()) {
            if (pkIdent.name().equals("_id")) {
                continue;
            }
            Reference pkInfo = tableInfo.getReference(pkIdent);
            assert pkInfo != null : "pk must not be null";

            AnalyzedColumnDefinition<Object> pkColumn = new AnalyzedColumnDefinition<>(null, null);
            pkColumn.ident(pkIdent);
            pkColumn.name(pkIdent.name());
            pkColumn.setPrimaryKeyConstraint();

            assert !(pkInfo.valueType() instanceof ArrayType) : "pk can't be an array";
            pkColumn.dataType(pkInfo.valueType().getName());
            tableElements.add(pkColumn);
        }

        for (ColumnIdent columnIdent : tableInfo.partitionedBy()) {
            AnalyzedTableElements.changeToPartitionedByColumn(tableElements, columnIdent, true, tableInfo.ident());
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
