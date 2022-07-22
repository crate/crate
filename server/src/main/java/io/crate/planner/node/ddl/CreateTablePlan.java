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

import io.crate.analyze.AnalyzedColumnDefinition;
import io.crate.analyze.AnalyzedCreateTable;
import io.crate.analyze.AnalyzedTableElements;
import io.crate.analyze.BoundCreateTable;
import io.crate.analyze.NumberOfShards;
import io.crate.analyze.SymbolEvaluator;
import io.crate.analyze.TableParameter;
import io.crate.analyze.TableParameters;
import io.crate.analyze.TablePropertiesAnalyzer;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.ddl.tables.TableCreator;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryAndParamBinder;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.ClusteredBy;
import io.crate.sql.tree.CreateTable;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.PartitionedBy;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;

import static io.crate.data.SentinelRow.SENTINEL;

public class CreateTablePlan implements Plan {

    private static final String CLUSTERED_BY_IN_PARTITIONED_ERROR = "Cannot use CLUSTERED BY column in PARTITIONED BY clause";

    private final AnalyzedCreateTable createTable;
    private final NumberOfShards numberOfShards;
    private final TableCreator tableCreator;
    private final Schemas schemas;

    public CreateTablePlan(AnalyzedCreateTable createTable,
                           NumberOfShards numberOfShards,
                           TableCreator tableCreator,
                           Schemas schemas) {
        this.createTable = createTable;
        this.numberOfShards = numberOfShards;
        this.tableCreator = tableCreator;
        this.schemas = schemas;
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
                              SubQueryResults subQueryResults) {
        BoundCreateTable boundCreateTable = bind(
            createTable,
            plannerContext.transactionContext(),
            dependencies.nodeContext(),
            params,
            subQueryResults,
            numberOfShards,
            schemas,
            dependencies.fulltextAnalyzerResolver());

        if (boundCreateTable.noOp()) {
            consumer.accept(InMemoryBatchIterator.empty(SENTINEL), null);
            return;
        }

        tableCreator.create(boundCreateTable)
            .whenComplete(new OneRowActionListener<>(consumer, rCount -> new Row1(rCount == null ? -1 : rCount)));
    }

    @VisibleForTesting
    public static BoundCreateTable bind(AnalyzedCreateTable createTable,
                                        CoordinatorTxnCtx txnCtx,
                                        NodeContext nodeCtx,
                                        Row params,
                                        SubQueryResults subQueryResults,
                                        NumberOfShards numberOfShards,
                                        Schemas schemas,
                                        FulltextAnalyzerResolver fulltextAnalyzerResolver) {
        Function<? super Symbol, Object> eval = x -> SymbolEvaluator.evaluate(
            txnCtx,
            nodeCtx,
            x,
            params,
            subQueryResults
        );
        CreateTable<Symbol> table = createTable.createTable();
        RelationName relationName = createTable.relationName();
        GenericProperties<Object> properties = table.properties().map(eval);
        AnalyzedTableElements<Object> tableElements = createTable.analyzedTableElements().map(eval);
        TableParameter tableParameter = new TableParameter();
        Optional<ClusteredBy<Object>> mappedClusteredBy = table.clusteredBy().map(x -> x.map(eval));
        Integer numShards = mappedClusteredBy
            .flatMap(ClusteredBy::numberOfShards)
            .map(numberOfShards::fromNumberOfShards)
            .orElseGet(numberOfShards::defaultNumberOfShards);
        tableParameter.settingsBuilder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards);

        // apply default in case it is not specified in the properties,
        // if it is it will get overwritten afterwards.
        TablePropertiesAnalyzer.analyzeWithBoundValues(
            tableParameter,
            TableParameters.TABLE_CREATE_PARAMETER_INFO,
            properties,
            true
        );

        AnalyzedTableElements<Symbol> tableElementsWithExpressions =
            createTable.analyzedTableElementsWithExpressions().map(x -> SubQueryAndParamBinder.convert(x, params, subQueryResults));

        // validate table elements
        AnalyzedTableElements.finalizeAndValidate(
            relationName,
            tableElementsWithExpressions,
            tableElements
        );

        // finalizeAndValidate used to compute mapping which implicitly validated storage settings.
        // Since toMapping call is removed from finalizeAndValidate we are triggering this check explicitly
        for (AnalyzedColumnDefinition<Object> column : tableElements.columns()) {
            AnalyzedColumnDefinition.docValuesSpecifiedAndDisabled(column);
        }

        // update table settings
        Settings tableSettings = AnalyzedTableElements.validateAndBuildSettings(
            tableElements, fulltextAnalyzerResolver);
        tableParameter.settingsBuilder().put(tableSettings);

        ColumnIdent routingColumn = mappedClusteredBy
            .map(clusteredBy -> resolveRoutingFromClusteredBy(clusteredBy, tableElements))
            .orElse(null);
        Optional<PartitionedBy<Object>> partitionedByOptional = table.partitionedBy().map(x -> x.map(eval));
        partitionedByOptional.ifPresent(partitionedBy -> processPartitionedBy(
            partitionedByOptional.get(),
            tableElements,
            relationName,
            routingColumn)
        );
        return new BoundCreateTable(
            relationName,
            tableElements,
            tableParameter,
            routingColumn,
            table.ifNotExists(),
            schemas);
    }

    private static ColumnIdent resolveRoutingFromClusteredBy(ClusteredBy<Object> clusteredBy,
                                                             AnalyzedTableElements<Object> tableElements) {
        if (clusteredBy.column().isPresent()) {
            Object routingColumnValue = clusteredBy.column().get();
            assert routingColumnValue instanceof String;
            ColumnIdent routingColumn = ColumnIdent.fromPath((String) routingColumnValue);

            for (AnalyzedColumnDefinition<Object> column : tableElements.partitionedByColumns) {
                if (column.ident().equals(routingColumn)) {
                    throw new IllegalArgumentException(CLUSTERED_BY_IN_PARTITIONED_ERROR);
                }
            }
            if (!hasColumnDefinition(tableElements, routingColumn)) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Invalid or non-existent routing column \"%s\"",
                                  routingColumn));
            }
            if (AnalyzedTableElements.primaryKeys(tableElements).size() > 0 &&
                !AnalyzedTableElements.primaryKeys(tableElements).contains(routingColumn.fqn())) {
                throw new IllegalArgumentException("Clustered by column must be part of primary keys");
            }

            if (routingColumn.name().equalsIgnoreCase("_id") == false) {
                return routingColumn;
            }
        }
        return null;
    }

    private static void processPartitionedBy(PartitionedBy<Object> node,
                                             AnalyzedTableElements<Object> tableElements,
                                             RelationName relationName,
                                             @Nullable ColumnIdent routing) {
        for (Object partitionByColumn : node.columns()) {
            assert partitionByColumn instanceof String;
            ColumnIdent partitionedByIdent = ColumnIdent.fromPath((String) partitionByColumn);

            AnalyzedTableElements.changeToPartitionedByColumn(tableElements, partitionedByIdent, false, relationName);
            if (routing != null && routing.equals(partitionedByIdent)) {
                throw new IllegalArgumentException(CLUSTERED_BY_IN_PARTITIONED_ERROR);
            }
        }
    }

    private static boolean hasColumnDefinition(AnalyzedTableElements tableElements, ColumnIdent columnIdent) {
        return (tableElements.columnIdents().contains(columnIdent) ||
                columnIdent.name().equalsIgnoreCase("_id"));
    }
}
