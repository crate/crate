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

import static io.crate.analyze.OptimizeTableSettings.FLUSH;
import static io.crate.analyze.OptimizeTableSettings.MAX_NUM_SEGMENTS;
import static io.crate.analyze.OptimizeTableSettings.ONLY_EXPUNGE_DELETES;
import static io.crate.analyze.OptimizeTableSettings.SUPPORTED_SETTINGS;
import static io.crate.analyze.OptimizeTableSettings.UPGRADE_SEGMENTS;
import static io.crate.analyze.PartitionPropertiesAnalyzer.toPartitionName;
import static io.crate.data.SentinelRow.SENTINEL;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.settings.Settings;

import io.crate.analyze.AnalyzedOptimizeTable;
import io.crate.analyze.SymbolEvaluator;
import org.jetbrains.annotations.VisibleForTesting;
import io.crate.common.collections.Lists;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.data.SentinelRow;
import io.crate.exceptions.PartitionUnknownException;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.blob.BlobTableInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.Table;

public class OptimizeTablePlan implements Plan {

    private final AnalyzedOptimizeTable optimizeTable;

    public OptimizeTablePlan(AnalyzedOptimizeTable optimizeTable) {
        this.optimizeTable = optimizeTable;
    }

    @Override
    public StatementType type() {
        return StatementType.DDL;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row parameters,
                              SubQueryResults subQueryResults) {
        if (optimizeTable.tables().isEmpty()) {
            consumer.accept(InMemoryBatchIterator.empty(SENTINEL), null);
            return;
        }

        BoundOptimizeTable stmt = bind(
            optimizeTable,
            plannerContext.transactionContext(),
            dependencies.nodeContext(),
            parameters,
            subQueryResults
        );

        var settings = stmt.settings();
        var toOptimize = stmt.indexNames();

        if (UPGRADE_SEGMENTS.get(settings)) {
            consumer.accept(InMemoryBatchIterator.of(new Row1(-1L), SentinelRow.SENTINEL), null);
        } else {
            var request = new ForceMergeRequest(toOptimize.toArray(new String[0]));
            request.maxNumSegments(MAX_NUM_SEGMENTS.get(settings));
            request.onlyExpungeDeletes(ONLY_EXPUNGE_DELETES.get(settings));
            request.flush(FLUSH.get(settings));
            request.indicesOptions(IndicesOptions.lenientExpandOpen());

            dependencies.client()
                .execute(ForceMergeAction.INSTANCE, request)
                .whenComplete(
                    new OneRowActionListener<>(
                        consumer,
                        response -> new Row1(toOptimize.isEmpty() ? -1L : (long) toOptimize.size())
                    )
                );
        }
    }

    @VisibleForTesting
    public static BoundOptimizeTable bind(AnalyzedOptimizeTable optimizeTable,
                                          CoordinatorTxnCtx txnCtx,
                                          NodeContext nodeCtx,
                                          Row parameters,
                                          SubQueryResults subQueryResults) {
        Function<? super Symbol, Object> eval = x -> SymbolEvaluator.evaluate(
            txnCtx,
            nodeCtx,
            x,
            parameters,
            subQueryResults
        );

        var genericProperties = optimizeTable.properties().map(eval);
        genericProperties.ensureContainsOnly(SUPPORTED_SETTINGS.keySet());
        var settings = Settings.builder().put(genericProperties).build();
        validateSettings(settings, genericProperties);

        ArrayList<String> toOptimize = new ArrayList<>();
        for (Map.Entry<Table<Symbol>, TableInfo> table : optimizeTable.tables().entrySet()) {
            var tableInfo = table.getValue();
            var tableSymbol = table.getKey();
            if (tableInfo instanceof BlobTableInfo) {
                toOptimize.add(((BlobTableInfo) tableInfo).concreteIndices()[0]);
            } else {
                var docTableInfo = (DocTableInfo) tableInfo;
                if (tableSymbol.partitionProperties().isEmpty()) {
                    toOptimize.addAll(Arrays.asList(docTableInfo.concreteOpenIndices()));
                } else {
                    var partitionName = toPartitionName(
                        docTableInfo,
                        Lists.map(tableSymbol.partitionProperties(), x -> x.map(eval)));
                    if (!docTableInfo.partitions().contains(partitionName)) {
                        throw new PartitionUnknownException(partitionName);
                    }
                    toOptimize.add(partitionName.asIndexName());
                }
            }
        }

        return new BoundOptimizeTable(toOptimize, settings);
    }

    private static void validateSettings(Settings settings, GenericProperties<?> properties) {
        if (UPGRADE_SEGMENTS.get(settings) && properties.size() > 1) {
            throw new IllegalArgumentException("cannot use other parameters if " +
                                               UPGRADE_SEGMENTS.getKey() + " is set to true");
        }
    }

    public static class BoundOptimizeTable {

        private final List<String> indexNames;
        private final Settings settings;

        BoundOptimizeTable(List<String> indexNames, Settings settings) {
            this.indexNames = indexNames;
            this.settings = settings;
        }

        public List<String> indexNames() {
            return indexNames;
        }

        public Settings settings() {
            return settings;
        }
    }
}
