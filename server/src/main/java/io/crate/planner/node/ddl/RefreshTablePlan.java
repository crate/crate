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

import io.crate.analyze.AnalyzedRefreshTable;
import io.crate.analyze.SymbolEvaluator;
import io.crate.common.collections.Lists2;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.exceptions.PartitionUnknownException;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.Table;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.support.IndicesOptions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;

import static io.crate.analyze.PartitionPropertiesAnalyzer.toPartitionName;
import static io.crate.data.SentinelRow.SENTINEL;

public class RefreshTablePlan implements Plan {

    private final AnalyzedRefreshTable analysis;

    public RefreshTablePlan(AnalyzedRefreshTable analysis) {
        this.analysis = analysis;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row parameters,
                              SubQueryResults subQueryResults) {
        if (analysis.tables().isEmpty()) {
            consumer.accept(InMemoryBatchIterator.empty(SENTINEL), null);
            return;
        }

        Function<? super Symbol, Object> eval = x -> SymbolEvaluator.evaluate(
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            x,
            parameters,
            subQueryResults
        );

        ArrayList<String> toRefresh = new ArrayList<>();
        for (Map.Entry<Table<Symbol>, DocTableInfo> table : analysis.tables().entrySet()) {
            var tableInfo = table.getValue();
            var tableSymbol = table.getKey();
            if (tableSymbol.partitionProperties().isEmpty()) {
                toRefresh.addAll(Arrays.asList(tableInfo.concreteOpenIndices()));
            } else {
                var partitionName = toPartitionName(
                    tableInfo,
                    Lists2.map(tableSymbol.partitionProperties(), p -> p.map(eval)));
                if (!tableInfo.partitions().contains(partitionName)) {
                    throw new PartitionUnknownException(partitionName);
                }
                toRefresh.add(partitionName.asIndexName());
            }
        }

        RefreshRequest request = new RefreshRequest(toRefresh.toArray(String[]::new));
        request.indicesOptions(IndicesOptions.lenientExpandOpen());

        var transportRefreshAction = dependencies.transportActionProvider().transportRefreshAction();
        transportRefreshAction.execute(
            request,
            new OneRowActionListener<>(
                consumer,
                response -> new Row1(toRefresh.isEmpty() ? -1L : (long) toRefresh.size())
            )
        );
    }

    @Override
    public StatementType type() {
        return StatementType.DDL;
    }
}
