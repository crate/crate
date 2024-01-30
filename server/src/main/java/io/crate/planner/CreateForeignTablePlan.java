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

package io.crate.planner;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.crate.analyze.AnalyzedCreateForeignTable;
import io.crate.analyze.SymbolEvaluator;
import io.crate.analyze.TableElementsAnalyzer.RefBuilder;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.Symbol;
import io.crate.fdw.CreateForeignTableRequest;
import io.crate.fdw.TransportCreateForeignTableAction;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.planner.operators.SubQueryAndParamBinder;
import io.crate.planner.operators.SubQueryResults;

public class CreateForeignTablePlan implements Plan {

    private final AnalyzedCreateForeignTable createTable;

    public CreateForeignTablePlan(AnalyzedCreateForeignTable createTable) {
        this.createTable = createTable;
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
        SubQueryAndParamBinder paramBinder = new SubQueryAndParamBinder(params, subQueryResults);
        Function<Symbol, Object> toValue = new SymbolEvaluator(
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            subQueryResults
        ).bind(params);
        Map<String, Object> options = createTable.options().entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, entry -> toValue.apply(entry.getValue())));

        Map<ColumnIdent, RefBuilder> columns = createTable.columns();
        Map<ColumnIdent, Reference> references = new LinkedHashMap<>();
        RelationName tableName = createTable.tableName();
        for (var entry : columns.entrySet()) {
            var columnIdent = entry.getKey();
            var refBuilder = entry.getValue();
            var reference = refBuilder.build(columns, tableName, paramBinder, toValue);
            references.put(columnIdent, reference);
        }
        CreateForeignTableRequest request = new CreateForeignTableRequest(
            tableName,
            createTable.ifNotExists(),
            references,
            createTable.server(),
            options
        );
        dependencies.client()
            .execute(TransportCreateForeignTableAction.ACTION, request)
            .whenComplete(OneRowActionListener.oneIfAcknowledged(consumer));
    }
}
