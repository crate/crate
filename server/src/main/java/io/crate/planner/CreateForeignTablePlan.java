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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.VisibleForTesting;

import io.crate.analyze.AnalyzedCreateForeignTable;
import io.crate.analyze.SymbolEvaluator;
import io.crate.analyze.TableElementsAnalyzer.RefBuilder;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.Symbol;
import io.crate.fdw.CreateForeignTableRequest;
import io.crate.fdw.ForeignDataWrappers;
import io.crate.fdw.ServersMetadata;
import io.crate.fdw.ServersMetadata.Server;
import io.crate.fdw.TransportCreateForeignTableAction;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.planner.operators.SubQueryAndParamBinder;
import io.crate.planner.operators.SubQueryResults;

public class CreateForeignTablePlan implements Plan {

    private final ForeignDataWrappers foreignDataWrappers;
    private final AnalyzedCreateForeignTable createTable;

    public CreateForeignTablePlan(ForeignDataWrappers foreignDataWrappers,
                                  AnalyzedCreateForeignTable createTable) {
        this.foreignDataWrappers = foreignDataWrappers;
        this.createTable = createTable;
    }

    @Override
    public StatementType type() {
        return StatementType.DDL;
    }

    @VisibleForTesting
    public static CreateForeignTableRequest toRequest(ForeignDataWrappers foreignDataWrappers,
                                                      AnalyzedCreateForeignTable createTable,
                                                      PlannerContext plannerContext,
                                                      Row params,
                                                      SubQueryResults subQueryResults) {
        SubQueryAndParamBinder paramBinder = new SubQueryAndParamBinder(params, subQueryResults);
        Function<Symbol, Object> toValue = new SymbolEvaluator(
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            subQueryResults
        ).bind(params);

        Metadata metadata = plannerContext.clusterState().metadata();
        ServersMetadata servers = metadata.custom(ServersMetadata.TYPE, ServersMetadata.EMPTY);
        Server server = servers.get(createTable.server());
        var foreignDataWrapper = foreignDataWrappers.get(server.fdw());

        Settings.Builder optionsBuilder = Settings.builder();
        Map<String, Symbol> options = new HashMap<>(createTable.options());
        for (Setting<?> option : foreignDataWrapper.optionalTableOptions()) {
            String optionName = option.getKey();
            Symbol symbol = options.remove(optionName);
            if (symbol == null) {
                continue;
            }
            optionsBuilder.put(optionName, toValue.apply(symbol));
        }
        if (!options.isEmpty()) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "Unsupported options for foreign table %s using fdw `%s`: %s. Valid options are: %s",
                createTable.tableName().sqlFqn(),
                server.fdw(),
                String.join(", ", options.keySet()),
                foreignDataWrapper.optionalTableOptions().stream()
                    .map(x -> x.getKey())
                    .collect(Collectors.joining(", "))
            ));
        }

        Map<ColumnIdent, RefBuilder> columns = createTable.columns();
        List<Reference> references = new ArrayList<>();
        RelationName tableName = createTable.tableName();
        for (var entry : columns.entrySet()) {
            var refBuilder = entry.getValue();
            var reference = refBuilder.build(columns, tableName, paramBinder, toValue);
            references.add(reference);
        }
        return new CreateForeignTableRequest(
            tableName,
            createTable.ifNotExists(),
            references,
            createTable.server(),
            optionsBuilder.build()
        );
    }


    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) throws Exception {
        var request = toRequest(
            foreignDataWrappers,
            createTable,
            plannerContext,
            params,
            subQueryResults
        );
        dependencies.client()
            .execute(TransportCreateForeignTableAction.ACTION, request)
            .whenComplete(OneRowActionListener.oneIfAcknowledged(consumer));
    }
}
