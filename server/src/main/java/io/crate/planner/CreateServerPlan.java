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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.elasticsearch.common.settings.Settings;

import io.crate.analyze.AnalyzedCreateServer;
import io.crate.analyze.SymbolEvaluator;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.Symbol;
import io.crate.fdw.CreateServerRequest;
import io.crate.fdw.ForeignDataWrappers;
import io.crate.fdw.TransportCreateServerAction;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.planner.operators.SubQueryResults;

public class CreateServerPlan implements Plan {

    private final ForeignDataWrappers foreignDataWrappers;
    private final AnalyzedCreateServer createServer;

    public CreateServerPlan(ForeignDataWrappers foreignDataWrappers,
                            AnalyzedCreateServer createServer) {
        this.foreignDataWrappers = foreignDataWrappers;
        this.createServer = createServer;
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

        CoordinatorTxnCtx transactionContext = plannerContext.transactionContext();
        Function<Symbol, Object> convert = new SymbolEvaluator(
            transactionContext,
            plannerContext.nodeContext(),
            subQueryResults
        ).bind(params);

        Settings.Builder optionsBuilder = Settings.builder();
        Map<String, Symbol> options = new HashMap<>(createServer.options());
        var foreignDataWrapper = foreignDataWrappers.get(createServer.fdw());
        for (var option : foreignDataWrapper.mandatoryServerOptions()) {
            String optionName = option.getKey();
            Symbol symbol = options.remove(optionName);
            if (symbol == null) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH,
                    "Mandatory server option `%s` for foreign data wrapper `%s` is missing",
                    optionName,
                    createServer.fdw()
                ));
            }
            optionsBuilder.put(optionName, convert.apply(symbol));
        }
        if (!options.isEmpty()) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "Unsupported server options for foreign data wrapper `%s`: %s. Valid options are: %s",
                createServer.fdw(),
                String.join(", ", options.keySet()),
                foreignDataWrapper.mandatoryServerOptions().stream()
                    .map(x -> x.getKey())
                    .collect(Collectors.joining(", "))
            ));
        }
        CreateServerRequest request = new CreateServerRequest(
            createServer.name(),
            createServer.fdw(),
            transactionContext.sessionSettings().sessionUser().name(),
            createServer.ifNotExists(),
            optionsBuilder.build()
        );
        dependencies.client()
            .execute(TransportCreateServerAction.ACTION, request)
            .whenComplete(OneRowActionListener.oneIfAcknowledged(consumer));
    }
}
