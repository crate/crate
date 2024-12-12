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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import io.crate.analyze.AnalyzedAlterServer;
import io.crate.analyze.SymbolEvaluator;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.Symbol;
import io.crate.fdw.AlterServerRequest;
import io.crate.fdw.ForeignDataWrapper;
import io.crate.fdw.ForeignDataWrappers;
import io.crate.fdw.ServersMetadata;
import io.crate.fdw.TransportAlterServerAction;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.planner.operators.SubQueryResults;

public class AlterServerPlan implements Plan {

    private final ForeignDataWrappers foreignDataWrappers;
    private final AnalyzedAlterServer alterServer;

    public AlterServerPlan(ForeignDataWrappers foreignDataWrappers,
                           AnalyzedAlterServer alterServer) {
        this.foreignDataWrappers = foreignDataWrappers;
        this.alterServer = alterServer;
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

        Metadata metadata = plannerContext.clusterState().metadata();
        ServersMetadata servers = metadata.custom(ServersMetadata.TYPE);
        if (servers == null) {
            throw new ResourceNotFoundException(
                String.format(Locale.ENGLISH, "Server `%s` not found", alterServer.name()));
        }
        ServersMetadata.Server server = servers.get(alterServer.name());

        ForeignDataWrapper fdw = foreignDataWrappers.get(server.fdw());

        HashSet<String> optionNames = new HashSet<>();
        Settings.Builder optionsAdded = Settings.builder();
        Settings.Builder optionsUpdated = Settings.builder();
        List<String> optionsRemoved = new ArrayList<>();

        Map<String, Setting<?>> mandatoryOptions = fdw.mandatoryServerOptions().stream()
            .collect(Collectors.toMap(Setting::getKey, Function.identity()));

        for (var option : alterServer.options()) {
            if (mandatoryOptions.get(option.key()) == null) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH,
                    "Unsupported server options for foreign data wrapper `%s`: %s. Valid options are: %s",
                    server.fdw(),
                    option.key(),
                    fdw.mandatoryServerOptions().stream()
                        .map(Setting::getKey)
                        .collect(Collectors.joining(", "))
                ));
            }
            if (optionNames.add(option.key()) == false) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH,
                    "Option `%s` is specified multiple times for server `%s`",
                    option.key(),
                    alterServer.name()
                ));
            }
            switch (option.operation()) {
                case ADD:
                    assert option.value() != null : "value must not be null for ADD operation";
                    optionsAdded.put(option.key(), convert.apply(option.value()));
                    break;
                case DROP:
                    if (mandatoryOptions.get(option.key()) != null) {
                        throw new IllegalArgumentException(String.format(
                            Locale.ENGLISH,
                            "Cannot remove mandatory server option `%s` for server `%s`",
                            option.key(),
                            alterServer.name()
                        ));
                    }
                    optionsRemoved.add(option.key());
                    break;
                case SET:
                    assert option.value() != null : "value must not be null for SET operation";
                    optionsUpdated.put(option.key(), convert.apply(option.value()));
                    break;
                default:
                    throw new AssertionError("Unsupported operation: " + option.operation());
            }
        }

        var request = new AlterServerRequest(
            alterServer.name(),
            optionsAdded.build(),
            optionsUpdated.build(),
            optionsRemoved
        );
        dependencies.client()
            .execute(TransportAlterServerAction.ACTION, request)
            .whenComplete(OneRowActionListener.oneIfAcknowledged(consumer));
    }
}
