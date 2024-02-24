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

import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Setting;

import io.crate.analyze.AnalyzedCreateUserMapping;
import io.crate.analyze.SymbolEvaluator;
import io.crate.common.collections.Lists;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.Symbol;
import io.crate.fdw.CreateUserMappingRequest;
import io.crate.fdw.ForeignDataWrappers;
import io.crate.fdw.ServersMetadata;
import io.crate.fdw.TransportCreateUserMappingAction;
import io.crate.planner.operators.SubQueryResults;

public class CreateUserMappingPlan implements Plan {

    private final ForeignDataWrappers foreignDataWrappers;
    private final AnalyzedCreateUserMapping createUserMapping;

    public CreateUserMappingPlan(ForeignDataWrappers foreignDataWrappers,
                                 AnalyzedCreateUserMapping createUserMapping) {
        this.foreignDataWrappers = foreignDataWrappers;
        this.createUserMapping = createUserMapping;
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
        Metadata metadata = plannerContext.clusterState().metadata();
        ServersMetadata serversMetadata = metadata.custom(ServersMetadata.TYPE, ServersMetadata.EMPTY);
        ServersMetadata.Server server = serversMetadata.get(createUserMapping.serverName());
        var supportedUserOptions = Lists.map(foreignDataWrappers.get(server.fdw()).optionalUserOptions(), Setting::getKey);
        createUserMapping.options().keySet().forEach(
            option -> {
                if (!supportedUserOptions.contains(option.toLowerCase(Locale.ROOT))) {
                    throw new IllegalArgumentException(
                        String.format(
                            Locale.ENGLISH,
                            "Invalid option '%s' provided, the supported options are: %s",
                            option,
                            supportedUserOptions)
                    );
                }
            });

        Function<Symbol, Object> toValue = new SymbolEvaluator(
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            subQueryResults
        ).bind(params);
        Map<String, Object> options = createUserMapping.options().entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, entry -> toValue.apply(entry.getValue())));

        var request = new CreateUserMappingRequest(
            createUserMapping.ifNotExists(),
            createUserMapping.user().name(),
            createUserMapping.serverName(),
            options
        );
        dependencies.client()
            .execute(TransportCreateUserMappingAction.ACTION, request)
            .whenComplete(OneRowActionListener.oneIfAcknowledged(consumer));
    }
}
