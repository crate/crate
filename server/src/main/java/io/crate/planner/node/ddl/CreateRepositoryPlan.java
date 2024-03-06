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

import java.util.Map;
import java.util.function.Function;

import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import io.crate.analyze.AnalyzedCreateRepository;
import io.crate.analyze.SymbolEvaluator;
import io.crate.analyze.repositories.RepositoryParamValidator;
import org.jetbrains.annotations.VisibleForTesting;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;


public class CreateRepositoryPlan implements Plan {

    private final AnalyzedCreateRepository createRepository;

    public CreateRepositoryPlan(AnalyzedCreateRepository createRepository) {
        this.createRepository = createRepository;
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
        PutRepositoryRequest request = createRequest(
            createRepository,
            plannerContext.transactionContext(),
            dependencies.nodeContext(),
            parameters,
            subQueryResults,
            dependencies.repositoryParamValidator());

        dependencies.repositoryService()
            .execute(request)
            .whenComplete(new OneRowActionListener<>(consumer, rCount -> new Row1(rCount == null ? -1 : rCount)));
    }

    @VisibleForTesting
    public static PutRepositoryRequest createRequest(AnalyzedCreateRepository createRepository,
                                                     CoordinatorTxnCtx txnCtx,
                                                     NodeContext nodeCtx,
                                                     Row parameters,
                                                     SubQueryResults subQueryResults,
                                                     RepositoryParamValidator repositoryParamValidator) {
        Function<? super Symbol, Object> eval = x -> SymbolEvaluator.evaluate(
            txnCtx,
            nodeCtx,
            x,
            parameters,
            subQueryResults
        );

        var genericProperties = createRepository.properties().map(eval);
        Map<String, Setting<?>> supportedSettings = repositoryParamValidator.settingsForType(createRepository.type()).all();
        genericProperties.ensureContainsOnly(supportedSettings.keySet());
        var settings = Settings.builder().put(genericProperties).build();

        repositoryParamValidator.validate(
            createRepository.type(), createRepository.properties(), settings);

        PutRepositoryRequest request = new PutRepositoryRequest(createRepository.name());
        request.type(createRepository.type());
        request.settings(settings);

        return request;
    }
}
