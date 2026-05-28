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

import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.repositories.put.AlterRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportAlterRepository;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoryMissingException;

import io.crate.analyze.AnalyzedAlterRepository;
import io.crate.analyze.SymbolEvaluator;
import io.crate.analyze.repositories.RepositoryParamValidator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.ddl.RepositoryService;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;

public class AlterRepositoryPlan implements Plan {

    private static final Logger LOGGER = LogManager.getLogger(AlterRepositoryPlan.class);

    private final AnalyzedAlterRepository alterRepository;

    public AlterRepositoryPlan(AnalyzedAlterRepository alterRepository) {
        this.alterRepository = alterRepository;
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

        if (plannerContext.clusterState().nodes().getMinNodeVersion().before(Version.V_6_4_0)) {
            throw new IllegalStateException("Cannot execute ALTER REPOSITORY while there are <6.4.0 nodes in the cluster");
        }

        AlterRepositoryRequest request = createRequest(
            alterRepository,
            plannerContext.transactionContext(),
            dependencies.nodeContext(),
            parameters,
            subQueryResults,
            dependencies.repositoryService(),
            dependencies.repositoryParamValidator()
        );

        dependencies.client()
            .execute(TransportAlterRepository.ACTION, request)
            .thenApply(response -> {
                if (!response.isAcknowledged()) {
                    LOGGER.info("alter repository '{}' not acknowledged", request.name());
                }
                return 1L;
            })
            .whenComplete(new OneRowActionListener<>(consumer, rCount -> new Row1(rCount == null ? -1 : rCount)));
    }

    private AlterRepositoryRequest createRequest(AnalyzedAlterRepository alterRepository,
                                                 CoordinatorTxnCtx txnCtx,
                                                 NodeContext nodeCtx,
                                                 Row parameters,
                                                 SubQueryResults subQueryResults,
                                                 RepositoryService repositoryService,
                                                 RepositoryParamValidator repositoryParamValidator) {

        // bind parameter values and get properties
        Function<? super Symbol, Object> eval = x -> SymbolEvaluator.evaluate(
            txnCtx,
            nodeCtx,
            x,
            parameters,
            subQueryResults
        );

        var repository = repositoryService.getRepository(alterRepository.name());
        if (repository == null) {
            throw new RepositoryMissingException(alterRepository.name());
        }

        // We have two "flavors" of AlterRepositoryRequest: one that sets properties and one that resets.
        // For both, we validate the properties and then build and return the request.
        if (!alterRepository.properties().isEmpty()) {
            var setProperties = alterRepository.properties().map(eval);
            repositoryParamValidator.validateSupportedOnly(repository.type(), setProperties);
            return new AlterRepositoryRequest(
                alterRepository.name(),
                Settings.builder().put(setProperties).build()
            );
        }

        repositoryParamValidator.validateCanReset(repository.type(), alterRepository.resetProperties());
        var resetProperties = Settings.builder();
        alterRepository.resetProperties().forEach(resetProperties::putNull);
        return new AlterRepositoryRequest(alterRepository.name(), resetProperties.build());
    }

}
