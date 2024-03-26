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

package io.crate.planner.node.management;

import java.util.List;
import java.util.UUID;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.SymbolEvaluator;
import org.jetbrains.annotations.VisibleForTesting;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.jobs.kill.KillAllNodeAction;
import io.crate.execution.jobs.kill.KillAllRequest;
import io.crate.execution.jobs.kill.KillJobsNodeAction;
import io.crate.execution.jobs.kill.KillJobsNodeRequest;
import io.crate.execution.jobs.kill.KillResponse;
import io.crate.execution.support.ActionExecutor;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.types.DataTypes;

public class KillPlan implements Plan {

    @Nullable
    private final Symbol jobId;

    public KillPlan(@Nullable Symbol jobId) {
        this.jobId = jobId;
    }

    @Override
    public StatementType type() {
        return StatementType.MANAGEMENT;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) {
        execute(
            boundJobId(
                jobId,
                plannerContext.transactionContext(),
                dependencies.nodeContext(),
                params,
                subQueryResults),
            plannerContext.transactionContext().sessionSettings().userName(),
            req -> dependencies.client().execute(KillJobsNodeAction.INSTANCE, req),
            req -> dependencies.client().execute(KillAllNodeAction.INSTANCE, req),
            consumer
        );
    }

    @VisibleForTesting
    @Nullable
    public static UUID boundJobId(@Nullable Symbol jobId,
                                  CoordinatorTxnCtx txnCtx,
                                  NodeContext nodeCtx,
                                  Row parameters,
                                  SubQueryResults subQueryResults) {
        if (jobId != null) {
            try {
                return UUID.fromString(
                    DataTypes.STRING.sanitizeValue(
                        SymbolEvaluator.evaluate(
                            txnCtx,
                            nodeCtx,
                            jobId,
                            parameters,
                            subQueryResults
                        )));
            } catch (Exception e) {
                throw new IllegalArgumentException("Can not parse job ID: " + jobId, e);
            }
        }
        return null;
    }

    @VisibleForTesting
    void execute(@Nullable UUID jobId,
                 String userName,
                 ActionExecutor<KillJobsNodeRequest, KillResponse> killJobsNodeAction,
                 ActionExecutor<KillAllRequest, KillResponse> killAllNodeAction,
                 RowConsumer consumer) {
        if (jobId != null) {
            killJobsNodeAction
                .execute(
                    new KillJobsNodeRequest(
                        List.of(),
                        List.of(jobId),
                        userName,
                        "KILL invoked by user: " + userName))
                .whenComplete(
                    new OneRowActionListener<>(
                        consumer,
                        killResponse -> new Row1(killResponse.numKilled()))
                );
        } else {
            killAllNodeAction
                .execute(
                    new KillAllRequest(userName))
                .whenComplete(
                    new OneRowActionListener<>(
                        consumer,
                        killResponse -> new Row1(killResponse.numKilled()))
                );
        }
    }
}

