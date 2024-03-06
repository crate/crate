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

import io.crate.analyze.AnalyzedDecommissionNode;
import io.crate.analyze.SymbolEvaluator;
import io.crate.cluster.decommission.DecommissionNodeAction;
import io.crate.cluster.decommission.DecommissionRequest;
import org.jetbrains.annotations.VisibleForTesting;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.types.DataTypes;

public final class DecommissionNodePlan implements Plan {

    private final AnalyzedDecommissionNode analyzedDecommissionNode;

    DecommissionNodePlan(AnalyzedDecommissionNode analyzedDecommissionNode) {
        this.analyzedDecommissionNode = analyzedDecommissionNode;
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
        var boundedNodeIdOrName = boundNodeIdOrName(
            analyzedDecommissionNode,
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            params,
            subQueryResults);

        String targetNodeId = NodeSelection.resolveNodeId(
            dependencies.clusterService().state().nodes(),
            boundedNodeIdOrName
        );

        dependencies.client()
            .execute(DecommissionNodeAction.INSTANCE, DecommissionRequest.of(targetNodeId))
            .whenComplete(new OneRowActionListener<>(
                consumer,
                r -> r.isAcknowledged() ? new Row1(1L) : new Row1(0L)));
    }

    @VisibleForTesting
    public static String boundNodeIdOrName(AnalyzedDecommissionNode decommissionNode,
                                           CoordinatorTxnCtx txnCtx,
                                           NodeContext nodeCtx,
                                           Row parameters,
                                           SubQueryResults subQueryResults) {
        var boundedNodeIdOrName = SymbolEvaluator.evaluate(
            txnCtx,
            nodeCtx,
            decommissionNode.nodeIdOrName(),
            parameters,
            subQueryResults
        );
        return DataTypes.STRING.sanitizeValue(boundedNodeIdOrName);
    }
}
