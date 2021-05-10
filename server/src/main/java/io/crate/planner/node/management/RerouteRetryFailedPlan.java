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

import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;

public class RerouteRetryFailedPlan implements Plan {

    @Override
    public StatementType type() {
        return StatementType.MANAGEMENT;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) throws Exception {
        dependencies
            .transportActionProvider()
            .transportClusterRerouteAction()
            .execute(
                new ClusterRerouteRequest().setRetryFailed(true),
                new OneRowActionListener<>(
                    consumer,
                    response -> {
                        if (response.isAcknowledged()) {
                            long rowCount = 0L;
                            for (RoutingNode routingNode : response.getState().getRoutingNodes()) {
                                // filter shards with failed allocation attempts
                                // failed allocation attempts can appear for shards
                                // with state UNASSIGNED and INITIALIZING
                                rowCount += routingNode.shardsWithState(
                                    ShardRoutingState.UNASSIGNED,
                                    ShardRoutingState.INITIALIZING)
                                    .stream()
                                    .filter(s -> {
                                        if (s.unassignedInfo() != null) {
                                            return s.unassignedInfo().getReason()
                                                .equals(UnassignedInfo.Reason.ALLOCATION_FAILED);
                                        }
                                        return false;
                                    })
                                    .count();
                            }
                            return new Row1(rowCount);
                        } else {
                            return new Row1(-1L);
                        }
                    }
                )
            );
    }
}
