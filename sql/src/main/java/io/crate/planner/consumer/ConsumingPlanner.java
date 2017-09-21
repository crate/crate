/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.planner.consumer;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.SelectSymbol;
import io.crate.metadata.Functions;
import io.crate.planner.MultiPhasePlan;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.TableStats;
import io.crate.planner.operators.LogicalPlanner;
import io.crate.planner.projection.builder.ProjectionBuilder;
import org.elasticsearch.cluster.service.ClusterService;

import javax.annotation.Nullable;
import java.util.Map;

public class ConsumingPlanner {

    private final OptimizingRewriter optimizer;
    private final Functions functions;

    public ConsumingPlanner(ClusterService clusterService,
                            Functions functions,
                            TableStats tableStats) {
        optimizer = new OptimizingRewriter(functions);
        this.functions = functions;
    }

    @Nullable
    public Plan plan(AnalyzedRelation rootRelation, Planner.Context plannerContext) {
        ConsumerContext consumerContext = new ConsumerContext(plannerContext);
        return plan(rootRelation, consumerContext);
    }

    @Nullable
    public Plan plan(AnalyzedRelation relation, ConsumerContext consumerContext) {
        relation = optimizer.optimize(relation,  consumerContext.plannerContext().transactionContext());

        if (relation instanceof QueriedRelation) {
            LogicalPlanner logicalPlanner = new LogicalPlanner();
            Planner.Context context = consumerContext.plannerContext();
            SubqueryPlanner subqueryPlanner = new SubqueryPlanner(context);
            QueriedRelation queriedRelation = (QueriedRelation) relation;
            Map<Plan, SelectSymbol> subQueries = subqueryPlanner.planSubQueries(queriedRelation.querySpec());
            return MultiPhasePlan.createIfNeeded(
                logicalPlanner.plan(
                    queriedRelation,
                    context,
                    new ProjectionBuilder(functions),
                    consumerContext.fetchMode()
                ),
                subQueries
            );
        }
        return null;
    }

}
