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

import io.crate.action.sql.SessionContext;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.SelectSymbol;
import io.crate.exceptions.ValidationException;
import io.crate.metadata.Functions;
import io.crate.planner.MultiPhasePlan;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.TableStats;
import io.crate.planner.projection.builder.ProjectionBuilder;
import org.elasticsearch.cluster.service.ClusterService;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ConsumingPlanner {

    private final List<Consumer> consumers = new ArrayList<>();
    private final OptimizingRewriter optimizer;

    public ConsumingPlanner(ClusterService clusterService,
                            Functions functions,
                            TableStats tableStats) {
        optimizer = new OptimizingRewriter(functions);
        ProjectionBuilder projectionBuilder = new ProjectionBuilder(functions);
        consumers.add(new NonDistributedGroupByConsumer(projectionBuilder));
        consumers.add(new ReduceOnCollectorGroupByConsumer(projectionBuilder));
        consumers.add(new DistributedGroupByConsumer(projectionBuilder));
        consumers.add(new CountConsumer());
        consumers.add(new GlobalAggregateConsumer(projectionBuilder));
        consumers.add(new QueryAndFetchConsumer());
        consumers.add(new MultiSourceAggregationConsumer(projectionBuilder));
        consumers.add(new MultiSourceGroupByConsumer(projectionBuilder));
        consumers.add(new ManyTableConsumer());
        consumers.add(new NestedLoopConsumer(clusterService, tableStats));
        consumers.add(new GroupingSubselectConsumer(projectionBuilder));
    }

    @Nullable
    public Plan plan(AnalyzedRelation rootRelation, Planner.Context plannerContext) {
        ConsumerContext consumerContext = new ConsumerContext(plannerContext);
        return plan(rootRelation, consumerContext);
    }

    @Nullable
    public Plan plan(AnalyzedRelation relation, ConsumerContext consumerContext) {
        relation = optimizer.optimize(relation,  consumerContext.plannerContext().transactionContext());

        Map<Plan, SelectSymbol> subQueries = getSubQueries(relation, consumerContext);
        for (Consumer consumer : consumers) {
            Plan plan = consumer.consume(relation, consumerContext);
            if (plan != null) {
                return MultiPhasePlan.createIfNeeded(plan, subQueries);
            }
        }
        ValidationException validationException = consumerContext.validationException();
        if (validationException != null) {
            throw validationException;
        }
        return null;
    }

    private static Map<Plan, SelectSymbol> getSubQueries(AnalyzedRelation relation, ConsumerContext consumerContext) {
        if (relation instanceof QueriedRelation) {
            QuerySpec qs = ((QueriedRelation) relation).querySpec();
            SubqueryPlanner subqueryPlanner = new SubqueryPlanner(consumerContext.plannerContext());
            return subqueryPlanner.planSubQueries(qs);
        }
        return Collections.emptyMap();
    }
}
