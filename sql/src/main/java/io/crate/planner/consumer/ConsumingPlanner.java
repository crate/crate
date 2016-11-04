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

import io.crate.analyze.QuerySpec;
import io.crate.analyze.Rewriter;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.SelectSymbol;
import io.crate.exceptions.ValidationException;
import io.crate.metadata.Functions;
import io.crate.planner.*;
import org.elasticsearch.cluster.ClusterService;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ConsumingPlanner {

    private final List<Consumer> consumers = new ArrayList<>();

    public ConsumingPlanner(ClusterService clusterService,
                            Functions functions,
                            TableStatsService tableStatsService) {
        consumers.add(new NonDistributedGroupByConsumer(functions));
        consumers.add(new ReduceOnCollectorGroupByConsumer(functions));
        consumers.add(new DistributedGroupByConsumer(functions));
        consumers.add(new CountConsumer());
        consumers.add(new GlobalAggregateConsumer(functions));
        consumers.add(new InsertFromSubQueryConsumer());
        consumers.add(new QueryAndFetchConsumer());
        consumers.add(new MultiSourceAggregationConsumer(functions));
        consumers.add(new ManyTableConsumer(this, new Rewriter(functions)));
        consumers.add(new NestedLoopConsumer(clusterService, functions, tableStatsService));
        consumers.add(new UnionConsumer());
    }

    @Nullable
    public Plan plan(AnalyzedRelation rootRelation, Planner.Context plannerContext) {
        ConsumerContext consumerContext = new ConsumerContext(rootRelation, plannerContext);
        return plan(rootRelation, consumerContext);
    }

    @Nullable
    public Plan plan(AnalyzedRelation relation, ConsumerContext consumerContext) {
        for (Consumer consumer : consumers) {
            Plan plan = consumer.consume(relation, consumerContext);
            if (plan != null) {
                if (relation instanceof QueriedRelation) {
                    QuerySpec qs = ((QueriedRelation) relation).querySpec();
                    SubqueryPlanner subqueryPlanner = new SubqueryPlanner(consumerContext.plannerContext());
                    Map<Plan, SelectSymbol> subQueries = subqueryPlanner.planSubQueries(qs);
                    return MultiPhasePlan.createIfNeeded(plan, subQueries);
                }
                return plan;
            }
        }
        ValidationException validationException = consumerContext.validationException();
        if (validationException != null) {
            throw validationException;
        }
        return null;
    }
}
