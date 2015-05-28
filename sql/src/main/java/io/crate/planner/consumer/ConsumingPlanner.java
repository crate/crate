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
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.exceptions.ValidationException;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class ConsumingPlanner {

    private final List<Consumer> consumers = new ArrayList<>();

    @Inject
    public ConsumingPlanner() {
        consumers.add(new NonDistributedGroupByConsumer());
        consumers.add(new ReduceOnCollectorGroupByConsumer());
        consumers.add(new DistributedGroupByConsumer());
        consumers.add(new CountConsumer());
        consumers.add(new GlobalAggregateConsumer());
        consumers.add(new ESGetConsumer());
        consumers.add(new QueryThenFetchConsumer());
        consumers.add(new InsertFromSubQueryConsumer());
        consumers.add(new QueryAndFetchConsumer());
    }

    @Nullable
    public Plan plan(AnalyzedRelation rootRelation, Planner.Context plannerContext) {
        ConsumerContext consumerContext = new ConsumerContext(rootRelation, plannerContext);
        for (int i = 0; i < consumers.size(); i++) {
            Consumer consumer = consumers.get(i);
            if (consumer.consume(consumerContext.rootRelation(), consumerContext)) {
                if (consumerContext.rootRelation() instanceof PlannedAnalyzedRelation) {
                    Plan plan = ((PlannedAnalyzedRelation) consumerContext.rootRelation()).plan();
                    assert plan != null;
                    return plan;
                } else {
                    i = 0;
                }
            }
        }
        ValidationException validationException = consumerContext.validationException();
        if (validationException != null) {
            throw validationException;
        }
        return null;
    }
}
