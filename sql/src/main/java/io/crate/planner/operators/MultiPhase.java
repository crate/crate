/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner.operators;

import io.crate.analyze.OrderBy;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.MultiPhasePlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.SubqueryPlanner;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is the {@link LogicalPlan} equivalent of the {@link MultiPhasePlan} plan.
 * It's used to describe that other logical plans needed to be executed first.
  */
public class MultiPhase extends ForwardingLogicalPlan {

    private final Map<LogicalPlan, SelectSymbol> subQueries;

    public static LogicalPlan createIfNeeded(LogicalPlan source,
                                             AnalyzedRelation relation,
                                             SubqueryPlanner subqueryPlanner) {
        Map<LogicalPlan, SelectSymbol> subQueries = subqueryPlanner.planSubQueries(relation);
        if (subQueries.isEmpty()) {
            return source;
        } else {
            return new MultiPhase(source, subQueries);
        }
    }

    private MultiPhase(LogicalPlan source, Map<LogicalPlan, SelectSymbol> subQueries) {
        super(source);
        HashMap<LogicalPlan, SelectSymbol> allSubQueries = new HashMap<>(source.dependencies());
        allSubQueries.putAll(subQueries);
        this.subQueries = Collections.unmodifiableMap(allSubQueries);
    }

    @Override
    public ExecutionPlan build(PlannerContext plannerContext,
                               ProjectionBuilder projectionBuilder,
                               int limit,
                               int offset,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               SubQueryResults subQueryResults) {
        return source.build(
            plannerContext, projectionBuilder, limit, offset, order, pageSizeHint, params, subQueryResults);
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        return new MultiPhase(Lists2.getOnlyElement(sources), subQueries);
    }

    @Override
    public Map<LogicalPlan, SelectSymbol> dependencies() {
        return subQueries;
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitMultiPhase(this, context);
    }
}
