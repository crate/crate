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

package io.crate.planner.operators;

import static io.crate.execution.engine.pipeline.LimitAndOffset.NO_LIMIT;

import java.util.List;
import java.util.Set;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.OrderBy;
import io.crate.common.collections.Lists;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;

/**
 * An operator with the primary purpose to ensure that the result is on the handler and no longer distributed.
 */
public class RootRelationBoundary extends ForwardingLogicalPlan {

    public RootRelationBoundary(LogicalPlan source) {
        super(source);
    }

    @Override
    public ExecutionPlan build(DependencyCarrier executor,
                               PlannerContext plannerContext,
                               Set<PlanHint> hints,
                               ProjectionBuilder projectionBuilder,
                               int limit,
                               int offset,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               SubQueryResults subQueryResults) {
        return Merge.ensureOnHandler(source.build(
            executor,
            plannerContext,
            hints,
            projectionBuilder,
            NO_LIMIT,
            0,
            null,
            null,
            params,
            subQueryResults
        ), plannerContext);
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        return new RootRelationBoundary(Lists.getOnlyElement(sources));
    }

    @Override
    public String toString() {
        return "RootBoundary{" + source + '}';
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitRootRelationBoundary(this, context);
    }

    @Override
    public void print(PrintContext printContext) {
        source.print(printContext);
    }
}
