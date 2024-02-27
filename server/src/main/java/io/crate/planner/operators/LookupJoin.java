/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.planner.operators;

import java.util.List;
import java.util.Set;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.OrderBy;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.common.collections.Lists;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.format.Style;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;

public class LookupJoin extends ForwardingLogicalPlan {

    private AbstractTableRelation<?> largerRelation;
    private AbstractTableRelation<?> smallerRelation;
    private Symbol from;
    private Symbol to;


    public LookupJoin(AbstractTableRelation<?> largerRelation,
                       AbstractTableRelation<?> smallerRelation,
                       Symbol from,
                       Symbol to,
                       LogicalPlan multiPhase) {
        super(multiPhase);
        this.largerRelation = largerRelation;
        this.smallerRelation = smallerRelation;
        this.from = from;
        this.to = to;
    }

    @Override
    public ExecutionPlan build(DependencyCarrier executor,
                               PlannerContext plannerContext,
                               Set<PlanHint> planHints,
                               ProjectionBuilder projectionBuilder,
                               int limit,
                               int offset,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               SubQueryResults subQueryResults) {
        return source.build(
            executor, plannerContext, planHints, projectionBuilder, limit, offset, order, pageSizeHint, params, subQueryResults);
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        return new LookupJoin(largerRelation, smallerRelation, from, to, Lists.getOnlyElement(sources));
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitLookupJoin(this, context);
    }

    @Override
    public void print(PrintContext printContext) {
        printContext
            .text("LookupJoin[")
            .text(from.toString(Style.QUALIFIED))
            .text(" = ")
            .text(to.toString(Style.QUALIFIED))
            .text("]");
        printStats(printContext);
        printContext.nest(source::print);
    }
}
