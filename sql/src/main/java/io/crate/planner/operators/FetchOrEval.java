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
import io.crate.analyze.symbol.Symbol;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.projection.EvalProjection;
import io.crate.planner.projection.builder.InputColumns;
import io.crate.planner.projection.builder.ProjectionBuilder;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Set;

class FetchOrEval implements LogicalPlan {

    final LogicalPlan source;
    private final Set<Symbol> usedColumns;
    final List<Symbol> outputs;

    static LogicalPlan.Builder create(LogicalPlan.Builder source, List<Symbol> outputs) {
        return usedColumns -> new FetchOrEval(source.build(Collections.emptySet()), usedColumns, outputs);
    }

    private FetchOrEval(LogicalPlan source, Set<Symbol> usedColumns, List<Symbol> outputs) {
        this.source = source;
        this.usedColumns = usedColumns;
        this.outputs = outputs;
    }

    @Override
    public Plan build(Planner.Context plannerContext,
                      ProjectionBuilder projectionBuilder,
                      int limit,
                      int offset,
                      @Nullable OrderBy order) {

        Plan plan = source.build(plannerContext, projectionBuilder, limit, offset, order);
        if (!source.outputs().equals(outputs)) {
            InputColumns.Context ctx = new InputColumns.Context(source.outputs());
            plan.addProjection(new EvalProjection(InputColumns.create(outputs, ctx)), null, null, null);
        }
        return plan;
    }

    @Override
    public LogicalPlan tryCollapse() {
        LogicalPlan collapsed = source.tryCollapse();
        if (collapsed == this) {
            return this;
        }
        return new FetchOrEval(collapsed, usedColumns, outputs);
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }
}
