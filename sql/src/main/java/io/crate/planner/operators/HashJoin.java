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
import io.crate.data.Row;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Map;

class HashJoin extends TwoInputPlan {

    HashJoin(LogicalPlan lhs,
             LogicalPlan rhs) {
        super(lhs, rhs, new ArrayList<>());
    }


    @Override
    public ExecutionPlan build(PlannerContext plannerContext, ProjectionBuilder projectionBuilder, int limit, int offset, @Nullable OrderBy order, @Nullable Integer pageSizeHint, Row params, Map<SelectSymbol, Object> subQueryValues) {
        throw new UnsupportedOperationException("HashJoin not implemented");
    }

    @Override
    protected LogicalPlan updateSources(LogicalPlan newLeftSource, LogicalPlan newRightSource) {
        throw new UnsupportedOperationException("HashJoin not implemented");
    }

    @Override
    public long numExpectedRows() {
        throw new UnsupportedOperationException("HashJoin not implemented");
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        throw new UnsupportedOperationException("HashJoin not implemented");
    }
}
