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
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.collections.Lists2;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.ColumnIndexWriterProjection;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.execution.dsl.projection.MergeCountProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Insert extends OneInputPlan {

    private final ColumnIndexWriterProjection writeToTable;

    @Nullable
    private final EvalProjection applyCasts;

    public Insert(LogicalPlan source, ColumnIndexWriterProjection writeToTable, @Nullable EvalProjection applyCasts) {
        super(source);
        this.writeToTable = writeToTable;
        this.applyCasts = applyCasts;
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
        ExecutionPlan sourcePlan = source.build(
            plannerContext, projectionBuilder, limit, offset, order, pageSizeHint, params, subQueryResults);
        if (applyCasts != null) {
            sourcePlan.addProjection(applyCasts);
        }
        if (sourcePlan.resultDescription().hasRemainingLimitOrOffset()) {
            ExecutionPlan localMerge = Merge.ensureOnHandler(sourcePlan, plannerContext);
            localMerge.addProjection(writeToTable);
            return localMerge;
        } else {
            sourcePlan.addProjection(writeToTable);
            ExecutionPlan localMerge = Merge.ensureOnHandler(sourcePlan, plannerContext);
            if (sourcePlan != localMerge) {
                localMerge.addProjection(MergeCountProjection.INSTANCE);
            }
            return localMerge;
        }
    }

    @Override
    public List<Symbol> outputs() {
        return MergeCountProjection.OUTPUTS;
    }

    @Override
    public Map<Symbol, Symbol> expressionMapping() {
        return Collections.emptyMap();
    }

    @Override
    public List<AbstractTableRelation> baseTables() {
        return Collections.emptyList();
    }

    @Override
    public List<LogicalPlan> sources() {
        return List.of(source);
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        return new Insert(Lists2.getOnlyElement(sources), writeToTable, applyCasts);
    }

    @Override
    public long numExpectedRows() {
        return 1;
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitInsert(this, context);
    }

    @Override
    public StatementType type() {
        return StatementType.INSERT;
    }

    public Collection<Projection> projections() {
        if (applyCasts == null) {
            return List.of(writeToTable);
        } else {
            return List.of(applyCasts, writeToTable);
        }
    }
}
