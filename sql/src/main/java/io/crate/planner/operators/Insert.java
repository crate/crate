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
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.SelectSymbol;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.Row;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;
import io.crate.execution.dsl.projection.ColumnIndexWriterProjection;
import io.crate.execution.dsl.projection.MergeCountProjection;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Insert extends OneInputPlan {

    private final QueriedRelation relation;
    final ColumnIndexWriterProjection projection;

    public Insert(LogicalPlan source, QueriedRelation relation, ColumnIndexWriterProjection projection) {
        super(source);
        this.relation = relation;
        this.projection = projection;
    }

    @Override
    public ExecutionPlan build(PlannerContext plannerContext,
                               ProjectionBuilder projectionBuilder,
                               int limit,
                               int offset,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               Map<SelectSymbol, Object> subQueryValues) {
        ExecutionPlan executionSubPlan = source.build(
            plannerContext, projectionBuilder, limit, offset, order, pageSizeHint, params, subQueryValues);
        executionSubPlan.addProjection(projection);
        ExecutionPlan executionPlan = Merge.ensureOnHandler(executionSubPlan, plannerContext);
        if (executionPlan == executionSubPlan) {
            return executionPlan;
        }
        executionPlan.addProjection(MergeCountProjection.INSTANCE);
        return executionPlan;
    }

    @Override
    protected LogicalPlan updateSource(LogicalPlan newSource) {
        return new Insert(newSource, relation, projection);
    }

    @Override
    public List<Symbol> outputs() {
        return relation.outputs();
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
    public long numExpectedRows() {
        return 1;
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitInsert(this, context);
    }
}
