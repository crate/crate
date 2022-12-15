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

package io.crate.planner.optimizer.iterative;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import io.crate.analyze.OrderBy;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanId;
import io.crate.planner.operators.LogicalPlanVisitor;
import io.crate.planner.operators.PlanHint;
import io.crate.planner.operators.SubQueryResults;
import io.crate.statistics.TableStats;

public class GroupReference implements LogicalPlan {
    private final int groupId;
    private final List<Symbol> outputs;
    private final LogicalPlanId id;

    public GroupReference(LogicalPlanId id, int groupId, List<Symbol> outputs)
    {
        this.id = id;
        this.groupId = groupId;
        this.outputs = List.copyOf(outputs);
    }

    public int groupId()
    {
        return groupId;
    }

    @Override
    public List<LogicalPlan> sources()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LogicalPlanId id() {
        return id;
    }

    @Override
    public LogicalPlan pruneOutputsExcept(TableStats tableStats, Collection<Symbol> outputsToKeep) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<LogicalPlan, SelectSymbol> dependencies() {
        return Map.of();
    }

    @Override
    public long numExpectedRows() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long estimatedRowSize() {
        return 0;
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitGroupReference(this, context);
    }

    @Override
    public Set<RelationName> getRelationNames() {
        throw new UnsupportedOperationException();
    }


    @Override
    public ExecutionPlan build(DependencyCarrier dependencyCarrier,
                               PlannerContext plannerContext,
                               Set<PlanHint> planHints,
                               ProjectionBuilder projectionBuilder,
                               int limit,
                               int offset,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               SubQueryResults subQueryResults) {
        return null;
    }

    @Override
    public List<Symbol> outputs()
    {
        return outputs;
    }

    @Override
    public List<AbstractTableRelation<?>> baseTables() {
        return List.of();
    }
}
