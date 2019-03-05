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
import io.crate.collections.Lists2;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.ProjectSetProjection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.types.ObjectType;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.crate.planner.operators.LogicalPlanner.extractColumns;

public class ProjectSet extends OneInputPlan {

    final List<Function> tableFunctions;
    final List<Symbol> standalone;

    static LogicalPlan.Builder create(LogicalPlan.Builder source, List<Function> tableFunctions) {
        if (tableFunctions.isEmpty()) {
            return source;
        }
        return (tableStats, usedBeforeNextFetch) -> {
            HashSet<Symbol> allUsedColumns = new HashSet<>(usedBeforeNextFetch);
            Set<Symbol> columnsUsedInTableFunctions = extractColumns(tableFunctions);
            allUsedColumns.addAll(columnsUsedInTableFunctions);
            LogicalPlan sourcePlan = source.build(tableStats, allUsedColumns);

            // Use sourcePlan.outputs() as standalone to simply pass along all source outputs as well;
            // Parent operators will discard them if not required
            // The reason to do this is that we've no good way to detect what is required. E.g.
            // select tableFunction(agg), agg, x
            //  -> agg is used as argument in tableFunction, but is also standalone,
            //     so we can't simply discard any source outputs that are used as arguments for the table functions.
            //  -> x might be converted to _fetch by the Collect operator,
            //       so we don't necessarily "get" the outputs we would expect based on the select list.
            return new ProjectSet(sourcePlan, tableFunctions, sourcePlan.outputs());
        };
    }

    private ProjectSet(LogicalPlan source, List<Function> tableFunctions, List<Symbol> standalone) {
        super(source, Lists2.concat(tableFunctions, standalone));
        this.tableFunctions = tableFunctions;
        this.standalone = standalone;
        for (Function tableFunction : tableFunctions) {
            // We should distinguish between single-column objects and multiple columns;
            // but the type system doesn't allow that yet.
            // So this restricts us to table functions that don't return objects
            if (tableFunction.info().returnType().id() == ObjectType.ID) {
                throw new UnsupportedOperationException("Table function used in select list must not return multiple columns");
            }
        }
    }

    @Override
    protected LogicalPlan updateSource(LogicalPlan newSource, SymbolMapper mapper) {
        return new ProjectSet(
            newSource,
            Lists2.map(tableFunctions, s -> (Function) mapper.apply(newSource.outputs(), s)),
            Lists2.map(standalone, s -> mapper.apply(newSource.outputs(), s))
        );
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
            plannerContext,
            projectionBuilder,
            limit,
            offset,
            order,
            pageSizeHint,
            params,
            subQueryResults
        );
        InputColumns.SourceSymbols sourceSymbols = new InputColumns.SourceSymbols(source.outputs());
        List<Symbol> tableFunctionsWithInputs = InputColumns.create(this.tableFunctions, sourceSymbols);
        List<Symbol> standaloneWithInputs = InputColumns.create(this.standalone, sourceSymbols);
        sourcePlan.addProjection(new ProjectSetProjection(tableFunctionsWithInputs, standaloneWithInputs));
        return sourcePlan;
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitProjectSet(this, context);
    }
}
