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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.SequencedCollection;
import java.util.Set;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.OrderBy;
import io.crate.common.collections.Lists;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.ProjectSetProjection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.FunctionType;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;

public class ProjectSet extends ForwardingLogicalPlan {

    final List<Function> tableFunctions;
    final List<Symbol> standalone;
    private final List<Symbol> outputs;

    static LogicalPlan create(LogicalPlan source, List<Function> tableFunctions) {
        if (tableFunctions.isEmpty()) {
            return source;
        }
        //
        // source.outputs() is used as standalone to pass along all source outputs as well;
        // Parent operators will discard them if not required
        // The reason to do this is that we've no good way to detect what is required. E.g.
        // select tableFunction(agg), agg, x
        //  -> agg is used as argument in tableFunction, but is also standalone,
        //     so we can't simply discard any source outputs that are used as arguments for the table functions.
        //  -> x might be converted to _fetch by the Collect operator,
        //       so we don't necessarily "get" the outputs we would expect based on the select list.
        //
        // ----
        //
        // Need to create 1 projectSet operator per level of nesting, because table functions cannot receive non-arrays as arguments
        //
        //  unnest(regexp_matches(...)) -> 2 operators
        //  unnest(arr)                 -> 1 operator
        //
        List<List<Function>> nestedFunctions = new ArrayList<>();
        nestedFunctions.add(tableFunctions);
        while (true) {
            List<Function> childTableFunctions = tableFunctions.stream()
                .flatMap(func -> func.arguments().stream())
                .filter(arg -> arg instanceof Function fn && fn.signature().getKind() == FunctionType.TABLE)
                .map(x -> (Function) x)
                .toList();

            if (childTableFunctions.isEmpty()) {
                break;
            }
            nestedFunctions.add(childTableFunctions);
            tableFunctions = childTableFunctions;
        }
        LogicalPlan result = source;
        for (int i = nestedFunctions.size() - 1; i >= 0; i--) {
            List<Symbol> standalone = result.outputs().stream()
                .filter(x -> !(x instanceof Function fn && fn.signature().getKind() == FunctionType.TABLE))
                .toList();
            result = new ProjectSet(result, nestedFunctions.get(i), standalone);
        }
        return result;
    }

    private ProjectSet(LogicalPlan source, List<Function> tableFunctions, List<Symbol> standalone) {
        super(source);
        this.outputs = Lists.concat(tableFunctions, standalone);
        this.tableFunctions = tableFunctions;
        this.standalone = standalone;
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
        ExecutionPlan sourcePlan = source.build(
            executor,
            plannerContext,
            planHints,
            projectionBuilder,
            limit,
            offset,
            order,
            pageSizeHint,
            params,
            subQueryResults
        );

        // When a query with placeholders looks like 'SELECT UNNEST(?)' then source is a table function over EMPTY_ROW_TABLE_RELATION
        // and parameter binding done inside TableFunction has no effect as EMPTY_ROW_TABLE_RELATION has zero arguments.
        SubQueryAndParamBinder paramBinder = new SubQueryAndParamBinder(params, subQueryResults);

        InputColumns.SourceSymbols sourceSymbols = new InputColumns.SourceSymbols(source.outputs());
        List<Symbol> tableFunctionsWithInputs = InputColumns.create(Lists.map(this.tableFunctions, paramBinder), sourceSymbols);
        List<Symbol> standaloneWithInputs = InputColumns.create(this.standalone, sourceSymbols);
        sourcePlan.addProjection(new ProjectSetProjection(tableFunctionsWithInputs, standaloneWithInputs));
        return sourcePlan;
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    public List<Symbol> standaloneOutputs() {
        return standalone;
    }

    @Override
    public LogicalPlan pruneOutputsExcept(SequencedCollection<Symbol> outputsToKeep) {
        LinkedHashSet<Symbol> toKeep = new LinkedHashSet<>();
        LinkedHashSet<Symbol> newStandalone = new LinkedHashSet<>();
        for (Symbol outputToKeep : outputsToKeep) {
            Symbols.intersection(outputToKeep, standalone, newStandalone::add);
        }
        for (Function tableFunction : tableFunctions) {
            Symbols.intersection(tableFunction, source.outputs(), toKeep::add);
        }
        toKeep.addAll(newStandalone);
        LogicalPlan newSource = source.pruneOutputsExcept(toKeep);
        if (newSource == source) {
            return this;
        }
        return new ProjectSet(newSource, tableFunctions, List.copyOf(newStandalone));
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        return new ProjectSet(Lists.getOnlyElement(sources), tableFunctions, standalone);
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitProjectSet(this, context);
    }
}
