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
import io.crate.analyze.WindowDefinition;
import io.crate.data.Row;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.execution.dsl.projection.WindowAggProjection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.WindowFunction;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.ExplainLeaf;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.crate.planner.operators.LogicalPlanner.extractColumns;

public class WindowAgg extends OneInputPlan {

    private final List<WindowFunction> windowFunctions;
    private final List<Symbol> outputs;

    static LogicalPlan.Builder create(LogicalPlan.Builder source, List<WindowFunction> windowFunctions) {
        if (windowFunctions.isEmpty()) {
            return source;
        }

        for (WindowFunction windowFunction : windowFunctions) {
            WindowDefinition windowDefinition = windowFunction.windowDefinition();
            if (windowDefinition.partitions().size() > 0 ||
                windowDefinition.orderBy() != null ||
                windowDefinition.windowFrameDefinition() != null) {
                throw new UnsupportedFeatureException("Custom window definitions are currently not supported. " +
                                                      "Only empty OVER() windows are supported. ");
            }
        }

        return (tableStats, usedBeforeNextFetch) -> {
            HashSet<Symbol> allUsedColumns = new HashSet<>(usedBeforeNextFetch);
            Set<Symbol> columnsUsedInFunctions = extractColumns(windowFunctions);
            allUsedColumns.addAll(columnsUsedInFunctions);
            LogicalPlan sourcePlan = source.build(tableStats, allUsedColumns);

            return new WindowAgg(sourcePlan, windowFunctions);
        };
    }

    private WindowAgg(LogicalPlan source, List<WindowFunction> windowFunctions) {
        super(source);
        this.windowFunctions = windowFunctions;
        this.outputs = new ArrayList<>(windowFunctions);
    }

    public List<WindowFunction> windowFunctions() {
        return windowFunctions;
    }

    @Override
    protected LogicalPlan updateSource(LogicalPlan newSource, SymbolMapper mapper) {
        return new WindowAgg(newSource, windowFunctions);
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

        sourcePlan = Merge.ensureOnHandler(sourcePlan, plannerContext);
        sourcePlan.addProjection(new WindowAggProjection(groupFunctionsByWindow(source.outputs(), windowFunctions)));
        return sourcePlan;
    }

    private static Map<WindowDefinition, List<Symbol>> groupFunctionsByWindow(List<Symbol> inputs,
                                                                              List<WindowFunction> windowFunctions) {
        InputColumns.SourceSymbols sourceSymbols = new InputColumns.SourceSymbols(inputs);
        Map<WindowDefinition, List<Symbol>> groupedFunctions = new HashMap<>();

        for (WindowFunction windowFunction : windowFunctions) {
            WindowDefinition windowDefinition = windowFunction.windowDefinition();

            Symbol windowFunctionSymbol = InputColumns.create(windowFunction, sourceSymbols);
            List<Symbol> functions = groupedFunctions.get(windowDefinition);
            if (functions == null) {
                ArrayList<Symbol> functionsForWindow = new ArrayList<>();
                functionsForWindow.add(windowFunctionSymbol);
                groupedFunctions.put(windowDefinition, functionsForWindow);
            } else {
                functions.add(windowFunctionSymbol);
            }
        }
        return Collections.unmodifiableMap(groupedFunctions);
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitWindowAgg(this, context);
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public String toString() {
        return "WindowAgg{[" + ExplainLeaf.printList(windowFunctions) + "]}";
    }
}
