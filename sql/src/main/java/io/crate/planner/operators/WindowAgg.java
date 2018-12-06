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
import io.crate.collections.Lists2;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.crate.planner.operators.LogicalPlanner.extractColumns;

public class WindowAgg extends OneInputPlan {

    private final List<WindowFunction> windowFunctions;
    private final List<Symbol> standalone;

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

            /**
             * Pass along the source outputs as standalone symbols as they might be required in cases like:
             *      select x, avg(x) OVER() from t;
             */
            return new WindowAgg(sourcePlan, windowFunctions, sourcePlan.outputs());
        };
    }

    private WindowAgg(LogicalPlan source, List<WindowFunction> windowFunctions, List<Symbol> standalone) {
        super(source, Lists2.concat(windowFunctions, standalone));
        this.windowFunctions = windowFunctions;
        this.standalone = standalone;
    }

    List<WindowFunction> windowFunctions() {
        return windowFunctions;
    }

    @Override
    protected LogicalPlan updateSource(LogicalPlan newSource, SymbolMapper mapper) {
        return new WindowAgg(newSource, windowFunctions, standalone);
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

        InputColumns.SourceSymbols sourceSymbols = new InputColumns.SourceSymbols(source.outputs());
        List<Symbol> standaloneWithInputs = InputColumns.create(this.standalone, sourceSymbols);
        for (Map.Entry<WindowDefinition, LinkedHashMap<WindowFunction, List<Symbol>>> entry : groupFunctionsByWindow(sourceSymbols, windowFunctions).entrySet()) {
            sourcePlan.addProjection(new WindowAggProjection(entry.getKey(), entry.getValue(), standaloneWithInputs));
        }
        return sourcePlan;
    }

    private static Map<WindowDefinition, LinkedHashMap<WindowFunction, List<Symbol>>> groupFunctionsByWindow(InputColumns.SourceSymbols sourceSymbols,
                                                                                                             List<WindowFunction> windowFunctions) {
        Map<WindowDefinition, LinkedHashMap<WindowFunction, List<Symbol>>> groupedFunctions = new HashMap<>();

        for (WindowFunction windowFunction : windowFunctions) {
            WindowDefinition windowDefinition = windowFunction.windowDefinition();

            WindowFunction windowFunctionSymbol = (WindowFunction) InputColumns.create(windowFunction, sourceSymbols);
            List<Symbol> inputs = InputColumns.create(windowFunction.arguments(), sourceSymbols);
            LinkedHashMap<WindowFunction, List<Symbol>> functionsWithInputs = groupedFunctions.get(windowDefinition);
            if (functionsWithInputs == null) {
                functionsWithInputs = new LinkedHashMap<>();
                groupedFunctions.put(windowDefinition, functionsWithInputs);
            }
            functionsWithInputs.put(windowFunctionSymbol, inputs);
        }
        return groupedFunctions;
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
