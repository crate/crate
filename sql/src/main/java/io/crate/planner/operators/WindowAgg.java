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
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.collections.Lists2;
import io.crate.data.Row;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.dsl.projection.WindowAggProjection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.pipeline.TopN;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.WindowFunction;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.ExplainLeaf;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;
import io.crate.planner.ResultDescription;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.distribution.DistributionType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static io.crate.execution.dsl.phases.ExecutionPhases.executesOnHandler;
import static io.crate.planner.operators.LogicalPlanner.extractColumns;

public class WindowAgg implements LogicalPlan {

    final WindowDefinition windowDefinition;
    private final List<WindowFunction> windowFunctions;
    private final List<Symbol> standalone;
    final LogicalPlan source;
    private final List<Symbol> outputs;

    static LogicalPlan.Builder create(LogicalPlan.Builder source, List<WindowFunction> windowFunctions) {
        if (windowFunctions.isEmpty()) {
            return source;
        }

        for (WindowFunction windowFunction : windowFunctions) {
            WindowDefinition windowDefinition = windowFunction.windowDefinition();
            if (!windowDefinition.windowFrameDefinition().equals(WindowDefinition.DEFAULT_WINDOW_FRAME)) {
                throw new UnsupportedFeatureException("Custom frame definitions are not supported");
            }
        }

        return (tableStats, usedBeforeNextFetch) -> {
            HashSet<Symbol> allUsedColumns = new HashSet<>(extractColumns(usedBeforeNextFetch));
            Set<Symbol> columnsUsedInFunctions = extractColumns(windowFunctions);
            LinkedHashMap<WindowDefinition, ArrayList<WindowFunction>> groupedFunctions = new LinkedHashMap<>();
            for (WindowFunction windowFunction : windowFunctions) {
                WindowDefinition windowDefinition = windowFunction.windowDefinition();
                OrderBy orderBy = windowDefinition.orderBy();
                if (orderBy != null) {
                    columnsUsedInFunctions.addAll(extractColumns(orderBy.orderBySymbols()));
                }
                columnsUsedInFunctions.addAll(extractColumns(windowDefinition.partitions()));
                ArrayList<WindowFunction> functions = groupedFunctions.computeIfAbsent(windowDefinition, w -> new ArrayList<>());
                functions.add(windowFunction);
            }
            allUsedColumns.addAll(columnsUsedInFunctions);
            LogicalPlan sourcePlan = source.build(tableStats, allUsedColumns);

            LogicalPlan lastWindowAgg = sourcePlan;
            for (Map.Entry<WindowDefinition, ArrayList<WindowFunction>> entry : groupedFunctions.entrySet()) {
                /**
                 * Pass along the source outputs as standalone symbols as they might be required in cases like:
                 *      select x, avg(x) OVER() from t;
                 */
                lastWindowAgg = new WindowAgg(lastWindowAgg, entry.getKey(), entry.getValue(), lastWindowAgg.outputs());
            }
            return lastWindowAgg;
        };
    }

    private WindowAgg(LogicalPlan source, WindowDefinition windowDefinition, List<WindowFunction> windowFunctions, List<Symbol> standalone) {
        this.source = source;
        this.outputs = Lists2.concat(standalone, windowFunctions);
        this.windowDefinition = windowDefinition;
        this.windowFunctions = windowFunctions;
        this.standalone = standalone;
    }

    List<WindowFunction> windowFunctions() {
        return windowFunctions;
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
        InputColumns.SourceSymbols sourceSymbols = new InputColumns.SourceSymbols(source.outputs());
        LinkedHashMap<WindowFunction, List<Symbol>> functionsWithInputs = new LinkedHashMap<>(windowFunctions.size(), 1f);
        for (WindowFunction windowFunction : windowFunctions) {
            List<Symbol> inputs = InputColumns.create(windowFunction.arguments(), sourceSymbols);
            functionsWithInputs.put(windowFunction, inputs);
        }
        List<Projection> projections = new ArrayList<>();
        WindowAggProjection windowAggProjection = new WindowAggProjection(
            windowDefinition.map(s -> InputColumns.create(s, sourceSymbols)),
            functionsWithInputs,
            InputColumns.create(this.standalone, sourceSymbols)
        );
        projections.add(windowAggProjection);
        ExecutionPlan sourcePlan = source.build(
            plannerContext,
            projectionBuilder,
            TopN.NO_LIMIT,
            TopN.NO_OFFSET,
            null,
            pageSizeHint,
            params,
            subQueryResults
        );
        ResultDescription resultDescription = sourcePlan.resultDescription();
        boolean executesOnHandler = executesOnHandler(plannerContext.handlerNode(), resultDescription.nodeIds());
        boolean nonDistExecution = windowDefinition.partitions().isEmpty()
                                   || resultDescription.hasRemainingLimitOrOffset()
                                   || executesOnHandler;
        if (nonDistExecution) {
            sourcePlan = Merge.ensureOnHandler(sourcePlan, plannerContext);
            for (Projection projection : projections) {
                sourcePlan.addProjection(projection);
            }
        } else {
            sourcePlan.setDistributionInfo(new DistributionInfo(
                DistributionType.MODULO,
                source.outputs().indexOf(windowDefinition.partitions().iterator().next()))
            );
            MergePhase distWindowAgg = new MergePhase(
                UUID.randomUUID(),
                plannerContext.nextExecutionPhaseId(),
                "distWindowAgg",
                resultDescription.nodeIds().size(),
                resultDescription.numOutputs(),
                resultDescription.nodeIds(),
                resultDescription.streamOutputs(),
                projections,
                DistributionInfo.DEFAULT_BROADCAST,
                null
            );
            return new Merge(
                sourcePlan,
                distWindowAgg,
                TopN.NO_LIMIT,
                TopN.NO_OFFSET,
                windowAggProjection.outputs().size(),
                resultDescription.maxRowsPerNode(),
                null
            );
        }
        return sourcePlan;
    }

    @Nullable
    static OrderBy createOrderByInclPartitionBy(WindowDefinition windowDefinition) {
        var orderBy = windowDefinition.orderBy();
        var partitions = windowDefinition.partitions();
        if (orderBy == null) {
            if (partitions.isEmpty()) {
                return null;
            } else {
                return new OrderBy(partitions);
            }
        } else {
            return orderBy.prependUnique(partitions);
        }
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
    public Map<Symbol, Symbol> expressionMapping() {
        return source.expressionMapping();
    }

    @Override
    public List<AbstractTableRelation> baseTables() {
        return source.baseTables();
    }

    @Override
    public List<LogicalPlan> sources() {
        return List.of(source);
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        return new WindowAgg(Lists2.getOnlyElement(sources), windowDefinition, windowFunctions, standalone);
    }

    @Override
    public Map<LogicalPlan, SelectSymbol> dependencies() {
        return source.dependencies();
    }

    @Override
    public long numExpectedRows() {
        return source.numExpectedRows();
    }

    @Override
    public long estimatedRowSize() {
        return source.estimatedRowSize();
    }

    @Override
    public String toString() {
        return "WindowAgg{[" + ExplainLeaf.printList(windowFunctions) + "]}";
    }
}
