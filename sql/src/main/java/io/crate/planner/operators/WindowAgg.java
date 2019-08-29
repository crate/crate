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
import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.dsl.projection.WindowAggProjection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.pipeline.TopN;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.WindowFunction;
import io.crate.expression.symbol.WindowFunctionContext;
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
import java.util.UUID;

import static io.crate.execution.dsl.phases.ExecutionPhases.executesOnHandler;
import static io.crate.planner.operators.LogicalPlanner.extractColumns;

public class WindowAgg extends ForwardingLogicalPlan {

    final WindowDefinition windowDefinition;
    private final List<WindowFunction> windowFunctions;
    private final List<Symbol> standalone;
    private final List<Symbol> outputs;

    static LogicalPlan.Builder create(LogicalPlan.Builder source, List<WindowFunction> windowFunctions) {
        if (windowFunctions.isEmpty()) {
            return source;
        }

        return (tableStats, hints, usedBeforeNextFetch) -> {
            HashSet<Symbol> allUsedColumns = new HashSet<>(extractColumns(usedBeforeNextFetch));
            allUsedColumns.addAll(extractColumns(windowFunctions));
            LinkedHashMap<WindowDefinition, ArrayList<WindowFunction>> groupedFunctions = new LinkedHashMap<>();
            for (WindowFunction windowFunction : windowFunctions) {
                WindowDefinition windowDefinition = windowFunction.windowDefinition();
                ArrayList<WindowFunction> functions = groupedFunctions.computeIfAbsent(windowDefinition, w -> new ArrayList<>());
                functions.add(windowFunction);
            }
            LogicalPlan lastWindowAgg = source.build(tableStats, hints, allUsedColumns);
            for (Map.Entry<WindowDefinition, ArrayList<WindowFunction>> entry : groupedFunctions.entrySet()) {
                /*
                 * Pass along the source outputs as standalone symbols as they might be required in cases like:
                 *      select x, avg(x) OVER() from t;
                 */
                lastWindowAgg = new WindowAgg(lastWindowAgg, entry.getKey(), entry.getValue(), lastWindowAgg.outputs());
            }
            return lastWindowAgg;
        };
    }

    private WindowAgg(LogicalPlan source, WindowDefinition windowDefinition, List<WindowFunction> windowFunctions, List<Symbol> standalone) {
        super(source);
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
        ArrayList<WindowFunctionContext> windowFunctionContexts =
            new ArrayList<>(windowFunctions.size());

        SubQueryAndParamBinder binder = new SubQueryAndParamBinder(params, subQueryResults);
        for (var windowFunction : windowFunctions) {
            var boundWindowFunction = (WindowFunction) binder.apply(windowFunction);

            List<Symbol> inputs = InputColumns.create(
                boundWindowFunction.arguments(),
                sourceSymbols);

            Symbol filter = boundWindowFunction.filter();
            Symbol filterInput;
            if (filter != null) {
                filterInput = InputColumns.create(filter, sourceSymbols);
            } else {
                filterInput = Literal.BOOLEAN_TRUE;
            }
            windowFunctionContexts.add(new WindowFunctionContext(
                boundWindowFunction,
                inputs,
                filterInput));
        }
        List<Projection> projections = new ArrayList<>();
        WindowAggProjection windowAggProjection = new WindowAggProjection(
            windowDefinition.map(binder.andThen(s -> InputColumns.create(s, sourceSymbols))),
            windowFunctionContexts,
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

    public WindowDefinition windowDefinition() {
        return windowDefinition;
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        return new WindowAgg(Lists2.getOnlyElement(sources), windowDefinition, windowFunctions, standalone);
    }

    @Override
    public String toString() {
        return "WindowAgg{[" + ExplainLeaf.printList(windowFunctions) + "]}";
    }
}
