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

import static io.crate.execution.dsl.phases.ExecutionPhases.executesOnHandler;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.SequencedCollection;
import java.util.Set;
import java.util.function.Function;

import org.elasticsearch.common.UUIDs;
import org.jetbrains.annotations.Nullable;

import io.crate.analyze.OrderBy;
import io.crate.analyze.WindowDefinition;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.dsl.projection.WindowAggProjection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.pipeline.LimitAndOffset;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.expression.symbol.WindowFunction;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;
import io.crate.planner.ResultDescription;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.distribution.DistributionType;

public class WindowAgg extends ForwardingLogicalPlan {

    final WindowDefinition windowDefinition;
    private final List<WindowFunction> windowFunctions;
    private final List<Symbol> standalone;
    private final List<Symbol> outputs;

    @VisibleForTesting
    public static LogicalPlan create(LogicalPlan source, List<WindowFunction> windowFunctions) {
        if (windowFunctions.isEmpty()) {
            return source;
        }
        LinkedHashMap<WindowDefinition, ArrayList<WindowFunction>> groupedFunctions = new LinkedHashMap<>();
        for (WindowFunction windowFunction : windowFunctions) {
            WindowDefinition windowDefinition = windowFunction.windowDefinition();
            ArrayList<WindowFunction> functions = groupedFunctions.computeIfAbsent(windowDefinition, w -> new ArrayList<>());
            functions.add(windowFunction);
        }
        LogicalPlan lastWindowAgg = source;
        for (Map.Entry<WindowDefinition, ArrayList<WindowFunction>> entry : groupedFunctions.entrySet()) {
            /*
             * Pass along the source outputs as standalone symbols as they might be required in cases like:
             *      select x, avg(x) OVER() from t;
             */

            ArrayList<WindowFunction> functions = entry.getValue();
            WindowDefinition windowDefinition = entry.getKey();
            OrderBy orderBy = windowDefinition.orderBy();
            if (orderBy == null || lastWindowAgg.outputs().containsAll(orderBy.orderBySymbols())) {
                lastWindowAgg = new WindowAgg(lastWindowAgg, windowDefinition, functions, lastWindowAgg.outputs());
            } else {
                // ``WindowProjector.createUpdateProbeValueFunction` expects that all OrderBY symbols are `InputColumn`
                // Here we have a case where there is a function or something in the orderBy expression that is *not*
                // already provided by the source.
                // -> Inject `eval` so that the `orderBy` of the window-function will turn into a `InputColumn`
                Eval eval = new Eval(
                    lastWindowAgg, Lists2.concatUnique(lastWindowAgg.outputs(), orderBy.orderBySymbols()));
                lastWindowAgg = new WindowAgg(eval, windowDefinition, functions, eval.outputs());
            }
        }
        return lastWindowAgg;
    }

    private WindowAgg(LogicalPlan source,
                      WindowDefinition windowDefinition,
                      List<WindowFunction> windowFunctions,
                      List<Symbol> standalone) {
        super(source);
        this.outputs = Lists2.concat(standalone, windowFunctions);
        this.windowDefinition = windowDefinition;
        this.windowFunctions = windowFunctions;
        this.standalone = standalone;
    }

    @Override
    public LogicalPlan pruneOutputsExcept(SequencedCollection<Symbol> outputsToKeep) {
        LinkedHashSet<Symbol> toKeep = new LinkedHashSet<>();
        ArrayList<WindowFunction> newWindowFunctions = new ArrayList<>();
        for (Symbol outputToKeep : outputsToKeep) {
            SymbolVisitors.intersection(outputToKeep, windowFunctions, newWindowFunctions::add);
            SymbolVisitors.intersection(outputToKeep, standalone, toKeep::add);
        }
        for (WindowFunction newWindowFunction : newWindowFunctions) {
            SymbolVisitors.intersection(newWindowFunction, source.outputs(), toKeep::add);
        }
        LogicalPlan newSource = source.pruneOutputsExcept(toKeep);
        if (newSource == source) {
            return this;
        }
        if (newWindowFunctions.isEmpty()) {
            return newSource;
        } else {
            return new WindowAgg(newSource, windowDefinition, List.copyOf(newWindowFunctions), newSource.outputs());
        }
    }

    public List<WindowFunction> windowFunctions() {
        return windowFunctions;
    }

    @SuppressWarnings("unchecked")
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
        InputColumns.SourceSymbols sourceSymbols = new InputColumns.SourceSymbols(source.outputs());

        SubQueryAndParamBinder binder = new SubQueryAndParamBinder(params, subQueryResults);
        Function<Symbol, Symbol> toInputCols = binder.andThen(s -> InputColumns.create(s, sourceSymbols));

        List<WindowFunction> boundWindowFunctions = (List<WindowFunction>)(List<?>) Lists2.map(windowFunctions, toInputCols);
        List<Projection> projections = new ArrayList<>();
        WindowAggProjection windowAggProjection = new WindowAggProjection(
            windowDefinition.map(toInputCols),
            boundWindowFunctions,
            InputColumns.create(this.standalone, sourceSymbols)
        );
        projections.add(windowAggProjection);
        ExecutionPlan sourcePlan = source.build(
            executor,
            plannerContext,
            planHints,
            projectionBuilder,
            LimitAndOffset.NO_LIMIT,
            LimitAndOffset.NO_OFFSET,
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
                UUIDs.dirtyUUID(),
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
                LimitAndOffset.NO_LIMIT,
                LimitAndOffset.NO_OFFSET,
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
        return "WindowAgg{" +
            "source=" + source + ", " +
            "windowDefinition=" + windowDefinition + ", " +
            "windowFunctions=[" + Lists2.joinOn(", ", windowFunctions, WindowFunction::toString) + "]" +
            "}";
    }
}
