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
import java.util.List;
import java.util.Map;
import java.util.SequencedCollection;
import java.util.Set;
import java.util.function.Function;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.OrderBy;
import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.FetchPhase;
import io.crate.execution.dsl.projection.FetchProjection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;
import io.crate.planner.ReaderAllocations;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.fetch.FetchSource;
import io.crate.types.DataType;

/**
 * <p>
 *   The fetch operator represents a 2 phase execution that is used to reduce value look-ups.
 * </p>
 *
 * For example:
 *
 * <pre>
 *     Limit[10]
 *      └ OrderBy [a]
 *        └ Collect [a, b, c]
 * </pre>
 *
 * Would fetch the values for all columns ([a, b, c]) {@code 10 * num_nodes} times.
 * With the fetch operator:
 *
 * <pre>
 *     Fetch [a, b, c]
 *      └ Limit [10]
 *        └ OrderBy [a]
 *          └ Collect [_fetchid, a]
 * </pre>
 *
 * In a first phase we can fetch the values for [_fetchid, a],
 * then do a sorted merge to apply the limit of 10, and afterwards fetch the values [b, c] for the remaining 10 _fetchids.
 *
 * Note: Fetch always requires a merge to the coordinator node.
 * That's why it is best used after a `Limit`, because a `Limit` requires a merge as well.
 */
public final class Fetch extends ForwardingLogicalPlan {

    private final List<Symbol> outputs;
    private final List<Reference> fetchRefs;
    private final Map<RelationName, FetchSource> fetchSourceByRelation;
    private final Map<Symbol, Symbol> replacedOutputs;

    public Fetch(Map<Symbol, Symbol> replacedOutputs,
                 List<Reference> fetchRefs,
                 Map<RelationName, FetchSource> fetchSourceByRelation,
                 LogicalPlan source) {
        super(source);
        this.outputs = List.copyOf(replacedOutputs.keySet());
        this.replacedOutputs = replacedOutputs;
        this.fetchRefs = fetchRefs;
        this.fetchSourceByRelation = fetchSourceByRelation;
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public LogicalPlan pruneOutputsExcept(SequencedCollection<Symbol> outputsToKeep) {
        return this;
    }

    @Override
    public ExecutionPlan build(DependencyCarrier executor,
                               PlannerContext plannerContext,
                               Set<PlanHint> hints,
                               ProjectionBuilder projectionBuilder,
                               int limit,
                               int offset,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               SubQueryResults subQueryResults) {
        plannerContext.newReaderAllocations();
        var executionPlan = Merge.ensureOnHandler(
            source.build(
                executor,
                plannerContext,
                hints,
                projectionBuilder,
                limit,
                offset,
                order,
                pageSizeHint,
                params,
                subQueryResults
            ),
            plannerContext
        );
        ReaderAllocations readerAllocations = plannerContext.buildReaderAllocations();
        Function<Symbol, Symbol> paramBinder = new SubQueryAndParamBinder(params, subQueryResults);
        FetchPhase fetchPhase = new FetchPhase(
            plannerContext.nextExecutionPhaseId(),
            readerAllocations.nodeReaders().keySet(),
            readerAllocations.bases(),
            readerAllocations.tableIndices(),
            fetchRefs
        );

        ArrayList<Symbol> boundOutputs = new ArrayList<>(replacedOutputs.size());
        for (var entry : replacedOutputs.entrySet()) {
            Symbol key = entry.getKey();
            Symbol value = entry.getValue();
            if (source.outputs().contains(key)) {
                boundOutputs.add(paramBinder.apply(key));
            } else {
                boundOutputs.add(paramBinder.apply(value));
            }
        }
        List<DataType<?>> inputTypes = Symbols.typeView(source.outputs());
        List<Symbol> fetchOutputs = InputColumns.create(boundOutputs, new InputColumns.SourceSymbols(source.outputs()));
        FetchProjection fetchProjection = new FetchProjection(
            fetchPhase.phaseId(),
            plannerContext.fetchSize(),
            fetchSourceByRelation,
            fetchOutputs,
            inputTypes,
            readerAllocations.nodeReaders(),
            readerAllocations.indices(),
            readerAllocations.indicesToIdents()
        );
        executionPlan.addProjection(fetchProjection);
        return new QueryThenFetch(executionPlan, fetchPhase);
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        return new Fetch(replacedOutputs, fetchRefs, fetchSourceByRelation, Lists2.getOnlyElement(sources));
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitFetch(this, context);
    }
}
