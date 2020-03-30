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
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.CountPhase;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.projection.MergeCountProjection;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.CountPlan;
import io.crate.statistics.TableStats;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * An optimized version for "select count(*) from t where ..."
 */
public class Count implements LogicalPlan {

    private static final String COUNT_PHASE_NAME = "count-merge";

    final AbstractTableRelation<?> tableRelation;
    final WhereClause where;
    private final List<Symbol> outputs;

    public Count(Function countFunction, AbstractTableRelation<?> tableRelation, WhereClause where) {
        this.outputs = List.of(countFunction);
        this.tableRelation = tableRelation;
        this.where = where;
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
        var normalizer = new EvaluatingNormalizer(
            plannerContext.functions(),
            RowGranularity.CLUSTER,
            null,
            tableRelation
        );
        var binder = new SubQueryAndParamBinder(params, subQueryResults)
            .andThen(x -> normalizer.normalize(x, plannerContext.transactionContext()));

        // bind all parameters and possible subQuery values and re-analyze the query
        // (could result in a NO_MATCH, routing could've changed, etc).
        WhereClause boundWhere = WhereClauseAnalyzer.resolvePartitions(
            where.map(binder),
            tableRelation,
            plannerContext.functions(),
            plannerContext.transactionContext()
        );
        Routing routing = plannerContext.allocateRouting(
            tableRelation.tableInfo(),
            boundWhere,
            RoutingProvider.ShardSelection.ANY,
            plannerContext.transactionContext().sessionContext());
        CountPhase countPhase = new CountPhase(
            plannerContext.nextExecutionPhaseId(),
            routing,
            boundWhere.queryOrFallback(),
            DistributionInfo.DEFAULT_BROADCAST
        );
        MergePhase mergePhase = new MergePhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            COUNT_PHASE_NAME,
            countPhase.nodeIds().size(),
            1,
            Collections.singletonList(plannerContext.handlerNode()),
            Collections.singletonList(DataTypes.LONG),
            Collections.singletonList(MergeCountProjection.INSTANCE),
            DistributionInfo.DEFAULT_BROADCAST,
            null
        );
        return new CountPlan(countPhase, mergePhase);
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public List<AbstractTableRelation<?>> baseTables() {
        return List.of(tableRelation);
    }

    @Override
    public List<LogicalPlan> sources() {
        return List.of();
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        assert sources.isEmpty() : "Count has no sources, cannot replace them";
        return this;
    }

    @Override
    public LogicalPlan pruneOutputsExcept(TableStats tableStats, Collection<Symbol> outputsToKeep) {
        return this;
    }

    @Override
    public Map<LogicalPlan, SelectSymbol> dependencies() {
        return Map.of();
    }

    @Override
    public long numExpectedRows() {
        return 1L;
    }

    @Override
    public long estimatedRowSize() {
        return Long.BYTES;
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitCount(this, context);
    }

    @Override
    public void print(PrintContext printContext) {
        printContext
            .text("Count[")
            .text(tableRelation.tableInfo().ident().toString())
            .text(" | ")
            .text(where.queryOrFallback().toString())
            .text("]");
    }
}
