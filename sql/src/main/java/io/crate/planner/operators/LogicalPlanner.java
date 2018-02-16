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

import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.AnalyzedStatementVisitor;
import io.crate.analyze.InsertFromSubQueryAnalyzedStatement;
import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.QueriedTableRelation;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.OrderedLimitedRelation;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.relations.UnionSelect;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.MultiPhaseExecutor;
import io.crate.execution.dsl.phases.NodeOperationTree;
import io.crate.execution.dsl.projection.builder.SplitPoints;
import io.crate.execution.engine.NodeOperationTreeGenerator;
import io.crate.expression.symbol.FieldsVisitor;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.RefVisitor;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Functions;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.SelectStatementPlanner;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.TableStats;
import io.crate.planner.consumer.FetchMode;
import io.crate.planner.consumer.InsertFromSubQueryPlanner;
import io.crate.planner.consumer.OptimizingRewriter;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static io.crate.expression.symbol.SelectSymbol.ResultType.SINGLE_COLUMN_SINGLE_VALUE;

/**
 * Planner which can create a {@link ExecutionPlan} using intermediate {@link LogicalPlan} nodes.
 */
public class LogicalPlanner {

    public static final int NO_LIMIT = -1;

    private final OptimizingRewriter optimizingRewriter;
    private final TableStats tableStats;
    private final SelectStatementPlanner selectStatementPlanner;
    private final Visitor statementVisitor = new Visitor();

    public LogicalPlanner(Functions functions, TableStats tableStats) {
        this.optimizingRewriter = new OptimizingRewriter(functions);
        this.tableStats = tableStats;
        selectStatementPlanner = new SelectStatementPlanner(this);
    }

    public LogicalPlan plan(AnalyzedStatement statement, PlannerContext plannerContext) {
        return statementVisitor.process(statement, plannerContext);
    }

    public LogicalPlan planSubSelect(SelectSymbol selectSymbol, PlannerContext plannerContext) {
        final int softLimit, fetchSize;
        if (selectSymbol.getResultType() == SINGLE_COLUMN_SINGLE_VALUE) {
            softLimit = fetchSize = 2;
        } else {
            softLimit = plannerContext.softLimit();
            fetchSize = plannerContext.fetchSize();
        }
        QueriedRelation relation = selectSymbol.relation();
        PlannerContext subSelectPlannerContext = PlannerContext.forSubPlan(plannerContext, softLimit, fetchSize);
        subSelectPlannerContext.applySoftLimit(relation.querySpec());
        SubqueryPlanner subqueryPlanner = new SubqueryPlanner(s -> planSubSelect(s, subSelectPlannerContext));

        LogicalPlan.Builder planBuilder = prePlan(
            relation,
            FetchMode.NEVER_CLEAR,
            subqueryPlanner,
            true);

        planBuilder = tryOptimizeForInSubquery(selectSymbol, relation, planBuilder);
        LogicalPlan optimizedPlan = tryOptimize(planBuilder.build(tableStats, Collections.emptySet()));
        return new RootRelationBoundary(MultiPhase.createIfNeeded(optimizedPlan, relation, subqueryPlanner));
    }

    // In case the subselect is inside an IN() or = ANY() apply a "natural" OrderBy to optimize
    // the building of TermInSetQuery which does a sort on the collection of values.
    // See issue https://github.com/crate/crate/issues/6755
    // If the output values are already sorted (even in desc order) no optimization is needed
    private LogicalPlan.Builder tryOptimizeForInSubquery(SelectSymbol selectSymbol, QueriedRelation relation, LogicalPlan.Builder planBuilder) {
        if (selectSymbol.getResultType() == SelectSymbol.ResultType.SINGLE_COLUMN_MULTIPLE_VALUES) {
            OrderBy relationOrderBy = relation.orderBy();
            if (relationOrderBy == null ||
                relationOrderBy.orderBySymbols().get(0).equals(relation.outputs().get(0)) == false) {
                return Order.create(
                    planBuilder,
                    new OrderBy(relation.outputs(), new boolean[]{false}, new Boolean[]{false}));
            }
        }
        return planBuilder;
    }

    public LogicalPlan plan(QueriedRelation queriedRelation,
                            PlannerContext plannerContext,
                            SubqueryPlanner subqueryPlanner,
                            FetchMode fetchMode) {
        QueriedRelation relation = optimizingRewriter.optimize(queriedRelation, plannerContext.transactionContext());

        LogicalPlan logicalPlan = plan(relation, fetchMode, subqueryPlanner, true)
            .build(tableStats, new LinkedHashSet<>(relation.outputs()));

        LogicalPlan optimizedPlan = tryOptimize(logicalPlan);

        return MultiPhase.createIfNeeded(optimizedPlan, relation, subqueryPlanner);
    }

    /**
     * Runs {@link LogicalPlan}.tryOptimize and returns an optimized plan.
     * @param plan The original plan
     * @return The optimized plan or the original if optimizing is not possible
     */
    private static LogicalPlan tryOptimize(LogicalPlan plan) {
        LogicalPlan optimizedPlan = plan.tryOptimize(null);
        if (optimizedPlan == null) {
            return plan;
        }
        return optimizedPlan;
    }

    static LogicalPlan.Builder plan(QueriedRelation relation,
                                    FetchMode fetchMode,
                                    SubqueryPlanner subqueryPlanner,
                                    boolean isLastFetch) {
        LogicalPlan.Builder builder = prePlan(relation, fetchMode, subqueryPlanner, isLastFetch);
        if (isLastFetch) {
            return builder;
        }
        return RelationBoundary.create(builder, relation, subqueryPlanner);
    }

    private static LogicalPlan.Builder prePlan(QueriedRelation relation,
                                               FetchMode fetchMode,
                                               SubqueryPlanner subqueryPlanner,
                                               boolean isLastFetch) {
        SplitPoints splitPoints = SplitPoints.create(relation);
        return
            FetchOrEval.create(
                Limit.create(
                    Order.create(
                        Filter.create(
                            groupByOrAggregate(
                                collectAndFilter(
                                    relation,
                                    splitPoints.toCollect(),
                                    relation.where(),
                                    subqueryPlanner,
                                    fetchMode
                                ),
                                relation.groupBy(),
                                splitPoints.aggregates()),
                            relation.having()
                        ),
                        relation.orderBy()
                    ),
                    relation.limit(),
                    relation.offset()
                ),
                relation.outputs(),
                fetchMode,
                isLastFetch,
                relation.limit() != null
            );
    }

    private static LogicalPlan.Builder groupByOrAggregate(LogicalPlan.Builder source,
                                                          List<Symbol> groupKeys,
                                                          List<Function> aggregates) {
        if (!groupKeys.isEmpty()) {
            return GroupHashAggregate.create(source, groupKeys, aggregates);
        }
        if (!aggregates.isEmpty()) {
            return (tableStats, usedColumns) ->
                new HashAggregate(source.build(tableStats, extractColumns(aggregates)), aggregates);
        }
        return source;
    }

    private static LogicalPlan.Builder collectAndFilter(QueriedRelation queriedRelation,
                                                        List<Symbol> toCollect,
                                                        WhereClause where,
                                                        SubqueryPlanner subqueryPlanner,
                                                        FetchMode fetchMode) {
        if (queriedRelation instanceof QueriedTableRelation) {
            return Collect.create((QueriedTableRelation) queriedRelation, toCollect, where);
        }
        if (queriedRelation instanceof MultiSourceSelect) {
            return Join.createNodes((MultiSourceSelect) queriedRelation, where, subqueryPlanner);
        }
        if (queriedRelation instanceof UnionSelect) {
            return Union.create((UnionSelect) queriedRelation, subqueryPlanner);
        }
        if (queriedRelation instanceof OrderedLimitedRelation) {
            return plan(((OrderedLimitedRelation) queriedRelation).childRelation(), fetchMode, subqueryPlanner, false);
        }
        if (queriedRelation instanceof QueriedSelectRelation) {
            QueriedSelectRelation selectRelation = (QueriedSelectRelation) queriedRelation;
            return Filter.create(
                plan(selectRelation.subRelation(), fetchMode, subqueryPlanner, false),
                where
            );
        }
        throw new UnsupportedOperationException("Cannot create LogicalPlan from: " + queriedRelation);
    }

    static Set<Symbol> extractColumns(Symbol symbol) {
        LinkedHashSet<Symbol> columns = new LinkedHashSet<>();
        RefVisitor.visitRefs(symbol, columns::add);
        FieldsVisitor.visitFields(symbol, columns::add);
        return columns;
    }

    static Set<Symbol> extractColumns(Collection<? extends Symbol> symbols) {
        LinkedHashSet<Symbol> columns = new LinkedHashSet<>();
        for (Symbol symbol : symbols) {
            RefVisitor.visitRefs(symbol, columns::add);
            FieldsVisitor.visitFields(symbol, columns::add);
        }
        return columns;
    }

    public static void execute(LogicalPlan logicalPlan,
                               DependencyCarrier executor,
                               PlannerContext plannerContext,
                               RowConsumer consumer,
                               Row params,
                               Map<SelectSymbol, Object> subQueryValues) {
        if (logicalPlan.dependencies().isEmpty()) {
            doExecute(logicalPlan, executor, plannerContext, consumer, params, subQueryValues);
        } else {
            MultiPhaseExecutor.execute(logicalPlan.dependencies(), executor, plannerContext, params)
                .whenComplete((valueBySubQuery, failure) -> {
                    if (failure == null) {
                        try {
                            doExecute(logicalPlan, executor, plannerContext, consumer, params, valueBySubQuery);
                        } catch (Exception e) {
                            consumer.accept(null, e);
                        }
                    } else {
                        consumer.accept(null, failure);
                    }
                });
        }
    }

    private static void doExecute(LogicalPlan logicalPlan,
                                  DependencyCarrier executor,
                                  PlannerContext plannerContext,
                                  RowConsumer consumer,
                                  Row params,
                                  Map<SelectSymbol, Object> subQueryValues) {
        ExecutionPlan executionPlan = logicalPlan.build(
            plannerContext, executor.projectionBuilder(), -1, 0, null, null, params, subQueryValues);

        // Ideally we'd include the binding into the `build` step and avoid the after-the-fact symbol mutation
        ExecutionPlanSymbolMapper.map(executionPlan, new SubQueryAndParamBinder(params, subQueryValues));

        NodeOperationTree nodeOpTree = NodeOperationTreeGenerator.fromPlan(executionPlan, executor.localNodeId());
        executor.phasesTaskFactory()
            .create(plannerContext.jobId(), Collections.singletonList(nodeOpTree))
            .execute(consumer);
    }

    private class Visitor extends AnalyzedStatementVisitor<PlannerContext, LogicalPlan> {


        @Override
        protected LogicalPlan visitAnalyzedStatement(AnalyzedStatement analyzedStatement, PlannerContext context) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                "Cannot create LogicalPlan from AnalyzedStatement \"%s\"  - not supported.", analyzedStatement));
        }

        @Override
        public LogicalPlan visitSelectStatement(QueriedRelation relation, PlannerContext context) {
            SubqueryPlanner subqueryPlanner = new SubqueryPlanner((s) -> planSubSelect(s, context));
            return selectStatementPlanner.plan(relation, context, subqueryPlanner);
        }

        @Override
        protected LogicalPlan visitInsertFromSubQueryStatement(InsertFromSubQueryAnalyzedStatement statement, PlannerContext context) {
            SubqueryPlanner subqueryPlanner = new SubqueryPlanner((s) -> planSubSelect(s, context));
            return InsertFromSubQueryPlanner.plan(statement, context, LogicalPlanner.this, subqueryPlanner);
        }
    }
}
