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

import io.crate.analyze.AnalyzedInsertStatement;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.AnalyzedStatementVisitor;
import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.AliasedAnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedView;
import io.crate.analyze.relations.RelationNormalizer;
import io.crate.analyze.relations.TableFunctionRelation;
import io.crate.analyze.relations.UnionSelect;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.MultiPhaseExecutor;
import io.crate.execution.dsl.phases.NodeOperationTree;
import io.crate.execution.dsl.projection.builder.SplitPoints;
import io.crate.execution.dsl.projection.builder.SplitPointsBuilder;
import io.crate.execution.engine.NodeOperationTreeGenerator;
import io.crate.expression.symbol.FieldsVisitor;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.RefVisitor;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.TransactionContext;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.consumer.InsertFromSubQueryPlanner;
import io.crate.planner.optimizer.Optimizer;
import io.crate.planner.optimizer.rule.DeduplicateOrder;
import io.crate.planner.optimizer.rule.MergeAggregateAndCollectToCount;
import io.crate.planner.optimizer.rule.MergeFilterAndCollect;
import io.crate.planner.optimizer.rule.MergeFilters;
import io.crate.planner.optimizer.rule.MoveFilterBeneathBoundary;
import io.crate.planner.optimizer.rule.MoveFilterBeneathFetchOrEval;
import io.crate.planner.optimizer.rule.MoveFilterBeneathGroupBy;
import io.crate.planner.optimizer.rule.MoveFilterBeneathHashJoin;
import io.crate.planner.optimizer.rule.MoveFilterBeneathNestedLoop;
import io.crate.planner.optimizer.rule.MoveFilterBeneathOrder;
import io.crate.planner.optimizer.rule.MoveFilterBeneathProjectSet;
import io.crate.planner.optimizer.rule.MoveFilterBeneathUnion;
import io.crate.planner.optimizer.rule.MoveFilterBeneathWindowAgg;
import io.crate.planner.optimizer.rule.MoveOrderBeneathBoundary;
import io.crate.planner.optimizer.rule.MoveOrderBeneathFetchOrEval;
import io.crate.planner.optimizer.rule.MoveOrderBeneathNestedLoop;
import io.crate.planner.optimizer.rule.MoveOrderBeneathUnion;
import io.crate.planner.optimizer.rule.RemoveRedundantFetchOrEval;
import io.crate.planner.optimizer.rule.RewriteCollectToGet;
import io.crate.planner.optimizer.rule.RewriteFilterOnOuterJoinToInnerJoin;
import io.crate.planner.optimizer.rule.RewriteGroupByKeysLimitToTopNDistinct;
import io.crate.statistics.TableStats;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

import static io.crate.expression.symbol.SelectSymbol.ResultType.SINGLE_COLUMN_SINGLE_VALUE;

/**
 * Planner which can create a {@link ExecutionPlan} using intermediate {@link LogicalPlan} nodes.
 */
public class LogicalPlanner {

    public static final int NO_LIMIT = -1;

    private final Optimizer optimizer;
    private final TableStats tableStats;
    private final Visitor statementVisitor = new Visitor();
    private final Functions functions;
    private final RelationNormalizer relationNormalizer;
    private final Optimizer writeOptimizer;

    public LogicalPlanner(Functions functions, TableStats tableStats, Supplier<Version> minNodeVersionInCluster) {
        this.optimizer = new Optimizer(
            List.of(
                new RemoveRedundantFetchOrEval(),
                new MergeAggregateAndCollectToCount(),
                new MergeFilters(),
                new MoveFilterBeneathBoundary(),
                new MoveFilterBeneathFetchOrEval(),
                new MoveFilterBeneathOrder(),
                new MoveFilterBeneathProjectSet(),
                new MoveFilterBeneathHashJoin(),
                new MoveFilterBeneathNestedLoop(),
                new MoveFilterBeneathUnion(),
                new MoveFilterBeneathGroupBy(),
                new MoveFilterBeneathWindowAgg(),
                new MergeFilterAndCollect(),
                new RewriteFilterOnOuterJoinToInnerJoin(functions),
                new MoveOrderBeneathUnion(),
                new MoveOrderBeneathNestedLoop(),
                new MoveOrderBeneathBoundary(),
                new MoveOrderBeneathFetchOrEval(),
                new DeduplicateOrder(),
                new RewriteCollectToGet(functions),
                new RewriteGroupByKeysLimitToTopNDistinct()
            ),
            minNodeVersionInCluster
        );
        this.writeOptimizer = new Optimizer(
            List.of(new RewriteInsertFromSubQueryToInsertFromValues()),
            minNodeVersionInCluster
        );
        this.tableStats = tableStats;
        this.functions = functions;
        this.relationNormalizer = new RelationNormalizer(functions);
    }

    public LogicalPlan plan(AnalyzedStatement statement, PlannerContext plannerContext) {
        return statementVisitor.process(statement, plannerContext);
    }

    public LogicalPlan planSubSelect(SelectSymbol selectSymbol, PlannerContext plannerContext) {
        CoordinatorTxnCtx txnCtx = plannerContext.transactionContext();
        AnalyzedRelation relation = relationNormalizer.normalize(
            selectSymbol.relation(), txnCtx);

        final int fetchSize;
        final java.util.function.Function<LogicalPlan, LogicalPlan> maybeApplySoftLimit;
        if (selectSymbol.getResultType() == SINGLE_COLUMN_SINGLE_VALUE) {
            // SELECT (SELECT foo FROM t)
            //         ^^^^^^^^^^^^^^^^^
            // The subquery must return at most 1 row, if more than 1 row is returned semantics require us to throw an error.
            // So we limit the query to 2 if there is no limit to avoid retrieval of many rows while being able to validate max1row
            fetchSize = 2;
            maybeApplySoftLimit = relation.limit() == null
                ? plan -> new Limit(plan, Literal.of(2L), Literal.of(0L))
                : plan -> plan;
        } else {
            fetchSize = 0;
            maybeApplySoftLimit = plan -> plan;
        }
        PlannerContext subSelectPlannerContext = PlannerContext.forSubPlan(plannerContext, fetchSize);
        SubqueryPlanner subqueryPlanner = new SubqueryPlanner(s -> planSubSelect(s, subSelectPlannerContext));
        LogicalPlan plan = prePlan(
            relation,
            subqueryPlanner,
            functions,
            txnCtx,
            Set.of(),
            tableStats,
            subSelectPlannerContext.params()
        );

        plan = tryOptimizeForInSubquery(selectSymbol, relation, plan);
        LogicalPlan optimizedPlan = optimizer.optimize(maybeApplySoftLimit.apply(plan), tableStats, txnCtx);
        return new RootRelationBoundary(optimizedPlan);
    }

    // In case the subselect is inside an IN() or = ANY() apply a "natural" OrderBy to optimize
    // the building of TermInSetQuery which does a sort on the collection of values.
    // See issue https://github.com/crate/crate/issues/6755
    // If the output values are already sorted (even in desc order) no optimization is needed
    private LogicalPlan tryOptimizeForInSubquery(SelectSymbol selectSymbol, AnalyzedRelation relation, LogicalPlan planBuilder) {
        if (selectSymbol.getResultType() == SelectSymbol.ResultType.SINGLE_COLUMN_MULTIPLE_VALUES) {
            OrderBy relationOrderBy = relation.orderBy();
            Symbol firstOutput = relation.outputs().get(0);
            if ((relationOrderBy == null || relationOrderBy.orderBySymbols().get(0).equals(firstOutput) == false)
                && DataTypes.PRIMITIVE_TYPES.contains(firstOutput.valueType())) {
                return Order.create(planBuilder, new OrderBy(Collections.singletonList(firstOutput)));
            }
        }
        return planBuilder;
    }


    public LogicalPlan normalizeAndPlan(AnalyzedRelation analyzedRelation,
                                        PlannerContext plannerContext,
                                        SubqueryPlanner subqueryPlanner,
                                        Set<PlanHint> hints) {
        CoordinatorTxnCtx coordinatorTxnCtx = plannerContext.transactionContext();
        AnalyzedRelation relation = relationNormalizer.normalize(analyzedRelation, coordinatorTxnCtx);
        LogicalPlan logicalPlan = plan(
            relation,
            subqueryPlanner,
            true,
            functions,
            coordinatorTxnCtx,
            hints,
            tableStats,
            plannerContext.params()
        );
        return optimizer.optimize(logicalPlan, tableStats, coordinatorTxnCtx);
    }

    static LogicalPlan plan(AnalyzedRelation relation,
                            SubqueryPlanner subqueryPlanner,
                            boolean isLastFetch,
                            Functions functions,
                            CoordinatorTxnCtx txnCtx,
                            Set<PlanHint> hints,
                            TableStats tableStats,
                            Row params) {
        LogicalPlan plan = prePlan(relation, subqueryPlanner, functions, txnCtx, hints, tableStats, params);
        if (isLastFetch) {
            return plan;
        }
        if (relation instanceof UnionSelect) {
            // Union already acts as boundary and doesn't require a additional dedicated boundary symbol.
            // Using a boundary would even break some optimization rules
            // E.g. Order -> Boundary -> Union; MoveOrderBeneathBoundary would prematurely remap
            // Fields to point to the left child of the Union.
            return plan;
        }
        return RelationBoundary.create(plan, relation);
    }

    private static LogicalPlan prePlan(AnalyzedRelation relation,
                                       SubqueryPlanner subqueryPlanner,
                                       Functions functions,
                                       CoordinatorTxnCtx txnCtx,
                                       Set<PlanHint> hints,
                                       TableStats tableStats,
                                       Row params) {
        SplitPoints splitPoints = SplitPointsBuilder.create(relation);
        return MultiPhase.createIfNeeded(
            Eval.create(
                Limit.create(
                    Order.create(
                        Distinct.create(
                            ProjectSet.create(
                                WindowAgg.create(
                                    Filter.create(
                                        groupByOrAggregate(
                                            collectAndFilter(
                                                relation,
                                                splitPoints.toCollect(),
                                                relation.where(),
                                                subqueryPlanner,
                                                functions,
                                                txnCtx,
                                                hints,
                                                tableStats,
                                                params
                                            ),
                                            relation.groupBy(),
                                            splitPoints.aggregates(),
                                            tableStats,
                                            params
                                        ),
                                        relation.having()
                                    ),
                                    splitPoints.windowFunctions()
                                ),
                                splitPoints.tableFunctions()
                            ),
                            relation.isDistinct(),
                            relation.outputs(),
                            tableStats
                        ),
                        relation.orderBy()
                    ),
                    relation.limit(),
                    relation.offset()
                ),
                relation.outputs()
            ),
            relation,
            subqueryPlanner
        );
    }


    private static LogicalPlan groupByOrAggregate(LogicalPlan source,
                                                  List<Symbol> groupKeys,
                                                  List<Function> aggregates,
                                                  TableStats tableStats,
                                                  Row params) {
        if (!groupKeys.isEmpty()) {
            long numExpectedRows = GroupHashAggregate.approximateDistinctValues(source.numExpectedRows(), tableStats, groupKeys);
            return new GroupHashAggregate(source, groupKeys, aggregates, numExpectedRows);
        }
        if (!aggregates.isEmpty()) {
            return new HashAggregate(source, aggregates);
        }
        return source;
    }

    private static LogicalPlan collectAndFilter(AnalyzedRelation analyzedRelation,
                                                List<Symbol> toCollect,
                                                WhereClause where,
                                                SubqueryPlanner subqueryPlanner,
                                                Functions functions,
                                                CoordinatorTxnCtx txnCtx,
                                                Set<PlanHint> hints,
                                                TableStats tableStats,
                                                Row params) {
        if (analyzedRelation instanceof AnalyzedView) {
            return plan(((AnalyzedView) analyzedRelation).relation(), subqueryPlanner, false, functions, txnCtx, hints, tableStats, params);
        }
        if (analyzedRelation instanceof AliasedAnalyzedRelation) {
            return plan(((AliasedAnalyzedRelation) analyzedRelation).relation(), subqueryPlanner, false, functions, txnCtx, hints, tableStats, params);
        }
        if (analyzedRelation instanceof AbstractTableRelation) {
            return Collect.create(((AbstractTableRelation<?>) analyzedRelation), toCollect, where, hints, tableStats, params);
        }
        if (analyzedRelation instanceof MultiSourceSelect) {
            return JoinPlanBuilder.createNodes(
                (MultiSourceSelect) analyzedRelation,
                where,
                subqueryPlanner,
                functions,
                txnCtx,
                hints,
                tableStats,
                params
            );
        }
        if (analyzedRelation instanceof UnionSelect) {
            return Union.create((UnionSelect) analyzedRelation, subqueryPlanner, functions, txnCtx, hints, tableStats, params);
        }
        if (analyzedRelation instanceof TableFunctionRelation) {
            return TableFunction.create(((TableFunctionRelation) analyzedRelation), toCollect, where);
        }
        if (analyzedRelation instanceof QueriedSelectRelation) {
            QueriedSelectRelation<?> selectRelation = (QueriedSelectRelation<?>) analyzedRelation;
            AnalyzedRelation subRelation = selectRelation.subRelation();
            if (subRelation instanceof AbstractTableRelation<?>) {
                return Collect.create(((AbstractTableRelation<?>) subRelation), toCollect, where, hints, tableStats, params);
            } else if (subRelation instanceof TableFunctionRelation) {
                return TableFunction.create(((TableFunctionRelation) subRelation), toCollect, where);
            }
            return Filter.create(
                plan(subRelation, subqueryPlanner, false, functions, txnCtx, hints, tableStats, params),
                where
            );
        }
        throw new UnsupportedOperationException("Cannot create LogicalPlan from: " + analyzedRelation);
    }

    public static Set<Symbol> extractColumns(Symbol symbol) {
        LinkedHashSet<Symbol> columns = new LinkedHashSet<>();
        RefVisitor.visitRefs(symbol, columns::add);
        FieldsVisitor.visitFields(symbol, columns::add);
        return columns;
    }

    public static Set<Symbol> extractColumns(Collection<? extends Symbol> symbols) {
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
                               SubQueryResults subQueryResults,
                               boolean enableProfiling) {
        if (logicalPlan.dependencies().isEmpty()) {
            doExecute(logicalPlan, executor, plannerContext, consumer, params, subQueryResults, enableProfiling);
        } else {
            MultiPhaseExecutor.execute(logicalPlan.dependencies(), executor, plannerContext, params)
                .whenComplete((valueBySubQuery, failure) -> {
                    if (failure == null) {
                        doExecute(logicalPlan, executor, plannerContext, consumer, params, valueBySubQuery, false);
                    } else {
                        consumer.accept(null, failure);
                    }
                });
        }
    }

    private static void doExecute(LogicalPlan logicalPlan,
                                  DependencyCarrier dependencies,
                                  PlannerContext plannerContext,
                                  RowConsumer consumer,
                                  Row params,
                                  SubQueryResults subQueryResults,
                                  boolean enableProfiling) {
        NodeOperationTree nodeOpTree;
        try {
            nodeOpTree = getNodeOperationTree(logicalPlan, dependencies, plannerContext, params, subQueryResults);
        } catch (Throwable t) {
            consumer.accept(null, t);
            return;
        }
        executeNodeOpTree(
            dependencies,
            plannerContext.transactionContext(),
            plannerContext.jobId(),
            consumer,
            enableProfiling,
            nodeOpTree
        );
    }

    public static NodeOperationTree getNodeOperationTree(LogicalPlan logicalPlan,
                                                         DependencyCarrier executor,
                                                         PlannerContext plannerContext,
                                                         Row params,
                                                         SubQueryResults subQueryResults) {
        ExecutionPlan executionPlan = logicalPlan.build(
            plannerContext, executor.projectionBuilder(), -1, 0, null, null, params, subQueryResults);
        return NodeOperationTreeGenerator.fromPlan(executionPlan, executor.localNodeId());
    }

    public static void executeNodeOpTree(DependencyCarrier dependencies,
                                         TransactionContext txnCtx,
                                         UUID jobId,
                                         RowConsumer consumer,
                                         boolean enableProfiling,
                                         NodeOperationTree nodeOpTree) {
        dependencies.phasesTaskFactory()
            .create(jobId, Collections.singletonList(nodeOpTree), enableProfiling)
            .execute(consumer, txnCtx);
    }

    private class Visitor extends AnalyzedStatementVisitor<PlannerContext, LogicalPlan> {

        @Override
        protected LogicalPlan visitAnalyzedStatement(AnalyzedStatement analyzedStatement, PlannerContext context) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                "Cannot create LogicalPlan from AnalyzedStatement \"%s\"  - not supported.", analyzedStatement));
        }

        @Override
        public LogicalPlan visitSelectStatement(AnalyzedRelation relation, PlannerContext context) {
            SubqueryPlanner subqueryPlanner = new SubqueryPlanner((s) -> planSubSelect(s, context));
            LogicalPlan logicalPlan = normalizeAndPlan(relation, context, subqueryPlanner, Set.of());
            return new RootRelationBoundary(logicalPlan);
        }

        @Override
        protected LogicalPlan visitAnalyzedInsertStatement(AnalyzedInsertStatement statement, PlannerContext context) {
            SubqueryPlanner subqueryPlanner = new SubqueryPlanner((s) -> planSubSelect(s, context));
            return writeOptimizer.optimize(
                InsertFromSubQueryPlanner.plan(
                    statement,
                    context,
                    LogicalPlanner.this,
                    subqueryPlanner),
                tableStats,
                context.transactionContext()
            );
        }
    }
}
