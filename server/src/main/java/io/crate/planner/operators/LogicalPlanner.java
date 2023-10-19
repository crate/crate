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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.elasticsearch.Version;

import io.crate.analyze.AnalyzedInsertStatement;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.AnalyzedStatementVisitor;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.AliasedAnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.AnalyzedView;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.TableFunctionRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.relations.UnionSelect;
import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.exceptions.ConversionException;
import io.crate.exceptions.CrateException;
import io.crate.execution.MultiPhaseExecutor;
import io.crate.execution.dsl.phases.NodeOperationTree;
import io.crate.execution.dsl.projection.builder.SplitPoints;
import io.crate.execution.dsl.projection.builder.SplitPointsBuilder;
import io.crate.execution.engine.NodeOperationTreeGenerator;
import io.crate.expression.symbol.FieldReplacer;
import io.crate.expression.symbol.FieldsVisitor;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.RefVisitor;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.SelectSymbol.ResultType;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.SubqueryPlanner.SubQueries;
import io.crate.planner.consumer.InsertFromSubQueryPlanner;
import io.crate.planner.optimizer.Optimizer;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.iterative.IterativeOptimizer;
import io.crate.planner.optimizer.rule.DeduplicateOrder;
import io.crate.planner.optimizer.rule.MergeAggregateAndCollectToCount;
import io.crate.planner.optimizer.rule.MergeAggregateRenameAndCollectToCount;
import io.crate.planner.optimizer.rule.MergeFilterAndCollect;
import io.crate.planner.optimizer.rule.MergeFilters;
import io.crate.planner.optimizer.rule.MoveConstantJoinConditionsBeneathNestedLoop;
import io.crate.planner.optimizer.rule.MoveFilterBeneathCorrelatedJoin;
import io.crate.planner.optimizer.rule.MoveFilterBeneathEval;
import io.crate.planner.optimizer.rule.MoveFilterBeneathGroupBy;
import io.crate.planner.optimizer.rule.MoveFilterBeneathJoin;
import io.crate.planner.optimizer.rule.MoveFilterBeneathOrder;
import io.crate.planner.optimizer.rule.MoveFilterBeneathProjectSet;
import io.crate.planner.optimizer.rule.MoveFilterBeneathRename;
import io.crate.planner.optimizer.rule.MoveFilterBeneathUnion;
import io.crate.planner.optimizer.rule.MoveFilterBeneathWindowAgg;
import io.crate.planner.optimizer.rule.MoveLimitBeneathEval;
import io.crate.planner.optimizer.rule.MoveLimitBeneathRename;
import io.crate.planner.optimizer.rule.MoveOrderBeneathFetchOrEval;
import io.crate.planner.optimizer.rule.MoveOrderBeneathNestedLoop;
import io.crate.planner.optimizer.rule.MoveOrderBeneathRename;
import io.crate.planner.optimizer.rule.MoveOrderBeneathUnion;
import io.crate.planner.optimizer.rule.OptimizeCollectWhereClauseAccess;
import io.crate.planner.optimizer.rule.RemoveRedundantFetchOrEval;
import io.crate.planner.optimizer.rule.ReorderHashJoin;
import io.crate.planner.optimizer.rule.ReorderNestedLoopJoin;
import io.crate.planner.optimizer.rule.RewriteFilterOnOuterJoinToInnerJoin;
import io.crate.planner.optimizer.rule.RewriteGroupByKeysLimitToLimitDistinct;
import io.crate.planner.optimizer.rule.RewriteNestedLoopJoinToHashJoin;
import io.crate.planner.optimizer.rule.RewriteToQueryThenFetch;
import io.crate.types.DataTypes;

/**
 * Planner which can create a {@link ExecutionPlan} using intermediate {@link LogicalPlan} nodes.
 */
public class LogicalPlanner {
    private final IterativeOptimizer optimizer;
    // Join ordering optimization rules have their own optimizer, because these rules have
    // little interaction with the other rules and we want to avoid unnecessary pattern matches on them.
    private final IterativeOptimizer joinOrderOptimizer;
    private final Visitor statementVisitor = new Visitor();
    private final Optimizer writeOptimizer;
    private final Optimizer fetchOptimizer;

    public static final List<Rule<?>> ITERATIVE_OPTIMIZER_RULES = List.of(
        new RemoveRedundantFetchOrEval(),
        new MergeAggregateAndCollectToCount(),
        new MergeAggregateRenameAndCollectToCount(),
        new MergeFilters(),
        new MoveFilterBeneathRename(),
        new MoveFilterBeneathEval(),
        new MoveFilterBeneathOrder(),
        new MoveFilterBeneathProjectSet(),
        new MoveFilterBeneathJoin(),
        new MoveFilterBeneathCorrelatedJoin(),
        new MoveFilterBeneathUnion(),
        new MoveFilterBeneathGroupBy(),
        new MoveFilterBeneathWindowAgg(),
        new MoveLimitBeneathRename(),
        new MoveLimitBeneathEval(),
        new MergeFilterAndCollect(),
        new RewriteFilterOnOuterJoinToInnerJoin(),
        new MoveOrderBeneathUnion(),
        new MoveOrderBeneathNestedLoop(),
        new MoveOrderBeneathFetchOrEval(),
        new MoveOrderBeneathRename(),
        new DeduplicateOrder(),
        new OptimizeCollectWhereClauseAccess(),
        new RewriteGroupByKeysLimitToLimitDistinct(),
        new MoveConstantJoinConditionsBeneathNestedLoop(),
        new RewriteNestedLoopJoinToHashJoin()
    );

    public static final List<Rule<?>> JOIN_ORDER_OPTIMIZER_RULES = List.of(
        new ReorderHashJoin(),
        new ReorderNestedLoopJoin()
    );

    public static final List<Rule<?>> FETCH_OPTIMIZER_RULES = List.of(
        new RemoveRedundantFetchOrEval(),
        new RewriteToQueryThenFetch()
    );

    // This rule is private because the RewriteInsertFromSubQueryToInsertFromValues
    // rule is mandatory to make inserts from a sub-query work correctly
    // and should therefore not be exposed to be configurable
    private static final List<Rule<?>> WRITE_OPTIMIZER_RULES =
        List.of(new RewriteInsertFromSubQueryToInsertFromValues());

    public LogicalPlanner(NodeContext nodeCtx, Supplier<Version> minNodeVersionInCluster) {
        this.optimizer = new IterativeOptimizer(
            nodeCtx,
            minNodeVersionInCluster,
            ITERATIVE_OPTIMIZER_RULES
        );
        this.joinOrderOptimizer = new IterativeOptimizer(
            nodeCtx,
            minNodeVersionInCluster,
            JOIN_ORDER_OPTIMIZER_RULES
        );
        this.fetchOptimizer = new Optimizer(
            nodeCtx,
            minNodeVersionInCluster,
            FETCH_OPTIMIZER_RULES
        );
        this.writeOptimizer = new Optimizer(
            nodeCtx,
            minNodeVersionInCluster,
            WRITE_OPTIMIZER_RULES
        );
    }

    public LogicalPlan plan(AnalyzedStatement statement, PlannerContext plannerContext) {
        return statement.accept(statementVisitor, plannerContext);
    }

    public LogicalPlan planSubSelect(SelectSymbol selectSymbol, PlannerContext plannerContext) {
        CoordinatorTxnCtx txnCtx = plannerContext.transactionContext();
        AnalyzedRelation relation = selectSymbol.relation();

        final int fetchSize;
        final java.util.function.Function<LogicalPlan, LogicalPlan> maybeApplySoftLimit;
        ResultType resultType = selectSymbol.getResultType();
        switch (resultType) {
            case SINGLE_COLUMN_EXISTS:
                // Exists only needs to know if there are any rows
                fetchSize = 1;
                maybeApplySoftLimit = plan -> new Limit(plan, Literal.of(1), Literal.of(0));
                break;
            case SINGLE_COLUMN_SINGLE_VALUE:
                // SELECT (SELECT foo FROM t)
                //         ^^^^^^^^^^^^^^^^^
                // The subquery must return at most 1 row, if more than 1 row is returned semantics require us to throw an error.
                // So we limit the query to 2 if there is no limit to avoid retrieval of many rows while being able to validate max1row
                fetchSize = 2;
                maybeApplySoftLimit = plan -> new Limit(plan, Literal.of(2L), Literal.of(0L));
                break;
            case SINGLE_COLUMN_MULTIPLE_VALUES:
            default:
                fetchSize = 0;
                maybeApplySoftLimit = plan -> plan;
                break;
        }
        PlannerContext subSelectPlannerContext = PlannerContext.forSubPlan(plannerContext, fetchSize);
        SubqueryPlanner subqueryPlanner = new SubqueryPlanner(s -> planSubSelect(s, subSelectPlannerContext));
        var planBuilder = new PlanBuilder(
            subqueryPlanner,
            txnCtx,
            plannerContext.planStats(),
            subSelectPlannerContext.params()
        );
        LogicalPlan plan = relation.accept(planBuilder, relation.outputs());

        plan = tryOptimizeForInSubquery(selectSymbol, relation, plan);
        LogicalPlan optimizedPlan = optimizer.optimize(maybeApplySoftLimit.apply(plan), plannerContext.planStats(), txnCtx);
        optimizedPlan = joinOrderOptimizer.optimize(optimizedPlan, plannerContext.planStats(), txnCtx);
        LogicalPlan prunedPlan = optimizedPlan.pruneOutputsExcept(relation.outputs());
        assert prunedPlan.outputs().equals(optimizedPlan.outputs()) : "Pruned plan must have the same outputs as original plan";
        return new RootRelationBoundary(prunedPlan);
    }

    // In case the subselect is inside an IN() or = ANY() apply a "natural" OrderBy to optimize
    // the building of TermInSetQuery which does a sort on the collection of values.
    // See issue https://github.com/crate/crate/issues/6755
    // If the output values are already sorted (even in desc order) no optimization is needed
    private LogicalPlan tryOptimizeForInSubquery(SelectSymbol selectSymbol, AnalyzedRelation relation, LogicalPlan planBuilder) {
        if (selectSymbol.isCorrelated()) {
            return planBuilder;
        }

        if (selectSymbol.parentIsOrderSensitive() == false
            && selectSymbol.getResultType() == SelectSymbol.ResultType.SINGLE_COLUMN_MULTIPLE_VALUES
            && relation instanceof QueriedSelectRelation queriedRelation) {

            OrderBy relationOrderBy = queriedRelation.orderBy();
            Symbol firstOutput = queriedRelation.outputs().get(0);
            if ((relationOrderBy == null || relationOrderBy.orderBySymbols().get(0).equals(firstOutput) == false)
                && DataTypes.isPrimitive(firstOutput.valueType())) {

                return Order.create(planBuilder, new OrderBy(Collections.singletonList(firstOutput)));
            }
        }
        return planBuilder;
    }


    public LogicalPlan plan(AnalyzedRelation relation,
                            PlannerContext plannerContext,
                            SubqueryPlanner subqueryPlanner,
                            boolean avoidTopLevelFetch) {
        CoordinatorTxnCtx coordinatorTxnCtx = plannerContext.transactionContext();
        PlanStats planStats = plannerContext.planStats();
        var planBuilder = new PlanBuilder(
            subqueryPlanner,
            coordinatorTxnCtx,
            planStats,
            plannerContext.params()
        );
        LogicalPlan logicalPlan = relation.accept(planBuilder, relation.outputs());
        LogicalPlan optimizedPlan = optimizer.optimize(logicalPlan, planStats, coordinatorTxnCtx);
        optimizedPlan = joinOrderOptimizer.optimize(optimizedPlan, planStats, coordinatorTxnCtx);
        assert logicalPlan.outputs().equals(optimizedPlan.outputs()) : "Optimized plan must have the same outputs as original plan";
        LogicalPlan prunedPlan = optimizedPlan.pruneOutputsExcept(relation.outputs());
        assert prunedPlan.outputs().equals(optimizedPlan.outputs()) : "Pruned plan must have the same outputs as original plan";
        LogicalPlan fetchOptimized = fetchOptimizer.optimize(
            prunedPlan,
            planStats,
            coordinatorTxnCtx
        );
        if (fetchOptimized != prunedPlan || avoidTopLevelFetch) {
            return fetchOptimized;
        }
        assert logicalPlan.outputs().equals(fetchOptimized.outputs()) : "Fetch optimized plan must have the same outputs as original plan";
        // Doing a second pass here to also rewrite additional plan patterns to "Fetch"
        // The `fetchOptimizer` operators on `Limit - X` fragments of a tree.
        // This here instead operators on a narrow selection of top-level patterns
        //
        // The reason for this is that some plans are cheaper to execute as fetch
        // even if there is no operator that reduces the number of records
        return RewriteToQueryThenFetch.tryRewrite(relation, fetchOptimized);
    }

    static class PlanBuilder extends AnalyzedRelationVisitor<List<Symbol>, LogicalPlan> {

        private final SubqueryPlanner subqueryPlanner;
        private final PlanStats planStats;
        private final Row params;

        private PlanBuilder(SubqueryPlanner subqueryPlanner,
                            CoordinatorTxnCtx txnCtx,
                            PlanStats planStats,
                            Row params) {
            this.subqueryPlanner = subqueryPlanner;
            this.planStats = planStats;
            this.params = params;
        }

        @Override
        public LogicalPlan visitAnalyzedRelation(AnalyzedRelation relation, List<Symbol> outputs) {
            throw new UnsupportedOperationException(relation.getClass().getSimpleName() + " NYI");
        }

        @Override
        public LogicalPlan visitTableFunctionRelation(TableFunctionRelation relation, List<Symbol> outputs) {
            // MultiPhase is needed here but not in `DocTableRelation` or `TableRelation` because
            // `TableFunctionRelation` is also used for top-level `VALUES`
            //    -> so there wouldn't be a `QueriedSelectRelation` that can do the MultiPhase handling

            SubQueries subQueries = subqueryPlanner.planSubQueries(relation);
            return MultiPhase.createIfNeeded(
                subQueries.uncorrelated(),
                TableFunction.create(relation, relation.outputs(), WhereClause.MATCH_ALL)
            );
        }

        @Override
        public LogicalPlan visitDocTableRelation(DocTableRelation relation, List<Symbol> outputs) {
            return new Collect(relation, outputs, WhereClause.MATCH_ALL);
        }

        @Override
        public LogicalPlan visitTableRelation(TableRelation relation, List<Symbol> outputs) {
            return new Collect(relation, outputs, WhereClause.MATCH_ALL);
        }

        @Override
        public LogicalPlan visitAliasedAnalyzedRelation(AliasedAnalyzedRelation relation, List<Symbol> outputs) {
            var child = relation.relation();
            if (child instanceof AbstractTableRelation<?>) {
                List<Symbol> mappedOutputs = Lists2.map(outputs, FieldReplacer.bind(relation::resolveField));
                var source = child.accept(this, mappedOutputs);
                return new Rename(outputs, relation.relationName(), relation, source);
            } else {
                // Can't do outputs propagation because field reverse resolving could be ambiguous
                //  `SELECT * FROM (select * from t as t1, t as t2)` -> x can refer to t1.x or t2.x
                var source = child.accept(this, child.outputs());
                return new Rename(relation.outputs(), relation.relationName(), relation, source);
            }
        }

        @Override
        public LogicalPlan visitView(AnalyzedView view, List<Symbol> outputs) {
            var child = view.relation();
            var source = child.accept(this, child.outputs());
            return new Rename(view.outputs(), view.relationName(), view, source);
        }

        @Override
        public LogicalPlan visitUnionSelect(UnionSelect union, List<Symbol> outputs) {
            var lhsRel = union.left();
            var rhsRel = union.right();
            return Distinct.create(
                new Union(
                    lhsRel.accept(this, lhsRel.outputs()),
                    rhsRel.accept(this, rhsRel.outputs()),
                    union.outputs()),
                union.isDistinct(),
                union.outputs()
            );
        }

        @Override
        public LogicalPlan visitQueriedSelectRelation(QueriedSelectRelation relation, List<Symbol> outputs) {
            SplitPoints splitPoints = SplitPointsBuilder.create(relation);
            SubQueries subQueries = subqueryPlanner.planSubQueries(relation);
            LogicalPlan source = JoinPlanBuilder.buildJoinTree(
                relation.from(),
                relation.where(),
                relation.joinPairs(),
                subQueries,
                rel -> {
                    if (relation.from().size() == 1) {
                        return rel.accept(this, splitPoints.toCollect());
                    } else {
                        // Need to pass along the `splitPoints.toCollect` symbols to the relation the symbols belong to
                        // We could get rid of `SplitPoints` and the logic here if we
                        // a) introduce a column pruning
                        // b) Make sure tableRelations contain all columns (incl. sys-columns) in `outputs`

                        var toCollect = new LinkedHashSet<Symbol>(splitPoints.toCollect().size());
                        Consumer<Reference> addRefIfMatch = ref -> {
                            if (ref.ident().tableIdent().equals(rel.relationName())) {
                                toCollect.add(ref);
                            }
                        };
                        Consumer<ScopedSymbol> addFieldIfMatch = field -> {
                            if (field.relation().equals(rel.relationName())) {
                                toCollect.add(field);
                            }
                        };
                        for (Symbol symbol : splitPoints.toCollect()) {
                            RefVisitor.visitRefs(symbol, addRefIfMatch);
                            FieldsVisitor.visitFields(symbol, addFieldIfMatch);
                        }
                        FieldsVisitor.visitFields(relation.where(), addFieldIfMatch);
                        RefVisitor.visitRefs(relation.where(), addRefIfMatch);
                        for (var joinPair : relation.joinPairs()) {
                            var condition = joinPair.condition();
                            if (condition != null) {
                                FieldsVisitor.visitFields(condition, addFieldIfMatch);
                                RefVisitor.visitRefs(condition, addRefIfMatch);
                            }
                        }
                        return rel.accept(this, List.copyOf(toCollect));
                    }
                }
            );
            Symbol having = relation.having();
            if (having != null && Symbols.containsCorrelatedSubQuery(having)) {
                throw new UnsupportedOperationException("Cannot use correlated subquery in HAVING clause");
            }
            return MultiPhase.createIfNeeded(
                subQueries.uncorrelated(),
                Eval.create(
                    Limit.create(
                        Order.create(
                            Distinct.create(
                                ProjectSet.create(
                                    WindowAgg.create(
                                        Filter.create(
                                            groupByOrAggregate(
                                                ProjectSet.create(
                                                    source,
                                                    splitPoints.tableFunctionsBelowGroupBy()
                                                ),
                                                relation.groupBy(),
                                                splitPoints.aggregates(),
                                                planStats
                                            ),
                                            having
                                        ),
                                        splitPoints.windowFunctions()
                                    ),
                                    splitPoints.tableFunctions()
                                ),
                                relation.isDistinct(),
                                relation.outputs()
                            ),
                            relation.orderBy()
                        ),
                        relation.limit(),
                        relation.offset()
                    ),
                    outputs
                )
            );
        }
    }

    private static LogicalPlan groupByOrAggregate(LogicalPlan source,
                                           List<Symbol> groupKeys,
                                           List<Function> aggregates,
                                           PlanStats planStats) {
        if (!groupKeys.isEmpty()) {
            return new GroupHashAggregate(source, groupKeys, aggregates);
        }
        if (!aggregates.isEmpty()) {
            return new HashAggregate(source, aggregates);
        }
        return source;
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
                                  DependencyCarrier executor,
                                  PlannerContext plannerContext,
                                  RowConsumer consumer,
                                  Row params,
                                  SubQueryResults subQueryResults,
                                  boolean enableProfiling) {
        NodeOperationTree nodeOpTree;
        try {
            nodeOpTree = getNodeOperationTree(logicalPlan, executor, plannerContext, params, subQueryResults);
        } catch (Throwable t) {
            consumer.accept(null, t);
            return;
        }
        executeNodeOpTree(
            executor,
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
        try {
            var executionPlan = logicalPlan.build(
                executor,
                plannerContext,
                Set.of(),
                executor.projectionBuilder(),
                -1,
                0,
                null,
                null,
                params,
                subQueryResults);
            return NodeOperationTreeGenerator.fromPlan(executionPlan, executor.localNodeId());
        } catch (ConversionException e) {
            throw e;
        } catch (Exception e) {
            if (e instanceof CrateException) {
                // Don't hide errors like MissingShardOperationsException, UnavailableShardsException
                throw e;
            }

            // This should really only happen if there are planner bugs,
            // so the additional costs of creating a more informative exception shouldn't matter.
            PrintContext printContext = new PrintContext(plannerContext.planStats());
            logicalPlan.print(printContext);
            IllegalArgumentException illegalArgumentException = new IllegalArgumentException(
                String.format(
                    Locale.ENGLISH,
                    "Couldn't create execution plan from logical plan because of: %s:%n%s",
                    e.getMessage(),
                    printContext
                ),
                e
            );
            illegalArgumentException.setStackTrace(e.getStackTrace());
            throw illegalArgumentException;
        }
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
            SubqueryPlanner subqueryPlanner = new SubqueryPlanner(s -> planSubSelect(s, context));
            LogicalPlan logicalPlan = plan(relation, context, subqueryPlanner, false);
            return new RootRelationBoundary(logicalPlan);
        }

        @Override
        protected LogicalPlan visitAnalyzedInsertStatement(AnalyzedInsertStatement statement, PlannerContext context) {
            SubqueryPlanner subqueryPlanner = new SubqueryPlanner(s -> planSubSelect(s, context));
            return writeOptimizer.optimize(
                InsertFromSubQueryPlanner.plan(
                    statement,
                    context,
                    LogicalPlanner.this,
                    subqueryPlanner),
                context.planStats(),
                context.transactionContext()
            );
        }
    }
}
