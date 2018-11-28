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
import io.crate.analyze.QueriedTable;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedView;
import io.crate.analyze.relations.OrderedLimitedRelation;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.relations.UnionSelect;
import io.crate.analyze.where.DocKeys;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.MultiPhaseExecutor;
import io.crate.execution.dsl.phases.NodeOperationTree;
import io.crate.execution.dsl.projection.builder.SplitPoints;
import io.crate.execution.dsl.projection.builder.SplitPointsBuilder;
import io.crate.execution.engine.NodeOperationTreeGenerator;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.FieldsVisitor;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.RefVisitor;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Functions;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.SelectStatementPlanner;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.TableStats;
import io.crate.planner.WhereClauseOptimizer;
import io.crate.planner.consumer.FetchMode;
import io.crate.planner.consumer.InsertFromSubQueryPlanner;
import io.crate.planner.consumer.OptimizingRewriter;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

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
    private final Functions functions;

    public LogicalPlanner(Functions functions, TableStats tableStats) {
        this.optimizingRewriter = new OptimizingRewriter(functions);
        this.tableStats = tableStats;
        this.functions = functions;
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
            true,
            functions,
            plannerContext.transactionContext());

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
        TransactionContext transactionContext = plannerContext.transactionContext();
        QueriedRelation relation = optimizingRewriter.optimize(queriedRelation, transactionContext);

        LogicalPlan logicalPlan = plan(relation, fetchMode, subqueryPlanner, true, functions, transactionContext)
            .build(tableStats, new HashSet<>(relation.outputs()));

        LogicalPlan optimizedPlan = tryOptimize(logicalPlan);

        return MultiPhase.createIfNeeded(optimizedPlan, relation, subqueryPlanner);
    }

    /**
     * Runs {@link LogicalPlan}.tryOptimize and returns an optimized plan.
     * @param plan The original plan
     * @return The optimized plan or the original if optimizing is not possible
     */
    private static LogicalPlan tryOptimize(LogicalPlan plan) {
        LogicalPlan optimizedPlan = plan.tryOptimize(null, SymbolMapper.identity());
        if (optimizedPlan == null) {
            return plan;
        }
        return optimizedPlan;
    }

    static LogicalPlan.Builder plan(QueriedRelation relation,
                                    FetchMode fetchMode,
                                    SubqueryPlanner subqueryPlanner,
                                    boolean isLastFetch,
                                    Functions functions,
                                    TransactionContext txnCtx) {
        LogicalPlan.Builder builder = prePlan(relation, fetchMode, subqueryPlanner, isLastFetch, functions, txnCtx);
        if (isLastFetch) {
            return builder;
        }
        return RelationBoundary.create(builder, relation, subqueryPlanner);
    }

    private static LogicalPlan.Builder prePlan(QueriedRelation relation,
                                               FetchMode fetchMode,
                                               SubqueryPlanner subqueryPlanner,
                                               boolean isLastFetch,
                                               Functions functions,
                                               TransactionContext txnCtx) {
        SplitPoints splitPoints = SplitPointsBuilder.create(relation);
        return FetchOrEval.create(
            Limit.create(
                Order.create(
                    ProjectSet.create(
                        WindowAgg.create(
                            Filter.create(
                                groupByOrAggregate(
                                    collectAndFilter(
                                        relation,
                                        splitPoints.toCollect(),
                                        relation.where(),
                                        subqueryPlanner,
                                        fetchMode,
                                        functions,
                                        txnCtx
                                    ),
                                    relation.groupBy(),
                                    splitPoints.aggregates()
                                ),
                                relation.having()
                            ),
                            splitPoints.windowFunctions()
                        ),
                        splitPoints.tableFunctions()
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
                                                        FetchMode fetchMode,
                                                        Functions functions,
                                                        TransactionContext txnCtx) {
        if (queriedRelation instanceof AnalyzedView) {
            return plan(((AnalyzedView) queriedRelation).relation(), fetchMode, subqueryPlanner, false, functions, txnCtx);
        }
        if (queriedRelation instanceof QueriedTable) {
            QueriedTable queriedTable = (QueriedTable) queriedRelation;
            TableInfo table = queriedTable.tableRelation().tableInfo();
            if (table instanceof DocTableInfo) {
                EvaluatingNormalizer normalizer = new EvaluatingNormalizer(
                    functions, RowGranularity.CLUSTER, null, queriedTable.tableRelation());

                WhereClauseOptimizer.DetailedQuery detailedQuery = WhereClauseOptimizer.optimize(
                    normalizer,
                    where.queryOrFallback(),
                    (DocTableInfo) table,
                    txnCtx
                );

                Optional<DocKeys> docKeys = detailedQuery.docKeys();
                if (docKeys.isPresent()) {
                    return (tableStats, usedBeforeNextFetch) ->
                        new Get(queriedTable, docKeys.get(), toCollect, tableStats);
                }
                return Collect.create(queriedTable, toCollect, new WhereClause(
                    detailedQuery.query(),
                    where.partitions(),
                    detailedQuery.clusteredBy()
                ));
            }
            return Collect.create(queriedTable, toCollect, where);
        }
        if (queriedRelation instanceof MultiSourceSelect) {
            return JoinPlanBuilder.createNodes((MultiSourceSelect) queriedRelation, where, subqueryPlanner, functions, txnCtx);
        }
        if (queriedRelation instanceof UnionSelect) {
            return Union.create((UnionSelect) queriedRelation, subqueryPlanner, functions, txnCtx);
        }
        if (queriedRelation instanceof OrderedLimitedRelation) {
            return plan(((OrderedLimitedRelation) queriedRelation).childRelation(), fetchMode, subqueryPlanner, false, functions, txnCtx);
        }
        if (queriedRelation instanceof QueriedSelectRelation) {
            QueriedSelectRelation selectRelation = (QueriedSelectRelation) queriedRelation;
            return Filter.create(
                plan(selectRelation.subRelation(), fetchMode, subqueryPlanner, false, functions, txnCtx),
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
        NodeOperationTree nodeOpTree = getNodeOperationTree(logicalPlan, executor, plannerContext, params, subQueryResults);
        executeNodeOpTree(executor, plannerContext.jobId(), consumer, enableProfiling, nodeOpTree);
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

    public static void executeNodeOpTree(DependencyCarrier executor, UUID jobId, RowConsumer consumer, boolean enableProfiling, NodeOperationTree nodeOpTree) {
        executor.phasesTaskFactory()
            .create(jobId, Collections.singletonList(nodeOpTree), enableProfiling)
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
