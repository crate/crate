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
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.QueriedTableRelation;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.FieldsVisitor;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.RefVisitor;
import io.crate.analyze.symbol.SelectSymbol;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.Functions;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.SelectStatementPlanner;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.TableStats;
import io.crate.planner.consumer.FetchMode;
import io.crate.planner.consumer.InsertFromSubQueryPlanner;
import io.crate.planner.consumer.OptimizingRewriter;
import io.crate.planner.projection.builder.SplitPoints;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static io.crate.analyze.symbol.SelectSymbol.ResultType.SINGLE_COLUMN_SINGLE_VALUE;

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
        return plan(
            selectSymbol.relation(),
            PlannerContext.forSubPlan(plannerContext, softLimit, fetchSize)
        ).tryCollapse();
    }

    public LogicalPlan plan(QueriedRelation queriedRelation,
                            PlannerContext plannerContext,
                            SubqueryPlanner subqueryPlanner,
                            FetchMode fetchMode) {
        QueriedRelation relation = optimizingRewriter.optimize(queriedRelation, plannerContext.transactionContext());

        LogicalPlan logicalPlan = plan(relation, fetchMode, subqueryPlanner, true)
            .build(tableStats, new HashSet<>(relation.outputs()))
            .tryCollapse();

        return MultiPhase.createIfNeeded(logicalPlan, relation, subqueryPlanner);
    }

    static LogicalPlan.Builder plan(QueriedRelation relation,
                                    FetchMode fetchMode,
                                    SubqueryPlanner subqueryPlanner,
                                    boolean isLastFetch) {
        SplitPoints splitPoints = SplitPoints.create(relation);
        LogicalPlan.Builder sourceBuilder =
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
                relation.querySpec().outputs(),
                fetchMode,
                isLastFetch,
                relation.limit() != null
            );
        if (isLastFetch) {
            return sourceBuilder;
        }
        return RelationBoundary.create(sourceBuilder, relation, subqueryPlanner);
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
            return Join.createNodes(((MultiSourceSelect) queriedRelation), where, subqueryPlanner);
        }
        if (queriedRelation instanceof QueriedSelectRelation) {
            QueriedSelectRelation selectRelation = (QueriedSelectRelation) queriedRelation;
            return Filter.create(
                plan(selectRelation.subRelation(), fetchMode, subqueryPlanner,false),
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
