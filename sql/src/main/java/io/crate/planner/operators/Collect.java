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

import io.crate.action.sql.SessionContext;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedTableRelation;
import io.crate.analyze.SymbolEvaluator;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.relations.TableFunctionRelation;
import io.crate.analyze.where.DocKeys;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.data.Row;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.exceptions.VersionInvalidException;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.phases.TableFunctionCollectPhase;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.pipeline.TopN;
import io.crate.expression.predicate.MatchPredicate;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.Reference;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.ExplainLeaf;
import io.crate.planner.PlannerContext;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.TableStats;
import io.crate.planner.consumer.OrderByPositionVisitor;
import io.crate.planner.distribution.DistributionInfo;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.crate.planner.operators.Limit.limitAndOffset;
import static io.crate.planner.operators.OperatorUtils.getUnusedColumns;

/**
 * An Operator for data-collection.
 * Data collection can occur on base-tables (either lucene-table, sys-table, or table-function).
 *
 * This operator can also eagerly apply ORDER BY and will utilize limit, offset and pageSizeHint
 * to avoid collecting too much data.
 *
 * Note:
 *  In case of lucene-tables this may not actually produce {@code toCollect} as output.
 *  Instead it may choose to output {@link DocSysColumns#FETCHID} + {@code usedColumns}.
 *
 *  {@link FetchOrEval} will then later use {@code fetchId} to fetch the values for the columns which are "unused".
 *  See also {@link LogicalPlan.Builder#build(TableStats, Set)}
 */
class Collect extends ZeroInputPlan {

    private static final String COLLECT_PHASE_NAME = "collect";
    final QueriedTableRelation relation;
    WhereClause where;

    final TableInfo tableInfo;
    private final long numExpectedRows;
    private final long estimatedRowSize;

    public static LogicalPlan.Builder create(QueriedTableRelation relation,
                                             List<Symbol> toCollect,
                                             WhereClause where) {
        if (where.docKeys().isPresent() && !((DocTableInfo) relation.tableRelation().tableInfo()).isAlias()) {
            DocKeys docKeys = where.docKeys().get();
            return ((tableStats, usedBeforeNextFetch) ->
                        new Get(((QueriedDocTable) relation), docKeys, toCollect, tableStats));
        }
        return (tableStats, usedColumns) -> new Collect(
            relation,
            toCollect,
            where,
            usedColumns,
            tableStats.numDocs(relation.tableRelation().tableInfo().ident()),
            tableStats.estimatedSizePerRow(relation.tableRelation().tableInfo().ident()));
    }

    private Collect(QueriedTableRelation relation,
                    List<Symbol> toCollect,
                    WhereClause where,
                    Set<Symbol> usedBeforeNextFetch,
                    long numExpectedRows,
                    long estimatedRowSize) {
        super(
            generateOutputs(toCollect, relation.tableRelation(), usedBeforeNextFetch, where),
            Collections.singletonList(relation.tableRelation()));

        this.numExpectedRows = numExpectedRows;
        this.estimatedRowSize = estimatedRowSize;
        if (where.hasVersions()) {
            throw new VersionInvalidException();
        }
        this.relation = relation;
        this.where = where;
        this.tableInfo = relation.tableRelation().tableInfo();
    }

    private static List<Symbol> generateOutputs(List<Symbol> toCollect,
                                                AbstractTableRelation tableRelation,
                                                Set<Symbol> usedBeforeNextFetch,
                                                WhereClause where) {
        if (tableRelation instanceof DocTableRelation) {
            return generateToCollectWithFetch(((DocTableRelation) tableRelation).tableInfo().ident(), toCollect, usedBeforeNextFetch);
        } else {
            if (where.hasQuery()) {
                NoPredicateVisitor.ensureNoMatchPredicate(where.query());
            }
            return toCollect;
        }
    }

    private static List<Symbol> generateToCollectWithFetch(TableIdent tableIdent,
                                                           List<Symbol> toCollect,
                                                           Set<Symbol> usedColumns) {

        List<Symbol> unusedCols = getUnusedColumns(toCollect, usedColumns);

        ArrayList<Symbol> fetchable = new ArrayList<>();
        Symbol scoreCol = null;
        for (Symbol unusedCol : unusedCols) {
            // _score cannot be fetched because it's a relative value only available during the query phase
            if (Symbols.containsColumn(unusedCol, DocSysColumns.SCORE)) {
                scoreCol = unusedCol;

            // literals or functions like random() shouldn't be tracked as fetchable
            } else if (SymbolVisitors.any(Symbols.IS_COLUMN, unusedCol)) {
                fetchable.add(unusedCol);
            }
        }
        if (fetchable.isEmpty()) {
            return toCollect;
        }
        Reference fetchIdRef = DocSysColumns.forTable(tableIdent, DocSysColumns.FETCHID);
        ArrayList<Symbol> preFetchSymbols = new ArrayList<>(usedColumns.size() + 1);
        preFetchSymbols.add(fetchIdRef);
        preFetchSymbols.addAll(usedColumns);
        if (scoreCol != null) {
            preFetchSymbols.add(scoreCol);
        }
        return preFetchSymbols;
    }

    @Override
    public ExecutionPlan build(PlannerContext plannerContext,
                               ProjectionBuilder projectionBuilder,
                               int limit,
                               int offset,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               Map<SelectSymbol, Object> subQueryValues) {
        RoutedCollectPhase collectPhase = createPhase(plannerContext, params, subQueryValues);
        relation.tableRelation().validateOrderBy(order);

        // The order by symbols may contain additional scalars that are not in the outputs. If that is the
        // case it is not possible to put the OrderBy on the CollectPhase, because we cannot create the PositionalOrderBy.
        // PositionalOrderBy does not support scalar evaluation. We don't set the orderBy here - which causes the Order operator
        // to add itself as projection.
        // TODO: Could we support this by extending the outputs of the collectPhase?
        final PositionalOrderBy positionalOrderBy;
        if (order == null) {
            positionalOrderBy = null;
        } else {
            int[] positions = OrderByPositionVisitor.orderByPositionsOrNull(order.orderBySymbols(), outputs);
            if (positions == null) {
                positionalOrderBy = null;
            } else {
                collectPhase.orderBy(order);
                positionalOrderBy = new PositionalOrderBy(positions, order.reverseFlags(), order.nullsFirst());
            }
        }
        int limitAndOffset = limitAndOffset(limit, offset);
        maybeApplyPageSize(limitAndOffset, pageSizeHint, collectPhase);
        return new io.crate.planner.node.dql.Collect(
            collectPhase,
            TopN.NO_LIMIT,
            0,
            outputs.size(),
            limitAndOffset,
            positionalOrderBy
        );
    }

    private static void maybeApplyPageSize(int limit, @Nullable Integer pageSizeHint, RoutedCollectPhase collectPhase) {
        if (pageSizeHint == null) {
            if (limit > TopN.NO_LIMIT) {
                collectPhase.nodePageSizeHint(limit);
            }
        } else {
            collectPhase.pageSizeHint(pageSizeHint);
        }
    }

    private RoutedCollectPhase createPhase(PlannerContext plannerContext, Row params, Map<SelectSymbol, Object> subQueryValues) {
        SessionContext sessionContext = plannerContext.transactionContext().sessionContext();
        if (relation.tableRelation() instanceof TableFunctionRelation) {
            TableFunctionRelation tableFunctionRelation = (TableFunctionRelation) relation.tableRelation();
            List<Symbol> args = tableFunctionRelation.function().arguments();
            ArrayList<Literal<?>> functionArguments = new ArrayList<>(args.size());
            for (Symbol arg : args) {
                // It's not possible to use columns as argument to a table function, so it's safe to evaluate at this point.
                functionArguments.add(
                    Literal.of(
                        arg.valueType(),
                        SymbolEvaluator.evaluate(plannerContext.functions(), arg, params, subQueryValues)
                    )
                );
            }
            return new TableFunctionCollectPhase(
                plannerContext.jobId(),
                plannerContext.nextExecutionPhaseId(),
                plannerContext.allocateRouting(tableInfo, where, RoutingProvider.ShardSelection.ANY, sessionContext),
                tableFunctionRelation.functionImplementation(),
                functionArguments,
                Collections.emptyList(),
                outputs,
                where
            );
        }

        // bind all parameters and possible subQuery values and re-analyze the query
        // (could result in a NO_MATCH, routing could've changed, etc).
        // the <p>where</p> instance variable must be overwritten as the plan creation of outer operators relies on it
        // (e.g. GroupHashAggregate will build different plans based on the collect routing)
        where = WhereClauseAnalyzer.bindAndAnalyze(
            where,
            params,
            subQueryValues,
            relation.tableRelation(),
            plannerContext.functions(),
            plannerContext.transactionContext());
        List<Symbol> boundOutputs = new ArrayList<>(outputs.size());
        for (Symbol output : outputs) {
            boundOutputs.add(SubQueryAndParamBinder.convert(output, params, subQueryValues));
        }

        return new RoutedCollectPhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            COLLECT_PHASE_NAME,
            plannerContext.allocateRouting(
                tableInfo,
                where,
                RoutingProvider.ShardSelection.ANY,
                sessionContext),
            tableInfo.rowGranularity(),
            boundOutputs,
            Collections.emptyList(),
            where,
            DistributionInfo.DEFAULT_BROADCAST,
            sessionContext.user()
        );
    }

    @Override
    public LogicalPlan tryOptimize(@Nullable LogicalPlan pushDown, SymbolMapper mapper) {
        if (pushDown instanceof Order) {
            return ((Order) pushDown).updateSource(this, mapper);
        }
        return super.tryOptimize(pushDown, mapper);
    }

    @Override
    public boolean preferShardProjections() {
        // Can't run on shard level for system tables
        // (Except tables like sys.shards, but in that case it's better to run operations per node as well,
        // because otherwise we'd use 1 thread per row which is unnecessary overhead and may use up all available threads)
        return tableInfo instanceof DocTableInfo;
    }

    @Override
    public long numExpectedRows() {
        return numExpectedRows;
    }

    @Override
    public long estimatedRowSize() {
        return estimatedRowSize;
    }

    @Override
    public String toString() {
        return "Collect{" +
               tableInfo.ident() +
               ", [" + ExplainLeaf.printList(outputs) +
               "], " + where +
               '}';
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitCollect(this, context);
    }

    private static final  class NoPredicateVisitor extends SymbolVisitor<Void, Void> {

        private static final NoPredicateVisitor NO_PREDICATE_VISITOR = new NoPredicateVisitor();

        private NoPredicateVisitor() {
        }

        static void ensureNoMatchPredicate(Symbol symbolTree) {
            NO_PREDICATE_VISITOR.process(symbolTree, null);
        }

        @Override
        public Void visitFunction(Function symbol, Void context) {
            if (symbol.info().ident().name().equals(MatchPredicate.NAME)) {
                throw new UnsupportedFeatureException("Cannot use match predicate on system tables");
            }
            for (Symbol argument : symbol.arguments()) {
                process(argument, context);
            }
            return null;
        }
    }
}
