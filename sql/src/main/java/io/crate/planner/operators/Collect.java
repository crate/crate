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

import com.google.common.collect.Sets;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedTableRelation;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.TableFunctionRelation;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolVisitor;
import io.crate.analyze.symbol.Symbols;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.Reference;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.predicate.MatchPredicate;
import io.crate.operation.projectors.TopN;
import io.crate.planner.ExplainLeaf;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.node.dql.TableFunctionCollectPhase;
import io.crate.planner.projection.builder.ProjectionBuilder;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.crate.planner.operators.Limit.limitAndOffset;
import static io.crate.planner.operators.LogicalPlanner.extractColumns;

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
 *  See also {@link LogicalPlan.Builder#build(Set)}
 */
class Collect implements LogicalPlan {

    private static final String COLLECT_PHASE_NAME = "collect";
    final QueriedTableRelation relation;
    final WhereClause where;

    final List<Symbol> toCollect;
    final TableInfo tableInfo;
    private final List<AbstractTableRelation> baseTables;

    Collect(QueriedTableRelation relation, List<Symbol> toCollect, WhereClause where, Set<Symbol> usedBeforeNextFetch) {
        if (where.hasVersions()) {
            throw new VersionInvalidException();
        }
        this.relation = relation;
        this.where = where;
        this.baseTables = Collections.singletonList(relation.tableRelation());
        AbstractTableRelation tableRelation = relation.tableRelation();
        this.tableInfo = relation.tableRelation().tableInfo();
        if (tableRelation instanceof DocTableRelation) {
            Set<Symbol> colsToCollect = extractColumns(toCollect);
            Sets.SetView<Symbol> unusedCols = Sets.difference(colsToCollect, usedBeforeNextFetch);
            this.toCollect = generateToCollectWithFetch(tableInfo.ident(), toCollect, unusedCols, usedBeforeNextFetch);
        } else {
            this.toCollect = toCollect;
            if (where.hasQuery()) {
                NoPredicateVisitor.ensureNoMatchPredicate(where.query());
            }
        }
    }

    private static List<Symbol> generateToCollectWithFetch(TableIdent tableIdent,
                                                           List<Symbol> toCollect,
                                                           Collection<Symbol> unusedCols,
                                                           Set<Symbol> usedColumns) {
        ArrayList<Symbol> fetchable = new ArrayList<>();
        Symbol scoreCol = null;
        for (Symbol unusedCol : unusedCols) {
            if (Symbols.containsColumn(unusedCol, DocSysColumns.SCORE)) {
                scoreCol = unusedCol;
            } else {
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
    public Plan build(Planner.Context plannerContext,
                      ProjectionBuilder projectionBuilder,
                      int limit,
                      int offset,
                      @Nullable OrderBy order,
                      @Nullable Integer pageSizeHint) {
        RoutedCollectPhase collectPhase = createPhase(plannerContext);
        relation.tableRelation().validateOrderBy(order);
        collectPhase.orderBy(order);
        int limitAndOffset = limitAndOffset(limit, offset);
        maybeApplyPageSize(limitAndOffset, pageSizeHint, collectPhase);
        return new io.crate.planner.node.dql.Collect(
            collectPhase,
            TopN.NO_LIMIT,
            0,
            toCollect.size(),
            limitAndOffset,
            PositionalOrderBy.of(order, toCollect)
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

    private RoutedCollectPhase createPhase(Planner.Context plannerContext) {
        SessionContext sessionContext = plannerContext.transactionContext().sessionContext();
        if (relation.tableRelation() instanceof TableFunctionRelation) {
            TableFunctionRelation tableFunctionRelation = (TableFunctionRelation) relation.tableRelation();
            List<Symbol> args = tableFunctionRelation.function().arguments();
            ArrayList<Literal<?>> functionArguments = new ArrayList<>(args.size());
            for (Symbol arg : args) {
                // It's not possible to use columns as argument to a table function and subqueries are currently not allowed either.
                functionArguments.add((Literal) plannerContext.normalizer().normalize(arg, plannerContext.transactionContext()));
            }
            return new TableFunctionCollectPhase(
                plannerContext.jobId(),
                plannerContext.nextExecutionPhaseId(),
                plannerContext.allocateRouting(tableInfo, where, null, sessionContext),
                tableFunctionRelation.functionImplementation(),
                functionArguments,
                Collections.emptyList(),
                toCollect,
                where
            );
        }
        return new RoutedCollectPhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            COLLECT_PHASE_NAME,
            plannerContext.allocateRouting(
                tableInfo,
                where,
                null,
                sessionContext),
            tableInfo.rowGranularity(),
            toCollect,
            Collections.emptyList(),
            where,
            DistributionInfo.DEFAULT_BROADCAST,
            sessionContext.user()
        );
    }

    @Override
    public LogicalPlan tryCollapse() {
        return this;
    }

    @Override
    public List<Symbol> outputs() {
        return toCollect;
    }

    @Override
    public boolean preferShardProjections() {
        // Can't run on shard level for system tables
        // (Except tables like sys.shards, but in that case it's better to run operations per node as well,
        // because otherwise we'd use 1 thread per row which is unnecessary overhead and may use up all available threads)
        return tableInfo instanceof DocTableInfo;
    }

    public Map<Symbol, Symbol> expressionMapping() {
        return Collections.emptyMap();
    }


    @Override
    public List<AbstractTableRelation> baseTables() {
        return baseTables;
    }

    @Override
    public String toString() {
        return "Collect{" +
               tableInfo.ident() +
               ", [" + ExplainLeaf.printList(toCollect) +
               "], " + where +
               '}';
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
