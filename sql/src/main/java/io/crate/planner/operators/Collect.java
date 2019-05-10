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
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.exceptions.VersioninigValidationException;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.pipeline.TopN;
import io.crate.expression.predicate.MatchPredicate;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.metadata.DocReferences;
import io.crate.metadata.Reference;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.ExplainLeaf;
import io.crate.planner.PlannerContext;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.consumer.OrderByPositionVisitor;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.selectivity.SelectivityFunctions;
import io.crate.statistics.Stats;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.crate.planner.operators.Limit.limitAndOffset;

/**
 * An Operator for data-collection.
 * Data collection can occur on base-tables (either lucene-table, sys-table, or table-function).
 *
 * This operator can also eagerly apply ORDER BY and will utilize limit, offset and pageSizeHint
 * to avoid collecting too much data.
 *
 */
public class Collect implements LogicalPlan {

    private static final String COLLECT_PHASE_NAME = "collect";
    final AbstractTableRelation relation;
    private final boolean preferSourceLookup;
    private final List<Symbol> outputs;
    private final List<AbstractTableRelation> baseTables;
    final TableInfo tableInfo;
    private final long numExpectedRows;
    private final long estimatedRowSize;

    WhereClause where;

    public static LogicalPlan.Builder create(AbstractTableRelation<?> relation,
                                             List<Symbol> toCollect,
                                             WhereClause where) {
        return (tableStats, hints, params) -> {
            Stats stats = tableStats.getStats(relation.tableInfo().ident());
            return new Collect(
                hints.contains(PlanHint.PREFER_SOURCE_LOOKUP),
                relation,
                toCollect,
                where,
                SelectivityFunctions.estimateNumRows(stats, where.queryOrFallback(), params),
                stats.averageSizePerRowInBytes()
            );
        };
    }

    public Collect(boolean preferSourceLookup,
                   AbstractTableRelation<?> relation,
                   List<Symbol> outputs,
                   WhereClause where,
                   long numExpectedRows,
                   long estimatedRowSize) {
        this.preferSourceLookup = preferSourceLookup;
        this.outputs = outputs;
        this.baseTables = List.of(relation);
        this.numExpectedRows = numExpectedRows;
        this.estimatedRowSize = estimatedRowSize;
        if (where.hasQuery() && !(relation instanceof DocTableRelation)) {
            NoPredicateVisitor.ensureNoMatchPredicate(where.query());
        }
        if (where.hasVersions()) {
            throw VersioninigValidationException.versionInvalidUsage();
        } else if (where.hasSeqNoAndPrimaryTerm()) {
            throw VersioninigValidationException.seqNoAndPrimaryTermUsage();
        }
        this.relation = relation;
        this.where = where;
        this.tableInfo = relation.tableInfo();
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
        RoutedCollectPhase collectPhase = createPhase(plannerContext, params, subQueryResults);
        PositionalOrderBy positionalOrderBy = getPositionalOrderBy(order, outputs);
        if (positionalOrderBy != null) {
            collectPhase.orderBy(
                order
                    .map(s -> SubQueryAndParamBinder.convert(s, params, subQueryResults))
                    // Filter out literal constants as ordering by constants is a NO-OP and also not supported
                    // on the collect operation.
                    .exclude(s -> s instanceof Literal));
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

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    public WhereClause where() {
        return where;
    }

    public AbstractTableRelation relation() {
        return relation;
    }

    private static boolean noLuceneSortSupport(OrderBy order) {
        for (Symbol sortKey : order.orderBySymbols()) {
            if (SymbolVisitors.any(Collect::isPartitionColOrAnalyzed, sortKey)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isPartitionColOrAnalyzed(Symbol s) {
        // 1) partition columns are normalized on shard to literal, but lucene sort doesn't support literals
        // 2) no docValues or field data for analyzed columns -> can't sort on lucene level
        return s instanceof Reference &&
               (((Reference) s).granularity() == RowGranularity.PARTITION
                || ((Reference) s).indexType() == Reference.IndexType.ANALYZED);
    }

    @Nullable
    private static PositionalOrderBy getPositionalOrderBy(@Nullable OrderBy order, List<Symbol> outputs) {
        if (order == null) {
            return null;
        }
        // The order by symbols may contain additional scalars that are not in the outputs. If that is the
        // case it is not possible to put the OrderBy on the CollectPhase, because we cannot create the PositionalOrderBy.
        // PositionalOrderBy does not support scalar evaluation. We don't set the orderBy here - which causes the Order operator
        // to add itself as projection.
        // TODO: Could we support this by extending the outputs of the collectPhase?
        int[] positions = OrderByPositionVisitor.orderByPositionsOrNull(order.orderBySymbols(), outputs);
        if (positions == null) {
            return null;
        } else if (noLuceneSortSupport(order)) {
            // force use of separate order projection
            return null;
        } else {
            return new PositionalOrderBy(positions, order.reverseFlags(), order.nullsFirst());
        }
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

    private RoutedCollectPhase createPhase(PlannerContext plannerContext, Row params, SubQueryResults subQueryResults) {
        SessionContext sessionContext = plannerContext.transactionContext().sessionContext();
        SubQueryAndParamBinder binder = new SubQueryAndParamBinder(params, subQueryResults);

        // bind all parameters and possible subQuery values and re-analyze the query
        // (could result in a NO_MATCH, routing could've changed, etc).
        // the <p>where</p> instance variable must be overwritten as the plan creation of outer operators relies on it
        // (e.g. GroupHashAggregate will build different plans based on the collect routing)
        where = WhereClauseAnalyzer.resolvePartitions(
            where.map(binder),
            relation,
            plannerContext.functions(),
            plannerContext.transactionContext());
        List<Symbol> boundOutputs = Lists2.map(outputs, binder);
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
            preferSourceLookup && tableInfo instanceof DocTableInfo
                ? Lists2.map(boundOutputs, DocReferences::toSourceLookup)
                : boundOutputs,
            Collections.emptyList(),
            where.queryOrFallback(),
            DistributionInfo.DEFAULT_BROADCAST
        );
    }

    @Override
    public boolean preferShardProjections() {
        // Can't run on shard level for system tables
        // (Except tables like sys.shards, but in that case it's better to run operations per node as well,
        // because otherwise we'd use 1 thread per row which is unnecessary overhead and may use up all available threads)
        return tableInfo instanceof DocTableInfo;
    }

    @Override
    public List<AbstractTableRelation> baseTables() {
        return baseTables;
    }

    @Override
    public List<LogicalPlan> sources() {
        return List.of();
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        assert sources.isEmpty() : "Collect has no sources, cannot replace them";
        return this;
    }

    @Override
    public Map<LogicalPlan, SelectSymbol> dependencies() {
        return Map.of();
    }

    public long numExpectedRows() {
        return numExpectedRows;
    }

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

    public boolean preferSourceLookup() {
        return preferSourceLookup;
    }

    private static final class NoPredicateVisitor extends SymbolVisitor<Void, Void> {

        private static final NoPredicateVisitor NO_PREDICATE_VISITOR = new NoPredicateVisitor();

        private NoPredicateVisitor() {
        }

        static void ensureNoMatchPredicate(Symbol symbolTree) {
            symbolTree.accept(NO_PREDICATE_VISITOR, null);
        }

        @Override
        public Void visitFunction(Function symbol, Void context) {
            if (symbol.info().ident().name().equals(MatchPredicate.NAME)) {
                throw new UnsupportedFeatureException("Cannot use match predicate on system tables");
            }
            for (Symbol argument : symbol.arguments()) {
                argument.accept(this, context);
            }
            return null;
        }
    }
}
