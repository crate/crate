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
import io.crate.exceptions.VersioninigValidationException;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.pipeline.TopN;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.FetchMarker;
import io.crate.expression.symbol.FetchStub;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.RefReplacer;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.expression.symbol.Symbols;
import io.crate.planner.optimizer.symbol.Optimizer;
import io.crate.metadata.DocReferences;
import io.crate.metadata.Reference;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.consumer.OrderByPositionVisitor;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.selectivity.SelectivityFunctions;
import io.crate.statistics.Stats;
import io.crate.statistics.TableStats;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    final AbstractTableRelation<?> relation;
    private final boolean preferSourceLookup;
    private final List<Symbol> outputs;
    private final List<AbstractTableRelation<?>> baseTables;
    final TableInfo tableInfo;
    private final long numExpectedRows;
    private final long estimatedRowSize;

    WhereClause where;

    public static Collect create(AbstractTableRelation<?> relation,
                                 List<Symbol> toCollect,
                                 WhereClause where,
                                 Set<PlanHint> hints,
                                 TableStats tableStats,
                                 Row params) {
        Stats stats = tableStats.getStats(relation.tableInfo().ident());
        return new Collect(
            hints.contains(PlanHint.PREFER_SOURCE_LOOKUP),
            relation,
            toCollect,
            where,
            SelectivityFunctions.estimateNumRows(stats, where.queryOrFallback(), params),
            stats.estimateSizeForColumns(toCollect)
        );
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
            EnsureNoMatchPredicate.ensureNoMatchPredicate(where.queryOrFallback(), "Cannot use MATCH on system tables");
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
        EvaluatingNormalizer normalizer = new EvaluatingNormalizer(
            plannerContext.nodeContext(),
            RowGranularity.CLUSTER,
            null,
            relation
        );
        var binder = new SubQueryAndParamBinder(params, subQueryResults)
            .andThen(x -> normalizer.normalize(x, plannerContext.transactionContext()));
        RoutedCollectPhase collectPhase = createPhase(plannerContext, binder);
        PositionalOrderBy positionalOrderBy = getPositionalOrderBy(order, outputs);
        if (positionalOrderBy != null) {
            collectPhase.orderBy(
                order.map(binder)
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

    public AbstractTableRelation<?> relation() {
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

    private RoutedCollectPhase createPhase(PlannerContext plannerContext, java.util.function.Function<Symbol, Symbol> binder) {
        SessionContext sessionContext = plannerContext.transactionContext().sessionContext();

        // bind all parameters and possible subQuery values and re-analyze the query
        // (could result in a NO_MATCH, routing could've changed, etc).
        // the <p>where</p> instance variable must be overwritten as the plan creation of outer operators relies on it
        // (e.g. GroupHashAggregate will build different plans based on the collect routing)
        where = WhereClauseAnalyzer.resolvePartitions(
            where.map(binder),
            relation,
            plannerContext.transactionContext(),
            plannerContext.nodeContext());
        if (where.hasVersions()) {
            throw VersioninigValidationException.versionInvalidUsage();
        } else if (where.hasSeqNoAndPrimaryTerm()) {
            throw VersioninigValidationException.seqNoAndPrimaryTermUsage();
        }

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
            Optimizer.optimizeCasts(where.queryOrFallback(), plannerContext),
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
    public List<AbstractTableRelation<?>> baseTables() {
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
    public LogicalPlan pruneOutputsExcept(TableStats tableStats, Collection<Symbol> outputsToKeep) {
        ArrayList<Symbol> newOutputs = new ArrayList<>();
        for (Symbol output : outputs) {
            if (outputsToKeep.contains(output)) {
                newOutputs.add(output);
            }
        }
        if (newOutputs.equals(outputs)) {
            return this;
        }
        Stats stats = tableStats.getStats(relation.relationName());
        return new Collect(
            preferSourceLookup,
            relation,
            newOutputs,
            where,
            numExpectedRows,
            stats.estimateSizeForColumns(newOutputs)
        );
    }

    @Nullable
    @Override
    public FetchRewrite rewriteToFetch(TableStats tableStats, Collection<Symbol> usedColumns) {
        if (!(tableInfo instanceof DocTableInfo)) {
            return null;
        }
        ArrayList<Symbol> newOutputs = new ArrayList<>();
        LinkedHashMap<Symbol, Symbol> replacedOutputs = new LinkedHashMap<>();
        ArrayList<Reference> refsToFetch = new ArrayList<>();
        FetchMarker fetchMarker = new FetchMarker(relation.relationName(), refsToFetch);
        for (int i = 0; i < outputs.size(); i++) {
            Symbol output = outputs.get(i);
            if (Symbols.containsColumn(output, DocSysColumns.SCORE)) {
                newOutputs.add(output);
                replacedOutputs.put(output, output);
            } else if (!SymbolVisitors.any(Symbols.IS_COLUMN, output)) {
                newOutputs.add(output);
                replacedOutputs.put(output, output);
            } else if (SymbolVisitors.any(usedColumns::contains, output)) {
                newOutputs.add(output);
                replacedOutputs.put(output, output);
            } else {
                Symbol outputWithFetchStub = RefReplacer.replaceRefs(output, ref -> {
                    Reference sourceLookup = DocReferences.toSourceLookup(ref);
                    refsToFetch.add(sourceLookup);
                    return new FetchStub(fetchMarker, sourceLookup);
                });
                replacedOutputs.put(output, outputWithFetchStub);
            }
        }
        if (newOutputs.size() == outputs.size()) {
            return null;
        }
        newOutputs.add(0, fetchMarker);
        Stats stats = tableStats.getStats(relation.relationName());
        return new FetchRewrite(
            replacedOutputs,
            new Collect(
                preferSourceLookup,
                relation,
                newOutputs,
                where,
                numExpectedRows,
                stats.estimateSizeForColumns(newOutputs)
            )
        );
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
               ", [" + Lists2.joinOn(", ", outputs, Symbol::toString) +
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

    @Override
    public void print(PrintContext printContext) {
        printContext
            .text("Collect[")
            .text(tableInfo.ident().toString())
            .text(" | [")
            .text(Lists2.joinOn(", ", outputs, Symbol::toString))
            .text("] | ")
            .text(where.queryOrFallback().toString())
            .text("]");
    }
}
