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

import static io.crate.planner.operators.Limit.limitAndOffset;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SequencedCollection;
import java.util.Set;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.GeneratedColumnExpander;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.exceptions.VersioningValidationException;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.pipeline.LimitAndOffset;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.AliasSymbol;
import io.crate.expression.symbol.FetchMarker;
import io.crate.expression.symbol.FetchStub;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.RefReplacer;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.DocReferences;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.WhereClauseOptimizer.DetailedQuery;
import io.crate.planner.consumer.OrderByPositionVisitor;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.optimizer.symbol.Optimizer;

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
    private final List<Symbol> outputs;
    private final List<AbstractTableRelation<?>> baseTables;
    final TableInfo tableInfo;
    final WhereClause immutableWhere;

    WhereClause mutableBoundWhere;
    DetailedQuery detailedQuery;

    public Collect(Collect collect, DetailedQuery detailedQuery) {
        assert detailedQuery.docKeys().isEmpty()
            : "`Collect` operator must not be used with queries that have docKeys. Use the `Get` operator instead";

        this.outputs = collect.outputs();
        this.baseTables = collect.baseTables;
        this.relation = collect.relation;
        this.mutableBoundWhere = collect.mutableBoundWhere;
        this.immutableWhere = collect.immutableWhere;
        this.tableInfo = collect.relation.tableInfo();
        this.detailedQuery = detailedQuery;
    }

    public Collect(AbstractTableRelation<?> relation,
                   List<Symbol> outputs,
                   WhereClause where) {
        this.outputs = outputs;
        this.baseTables = List.of(relation);
        if (where.hasQuery() && !(relation instanceof DocTableRelation)) {
            EnsureNoMatchPredicate.ensureNoMatchPredicate(where.queryOrFallback(), "Cannot use MATCH on system tables");
        }
        this.relation = relation;
        this.immutableWhere = where;
        this.mutableBoundWhere = where;
        this.tableInfo = relation.tableInfo();
    }

    @Override
    public ExecutionPlan build(DependencyCarrier executor,
                               PlannerContext plannerContext,
                               Set<PlanHint> hints,
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
        RoutedCollectPhase collectPhase = createPhase(plannerContext, hints, binder, params, subQueryResults);
        PositionalOrderBy positionalOrderBy = getPositionalOrderBy(order, outputs);
        if (positionalOrderBy != null) {
            if (hints.contains(PlanHint.PREFER_SOURCE_LOOKUP)) {
                order = order.map(DocReferences::toSourceLookup);
            }
            collectPhase.orderBy(
                order.map(binder)
                    // Filter out literal constants as ordering by constants is a NO-OP and also not supported
                    // on the collect operation.
                    .exclude(s -> s instanceof Literal ||
                                  s instanceof AliasSymbol alias && alias.symbol() instanceof Literal));
        }
        int limitAndOffset = limitAndOffset(limit, offset);
        maybeApplyPageSize(limitAndOffset, pageSizeHint, collectPhase);
        return new io.crate.planner.node.dql.Collect(
            collectPhase,
            LimitAndOffset.NO_LIMIT,
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
        return immutableWhere;
    }

    public DetailedQuery detailedQuery() {
        return detailedQuery;
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
        return s instanceof Reference ref &&
               (ref.granularity() == RowGranularity.PARTITION
                || ref.indexType() == IndexType.FULLTEXT);
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
            if (limit > LimitAndOffset.NO_LIMIT) {
                collectPhase.nodePageSizeHint(limit);
            }
        } else {
            collectPhase.pageSizeHint(pageSizeHint);
        }
    }

    private RoutedCollectPhase createPhase(PlannerContext plannerContext,
                                           Set<PlanHint> planHints,
                                           java.util.function.Function<Symbol, Symbol> binder,
                                           Row params,
                                           SubQueryResults subQueryResults) {
        WhereClause boundWhere;
        if (tableInfo instanceof DocTableInfo docTable) {
            if (detailedQuery == null) {
                boundWhere = immutableWhere.map(binder);
            } else {
                boundWhere = detailedQuery.toBoundWhereClause(
                    docTable,
                    params,
                    subQueryResults,
                    plannerContext.transactionContext(),
                    plannerContext.nodeContext()
                );
            }
            Symbol query = GeneratedColumnExpander.maybeExpand(
                boundWhere.queryOrFallback(),
                docTable.generatedColumns(),
                Lists2.concat(docTable.partitionedByColumns(), Lists2.map(docTable.primaryKey(), docTable::getReference)),
                plannerContext.nodeContext()
            );
            if (!query.equals(boundWhere.queryOrFallback())) {
                boundWhere = new WhereClause(query, boundWhere.partitions(), boundWhere.clusteredBy());
            }
        } else {
            boundWhere = immutableWhere.map(binder);
        }

        // bind all parameters and possible subQuery values and re-analyze the query
        // (could result in a NO_MATCH, routing could've changed, etc).
        // the <p>where</p> instance variable must be overwritten as the plan creation of outer operators relies on it
        // (e.g. GroupHashAggregate will build different plans based on the collect routing)
        mutableBoundWhere = WhereClauseAnalyzer.resolvePartitions(
            boundWhere,
            relation,
            plannerContext.transactionContext(),
            plannerContext.nodeContext());
        if (mutableBoundWhere.hasVersions()) {
            throw VersioningValidationException.versionInvalidUsage();
        } else if (mutableBoundWhere.hasSeqNoAndPrimaryTerm()) {
            throw VersioningValidationException.seqNoAndPrimaryTermUsage();
        }

        var sessionSettings = plannerContext.transactionContext().sessionSettings();
        List<Symbol> boundOutputs = Lists2.map(outputs, binder);
        return new RoutedCollectPhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            COLLECT_PHASE_NAME,
            plannerContext.allocateRouting(
                tableInfo,
                mutableBoundWhere,
                RoutingProvider.ShardSelection.ANY,
                sessionSettings),
            tableInfo.rowGranularity(),
            planHints.contains(PlanHint.PREFER_SOURCE_LOOKUP) && tableInfo instanceof DocTableInfo
                ? Lists2.map(boundOutputs, DocReferences::toSourceLookup)
                : boundOutputs,
            Collections.emptyList(),
            Optimizer.optimizeCasts(mutableBoundWhere.queryOrFallback(), plannerContext),
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
    public List<RelationName> getRelationNames() {
        return List.of(relation.relationName());
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
    public LogicalPlan pruneOutputsExcept(SequencedCollection<Symbol> outputsToKeep) {
        ArrayList<Symbol> newOutputs = new ArrayList<>();
        for (Symbol output : outputs) {
            if (outputsToKeep.contains(output)) {
                newOutputs.add(output);
            }
        }
        if (newOutputs.equals(outputs)) {
            return this;
        }
        return new Collect(relation, newOutputs, immutableWhere);
    }

    @Nullable
    @Override
    public FetchRewrite rewriteToFetch(Collection<Symbol> usedColumns) {
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
            } else if (SymbolVisitors.any(output::equals, usedColumns)) {
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
        return new FetchRewrite(
            replacedOutputs,
            new Collect(
                relation,
                newOutputs,
                immutableWhere
            )
        );
    }

    @Override
    public Map<LogicalPlan, SelectSymbol> dependencies() {
        return Map.of();
    }

    @Override
    public String toString() {
        return "Collect{" +
               tableInfo.ident() +
               ", [" + Lists2.joinOn(", ", outputs, Symbol::toString) +
               "], " + immutableWhere +
               '}';
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitCollect(this, context);
    }

    @Override
    public void print(PrintContext printContext) {
        printContext
            .text("Collect[")
            .text(tableInfo.ident().toString())
            .text(" | [")
            .text(Lists2.joinOn(", ", outputs, Symbol::toString))
            .text("] | ")
            .text(immutableWhere.queryOrFallback().toString())
            .text("]");
        printStats(printContext);
    }
}
