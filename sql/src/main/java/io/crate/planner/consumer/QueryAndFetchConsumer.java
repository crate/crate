/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.planner.consumer;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.QueriedTable;
import io.crate.analyze.QueriedTableRelation;
import io.crate.analyze.QueryClause;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolVisitor;
import io.crate.collections.Lists2;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.exceptions.VersionInvalidException;
import io.crate.operation.predicate.MatchPredicate;
import io.crate.operation.projectors.TopN;
import io.crate.planner.Limits;
import io.crate.planner.Merge;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.ReaderAllocations;
import io.crate.planner.fetch.FetchRewriter;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.PlanWithFetchDescription;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.node.fetch.FetchPhase;
import io.crate.planner.node.fetch.FetchSource;
import io.crate.planner.projection.EvalProjection;
import io.crate.planner.projection.FetchProjection;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.projection.builder.InputColumns;
import io.crate.planner.projection.builder.ProjectionBuilder;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class QueryAndFetchConsumer implements Consumer {

    private final Visitor visitor;

    public QueryAndFetchConsumer() {
        visitor = new Visitor();
    }

    @Override
    public Plan consume(AnalyzedRelation relation, ConsumerContext context) {
        return visitor.process(relation, context);
    }

    private static final  class Visitor extends RelationPlanningVisitor {

        @Override
        public Plan visitQueriedDocTable(QueriedDocTable table, ConsumerContext context) {
            QuerySpec qs = table.querySpec();
            if (!isSimpleSelect(qs, context)) {
                return null;
            }
            FetchMode fetchMode = context.fetchMode();
            if (fetchMode == FetchMode.NEVER || !FetchRewriter.isFetchFeasible(qs)) {
                return normalSelect(table, context);
            }
            FetchRewriter.FetchDescription fetchDescription = FetchRewriter.rewrite(table);
            Plan subPlan = normalSelect(table, context);
            if (fetchMode == FetchMode.NO_PROPAGATION) {
                Planner.Context plannerContext = context.plannerContext();
                subPlan = Merge.ensureOnHandler(subPlan, plannerContext);
                return planFetch(fetchDescription, plannerContext, subPlan);
            }
            return new PlanWithFetchDescription(subPlan, fetchDescription);
        }

        @Override
        public Plan visitQueriedTable(QueriedTable table, ConsumerContext context) {
            QuerySpec querySpec = table.querySpec();
            if (!isSimpleSelect(querySpec, context)) {
                return null;
            }
            if (querySpec.where().hasQuery()) {
                NoPredicateVisitor.ensureNoMatchPredicate(querySpec.where().query());
            }
            return normalSelect(table, context);
        }

        @Override
        public Plan visitQueriedSelectRelation(QueriedSelectRelation relation, ConsumerContext context) {
            QuerySpec qs = relation.querySpec();
            if (!isSimpleSelect(qs, context)) {
                return null;
            }
            Planner.Context plannerContext = context.plannerContext();
            QueriedRelation subRelation = relation.subRelation();
            FetchMode parentFetchMode = context.fetchMode();
            if (parentFetchMode != FetchMode.NEVER) {
                context.setFetchMode(FetchMode.WITH_PROPAGATION);
            }
            Plan subPlan = plannerContext.planSubRelation(subRelation, context);
            context.setFetchMode(parentFetchMode);

            FetchRewriter.FetchDescription fetchDescription = subPlan.resultDescription().fetchDescription();
            if (fetchDescription == null) {
                return finalizeQueriedSelectPlanWithoutFetch(qs, plannerContext, subRelation, subPlan);
            }
            return tryToPropagateFetch(qs, subRelation, subPlan, context);
        }
    }

    /**
     *  - Applies WHERE / ORDER BY / LIMIT projections PRE-fetch if possible
     *  - Propagates FetchDescription to parent relation if possible (only if requested via FetchMode)
     *  - Finalizes plan (fetch projection + phase) if propagation is not possible or wasn't requested by a parent relation
     */
    private static Plan tryToPropagateFetch(QuerySpec querySpec,
                                            QueriedRelation subRelation,
                                            Plan plan,
                                            ConsumerContext context) {
        FetchRewriter.FetchDescription fetchDescription = plan.resultDescription().fetchDescription();
        Planner.Context plannerContext = context.plannerContext();
        plan = Merge.ensureOnHandler(plan, plannerContext);

        WhereClause where = querySpec.where();
        boolean appliedWhere = tryApplyWherePreFetch(plan, fetchDescription, where);

        Limits limits = plannerContext.getLimits(querySpec);
        boolean appliedOrderByAndLimit = false;
        if (appliedWhere) {
            appliedOrderByAndLimit = tryApplyOrderAndLimitPreFetch(querySpec, plan, fetchDescription, limits);
        }

        if (appliedWhere && appliedOrderByAndLimit && context.fetchMode() == FetchMode.WITH_PROPAGATION) {
            fetchDescription.updatePostFetchOutputs(querySpec.outputs());
            return new PlanWithFetchDescription(plan, fetchDescription);
        }

        plan = planFetch(fetchDescription, plannerContext, plan);
        if (!appliedWhere) {
            applyFilter(plan, subRelation.fields(), where);
        }
        if (appliedOrderByAndLimit) {
            if (!querySpec.outputs().equals(subRelation.fields())) {
                EvalProjection eval = new EvalProjection(
                    InputColumns.create(querySpec.outputs(), new InputColumns.Context(subRelation.fields())));
                plan.addProjection(eval);
            }
        } else {
            Projection projection = ProjectionBuilder.topNOrEval(
                subRelation.fields(),
                querySpec.orderBy(),
                limits.offset(),
                limits.finalLimit(),
                querySpec.outputs()
            );
            plan.addProjection(projection, TopN.NO_LIMIT, 0, null);
        }
        return plan;
    }

    private static boolean tryApplyWherePreFetch(Plan plan,
                                                 FetchRewriter.FetchDescription fetchDescription,
                                                 WhereClause where) {
        if (where.noMatch()) {
            applyFilter(plan, fetchDescription.preFetchOutputs(), where);
            return true;
        }
        if (!where.hasQuery()) {
            return true;
        }
        if (fetchDescription.availablePreFetch(where.query())) {
            Symbol query = fetchDescription.mapFieldsInTreeToPostFetch().apply(where.query());
            FilterProjection filterProjection = new FilterProjection(
                InputColumns.create(query, fetchDescription.preFetchOutputs()),
                InputColumn.fromSymbols(fetchDescription.preFetchOutputs())
            );
            plan.addProjection(filterProjection);
            return true;
        }
        return false;
    }

    private static boolean tryApplyOrderAndLimitPreFetch(QuerySpec qs,
                                                         Plan plan,
                                                         FetchRewriter.FetchDescription fetchDescription,
                                                         Limits limits) {
        OrderBy orderBy = qs.orderBy();
        if (orderBy == null) {
            TopNProjection topN = new TopNProjection(
                limits.finalLimit(),
                limits.offset(),
                InputColumn.fromSymbols(fetchDescription.preFetchOutputs()));
            plan.addProjection(topN, TopN.NO_LIMIT, 0, plan.resultDescription().orderBy());
            return true;
        }
        if (fetchDescription.availablePreFetch(orderBy.orderBySymbols())) {
            orderBy.replace(fetchDescription.mapFieldsInTreeToPostFetch());
            Projection projection = ProjectionBuilder.topNOrEval(
                fetchDescription.preFetchOutputs(),
                orderBy,
                limits.offset(),
                limits.finalLimit(),
                fetchDescription.preFetchOutputs()
            );
            plan.addProjection(projection, TopN.NO_LIMIT, 0, null);
            return true;
        }
        return false;
    }

    private static Plan planFetch(FetchRewriter.FetchDescription fetchDescription,
                                  Planner.Context plannerContext,
                                  Plan subPlan) {
        ReaderAllocations readerAllocations = plannerContext.buildReaderAllocations();
        FetchPhase fetchPhase = new FetchPhase(
            plannerContext.nextExecutionPhaseId(),
            readerAllocations.nodeReaders().keySet(),
            readerAllocations.bases(),
            readerAllocations.tableIndices(),
            fetchDescription.fetchRefs()
        );
        InputColumn fetchId = new InputColumn(0);
        FetchSource fetchSource = new FetchSource(
            fetchDescription.partitionedByColumns(),
            Collections.singletonList(fetchId),
            fetchDescription.fetchRefs()
        );
        FetchProjection fetchProjection = new FetchProjection(
            fetchPhase.phaseId(),
            plannerContext.fetchSize(),
            ImmutableMap.of(fetchDescription.table(), fetchSource),
            FetchRewriter.generateFetchOutputs(fetchDescription),
            readerAllocations.nodeReaders(),
            readerAllocations.indices(),
            readerAllocations.indicesToIdents()
        );
        subPlan.addProjection(fetchProjection);
        return new QueryThenFetch(subPlan, fetchPhase);
    }

    private static Plan finalizeQueriedSelectPlanWithoutFetch(QuerySpec qs,
                                                              Planner.Context plannerContext,
                                                              QueriedRelation subRelation,
                                                              Plan subPlan) {
        subPlan = Merge.ensureOnHandler(subPlan, plannerContext);
        applyFilter(subPlan, subRelation.fields(), qs.where());
        Limits limits = plannerContext.getLimits(qs);
        Projection projection = ProjectionBuilder.topNOrEval(
            subRelation.fields(),
            qs.orderBy(),
            limits.offset(),
            limits.finalLimit(),
            qs.outputs()
        );
        subPlan.addProjection(projection, TopN.NO_LIMIT, 0, null);
        return subPlan;
    }

    private static void applyFilter(Plan plan, Collection<? extends Symbol> filterInputs, QueryClause query) {
        if (query.hasQuery() || query.noMatch()) {
            FilterProjection filterProjection = ProjectionBuilder.filterProjection(filterInputs, query);
            plan.addProjection(filterProjection);
        }
    }

    private static boolean isSimpleSelect(QuerySpec querySpec, ConsumerContext context) {
        if (querySpec.hasAggregates() || !querySpec.groupBy().isEmpty()) {
            return false;
        }
        if (querySpec.where().hasVersions()) {
            context.validationException(new VersionInvalidException());
            return false;
        }
        return true;
    }

    /**
     * Create a (distributed if possible) Collect plan.
     *
     * Data will be pre-sorted if there is a ORDER BY clause.
     * Limit (incl offset) will be pre-applied.
     *
     * Both ORDER-BY & Limit needs to be "finalized" in a Merge-to-handler.
     */
    private static Collect normalSelect(QueriedTableRelation table, ConsumerContext context) {
        QuerySpec querySpec = table.querySpec();
        Planner.Context plannerContext = context.plannerContext();
        OrderBy orderBy = querySpec.orderBy();
        /*
         * ORDER BY columns are added to OUTPUTS - they're required to do an ordered merge.
         *
         * select name order by date
         *
         * qsOutputs:           [name]
         * toCollect:           [name, date]           // includes order by symbols, that aren't already selected
         */
        List<Symbol> qsOutputs = querySpec.outputs();
        List<Symbol> toCollect = getToCollectSymbols(qsOutputs, orderBy);
        table.tableRelation().validateOrderBy(orderBy);

        Limits limits = plannerContext.getLimits(querySpec);
        RoutedCollectPhase collectPhase = RoutedCollectPhase.forQueriedTable(
            plannerContext,
            table,
            toCollect,
            topNOrEmptyProjections(toCollect, limits)
        );
        tryApplySizeHint(context.requiredPageSize(), limits, collectPhase);
        collectPhase.orderBy(orderBy);
        return new Collect(
            collectPhase,
            limits.finalLimit(),
            limits.offset(),
            qsOutputs.size(),
            limits.limitAndOffset(),
            PositionalOrderBy.of(orderBy, toCollect)
        );
    }

    private static void tryApplySizeHint(@Nullable Integer requiredPageSize, Limits limits, RoutedCollectPhase collectPhase) {
        if (requiredPageSize == null) {
            if (limits.hasLimit()) {
                collectPhase.nodePageSizeHint(limits.limitAndOffset());
            }
        } else {
            collectPhase.pageSizeHint(requiredPageSize);
        }
    }

    private static List<Projection> topNOrEmptyProjections(List<Symbol> toCollect, Limits limits) {
        if (limits.hasLimit()) {
            return Collections.singletonList(new TopNProjection(
                limits.limitAndOffset(),
                0,
                InputColumn.fromSymbols(toCollect)
            ));
        }
        return Collections.emptyList();
    }

    /**
     * @return qsOutputs + symbols from orderBy which are not already within qsOutputs (if orderBy is present)
     */
    private static List<Symbol> getToCollectSymbols(List<Symbol> qsOutputs, @Nullable OrderBy orderBy) {
        if (orderBy == null) {
            return qsOutputs;
        }
        return Lists2.concatUnique(qsOutputs, orderBy.orderBySymbols());
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
