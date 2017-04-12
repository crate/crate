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

import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedTable;
import io.crate.analyze.QueriedTableRelation;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolVisitor;
import io.crate.collections.Lists2;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.exceptions.VersionInvalidException;
import io.crate.operation.predicate.MatchPredicate;
import io.crate.planner.*;
import io.crate.planner.fetch.FetchPushDown;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class QueryAndFetchConsumer implements Consumer {

    private final Visitor visitor;

    public QueryAndFetchConsumer() {
        visitor = new Visitor();
    }

    @Override
    public Plan consume(AnalyzedRelation relation, ConsumerContext context) {
        return visitor.process(relation, context);
    }

    private final static class Visitor extends RelationPlanningVisitor {

        @Override
        public Plan visitQueriedDocTable(QueriedDocTable table, ConsumerContext context) {
            if (!isSimpleSelect(table.querySpec(), context)) {
                return null;
            }
            if (!context.fetchDecider().tryFetchRewrite()) {
                return normalSelect(table, context);
            }
            FetchPushDown.Builder<QueriedDocTable> fetchPhaseBuilder = FetchPushDown.pushDown(table);
            if (fetchPhaseBuilder == null) {
                return normalSelect(table, context);
            }
            Planner.Context plannerContext = context.plannerContext();
            Plan plan = Merge.ensureOnHandler(
                normalSelect(fetchPhaseBuilder.replacedRelation(), context), plannerContext);
            FetchPushDown.PhaseAndProjection fetchPhaseAndProjection = fetchPhaseBuilder.build(plannerContext);
            plan.addProjection(
                fetchPhaseAndProjection.projection,
                null,
                null,
                fetchPhaseAndProjection.projection.outputs().size(),
                null
            );
            return new QueryThenFetch(plan, fetchPhaseAndProjection.phase);
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
    }

    private static boolean isSimpleSelect(QuerySpec querySpec, ConsumerContext context) {
        if (querySpec.hasAggregates() || querySpec.groupBy().isPresent()) {
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
        Optional<OrderBy> optOrderBy = querySpec.orderBy();
        /*
         * ORDER BY columns are added to OUTPUTS - they're required to do an ordered merge.
         *
         * select name order by date
         *
         * qsOutputs:           [name]
         * toCollect:           [name, date]           // includes order by symbols, that aren't already selected
         */
        List<Symbol> qsOutputs = querySpec.outputs();
        List<Symbol> toCollect = getToCollectSymbols(qsOutputs, optOrderBy);
        table.tableRelation().validateOrderBy(optOrderBy);

        Limits limits = plannerContext.getLimits(querySpec);
        RoutedCollectPhase collectPhase = RoutedCollectPhase.forQueriedTable(
            plannerContext,
            table,
            toCollect,
            topNOrEmptyProjections(toCollect, limits)
        );
        tryApplySizeHint(context.requiredPageSize(), limits, collectPhase);
        collectPhase.orderBy(optOrderBy.orElse(null));
        return new Collect(
            collectPhase,
            limits.finalLimit(),
            limits.offset(),
            qsOutputs.size(),
            limits.limitAndOffset(),
            PositionalOrderBy.of(optOrderBy.orElse(null), toCollect)
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
    private static List<Symbol> getToCollectSymbols(List<Symbol> qsOutputs, Optional<OrderBy> optOrderBy) {
        if (optOrderBy.isPresent()) {
            return Lists2.concatUnique(qsOutputs, optOrderBy.get().orderBySymbols());
        }
        return qsOutputs;
    }

    private final static class NoPredicateVisitor extends SymbolVisitor<Void, Void> {

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
