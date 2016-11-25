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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
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
import io.crate.planner.Limits;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;

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

    private static class Visitor extends RelationPlanningVisitor {

        private static final NoPredicateVisitor NO_PREDICATE_VISITOR = new NoPredicateVisitor();

        @Override
        public Plan visitQueriedDocTable(QueriedDocTable table, ConsumerContext context) {
            if (table.querySpec().hasAggregates()) {
                return null;
            }
            if (table.querySpec().where().hasVersions()) {
                context.validationException(new VersionInvalidException());
                return null;
            }
            return normalSelect(table, context);
        }

        @Override
        public Plan visitQueriedTable(QueriedTable table, ConsumerContext context) {
            QuerySpec querySpec = table.querySpec();
            if (querySpec.hasAggregates()) {
                return null;
            }
            if (querySpec.where().hasQuery()) {
                ensureNoLuceneOnlyPredicates(querySpec.where().query());
            }
            return normalSelect(table, context);
        }

        private void ensureNoLuceneOnlyPredicates(Symbol query) {
            NO_PREDICATE_VISITOR.process(query, null);
        }

        private static class NoPredicateVisitor extends SymbolVisitor<Void, Void> {
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

        private Plan normalSelect(QueriedTableRelation table, ConsumerContext context) {
            QuerySpec querySpec = table.querySpec();
            Planner.Context plannerContext = context.plannerContext();
            /*
             * ORDER BY columns are added to OUTPUTS - they're required to do an ordered merge.
             * select id, name, order by id, date
             *
             * toCollect:           [id, name, date]       // includes order by symbols, that aren't already selected
             * outputsInclOrder:    [in(0), in(1), in(2)]  // for topN projection on shards/collectPhase
             * orderByInputs:       [in(0), in(2)]         // for topN projection on shards/collectPhase AND handler
             * finalOutputs:        [in(0), in(1)]         // for topN output on handler -> changes output to what should be returned.
             */
            List<Symbol> qsOutputs = querySpec.outputs();
            List<Symbol> toCollect;
            Optional<OrderBy> optOrderBy = querySpec.orderBy();
            table.tableRelation().validateOrderBy(optOrderBy);
            if (optOrderBy.isPresent()) {
                toCollect = Lists2.concatUnique(qsOutputs, optOrderBy.get().orderBySymbols());
            } else {
                toCollect = qsOutputs;
            }
            List<Symbol> outputsInclOrder = InputColumn.fromSymbols(toCollect);

            Limits limits = plannerContext.getLimits(querySpec);
            List<Projection> projections = ImmutableList.of();
            if (limits.hasLimit()) {
                TopNProjection collectTopN = new TopNProjection(limits.limitAndOffset(), 0, outputsInclOrder);
                projections = Collections.singletonList(collectTopN);
            }
            RoutedCollectPhase collectPhase = RoutedCollectPhase.forQueriedTable(
                plannerContext,
                table,
                toCollect,
                projections,
                table.relationId()
            );
            Integer requiredPageSize = context.requiredPageSize();
            if (requiredPageSize == null ) {
                if (limits.hasLimit()) {
                    collectPhase.nodePageSizeHint(limits.limitAndOffset());
                }
            } else {
                collectPhase.pageSizeHint(requiredPageSize);
            }
            collectPhase.orderBy(optOrderBy.orNull());

            return new Collect(
                collectPhase,
                limits.finalLimit(),
                limits.offset(),
                qsOutputs.size(),
                limits.limitAndOffset(),
                PositionalOrderBy.of(optOrderBy.orNull(), toCollect)
            );
        }
    }
}
