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
import io.crate.Constants;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedTable;
import io.crate.analyze.QueriedTableRelation;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolVisitor;
import io.crate.collections.Lists2;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.exceptions.VersionInvalidException;
import io.crate.operation.predicate.MatchPredicate;
import io.crate.planner.Planner;
import io.crate.planner.node.dql.CollectAndMerge;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.List;

@Singleton
public class QueryAndFetchConsumer implements Consumer {

    private final Visitor visitor;

    @Inject
    public QueryAndFetchConsumer() {
        visitor = new Visitor();
    }

    @Override
    public PlannedAnalyzedRelation consume(AnalyzedRelation relation, ConsumerContext context) {
        return visitor.process(relation, context);
    }

    private static class Visitor extends RelationPlanningVisitor {

        private static final NoPredicateVisitor NO_PREDICATE_VISITOR = new NoPredicateVisitor();

        @Override
        public PlannedAnalyzedRelation visitQueriedDocTable(QueriedDocTable table, ConsumerContext context) {
            if (table.querySpec().hasAggregates()) {
                return null;
            }
            if(table.querySpec().where().hasVersions()){
                context.validationException(new VersionInvalidException());
                return null;
            }
            return normalSelect(table, context, table.querySpec().outputs());
        }

        @Override
        public PlannedAnalyzedRelation visitQueriedTable(QueriedTable table, ConsumerContext context) {
            QuerySpec querySpec = table.querySpec();
            if (querySpec.hasAggregates()) {
                return null;
            }
            if (querySpec.where().hasQuery()) {
                ensureNoLuceneOnlyPredicates(querySpec.where().query());
            }
            return normalSelect(table, context, querySpec.outputs());
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

        private PlannedAnalyzedRelation normalSelect(QueriedTableRelation table,
                                                     ConsumerContext context,
                                                     List<Symbol> outputSymbols){
            QuerySpec querySpec = table.querySpec();

            RoutedCollectPhase collectPhase;
            MergePhase mergeNode = null;
            Optional<OrderBy> orderBy = querySpec.orderBy();
            Planner.Context plannerContext = context.plannerContext();
            if (querySpec.isLimited() || orderBy.isPresent()) {
                /**
                 * select id, name, order by id, date
                 *
                 * toCollect:       [id, name, date]            // includes order by symbols, that aren't already selected
                 * allOutputs:      [in(0), in(1), in(2)]       // for topN projection on shards/collectPhase
                 * orderByInputs:   [in(0), in(2)]              // for topN projection on shards/collectPhase AND handler
                 * finalOutputs:    [in(0), in(1)]              // for topN output on handler -> changes output to what should be returned.
                 */
                List<Symbol> toCollect;
                if (orderBy.isPresent()) {
                    toCollect = Lists2.concatUnique(outputSymbols, orderBy.get().orderBySymbols());
                } else {
                    toCollect = outputSymbols;
                }
                List<Symbol> allOutputs = InputColumn.fromSymbols(toCollect);
                List<Symbol> finalOutputs = InputColumn.fromSymbols(outputSymbols);

                List<Projection> projections = ImmutableList.of();
                Integer nodePageSizeHint = null;
                if (context.isRoot() || querySpec.limit().isPresent()) {
                    int limit = querySpec.limit().or(Constants.DEFAULT_SELECT_LIMIT);
                    TopNProjection topNProjection = new TopNProjection(querySpec.offset() + limit, 0);
                    topNProjection.outputs(allOutputs);
                    projections = ImmutableList.<Projection>of(topNProjection);
                    nodePageSizeHint = limit + querySpec.offset();
                }
                collectPhase = RoutedCollectPhase.forQueriedTable(
                        plannerContext,
                        table,
                        toCollect,
                        projections
                );
                collectPhase.orderBy(orderBy.orNull());
                collectPhase.nodePageSizeHint(nodePageSizeHint);

                // MERGE
                if (context.isRoot()) {
                    TopNProjection tnp = new TopNProjection(querySpec.limit().or(Constants.DEFAULT_SELECT_LIMIT), querySpec.offset());
                    tnp.outputs(finalOutputs);
                    if (!orderBy.isPresent()) {
                        // no sorting needed
                        mergeNode = MergePhase.localMerge(
                                plannerContext.jobId(),
                                plannerContext.nextExecutionPhaseId(),
                                ImmutableList.<Projection>of(tnp),
                                collectPhase.executionNodes().size(),
                                collectPhase.outputTypes()
                        );
                    } else {
                        // no order by needed in TopN as we already sorted on collector
                        // and we merge sorted with SortedBucketMerger
                        mergeNode = MergePhase.sortedMerge(
                                plannerContext.jobId(),
                                plannerContext.nextExecutionPhaseId(),
                                orderBy.get(),
                                allOutputs,
                                InputColumn.fromSymbols(orderBy.get().orderBySymbols(), toCollect),
                                ImmutableList.<Projection>of(tnp),
                                collectPhase.executionNodes().size(),
                                collectPhase.outputTypes()
                        );
                    }
                }
            } else {
                collectPhase = RoutedCollectPhase.forQueriedTable(
                        plannerContext,
                        table,
                        outputSymbols,
                        ImmutableList.<Projection>of()
                );
                if (context.isRoot()) {
                    mergeNode = MergePhase.localMerge(
                            plannerContext.jobId(),
                            plannerContext.nextExecutionPhaseId(),
                            ImmutableList.<Projection>of(),
                            collectPhase.executionNodes().size(),
                            collectPhase.outputTypes()
                    );
                }
            }

            if (context.requiredPageSize() != null) {
                collectPhase.pageSizeHint(context.requiredPageSize());
            }
            SimpleSelect.enablePagingIfApplicable(
                    collectPhase, mergeNode, querySpec.limit().orNull(), querySpec.offset(), plannerContext.clusterService().localNode().id());
            return new CollectAndMerge(collectPhase, mergeNode);
        }
    }
}
