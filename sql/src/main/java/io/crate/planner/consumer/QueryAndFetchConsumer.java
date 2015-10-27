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
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.DocReferenceConverter;
import io.crate.operation.predicate.MatchPredicate;
import io.crate.planner.Planner;
import io.crate.planner.node.dql.CollectAndMerge;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
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
            List<Symbol> orderBySymbols;
            if (table.querySpec().orderBy().isPresent()) {
                orderBySymbols = table.querySpec().orderBy().get().orderBySymbols();
            } else {
                orderBySymbols = Collections.emptyList();
            }
            List<Symbol> outputSymbols = new ArrayList<>(table.querySpec().outputs().size());
            for (Symbol symbol : table.querySpec().outputs()) {
                if (!orderBySymbols.contains(symbol)) {
                    outputSymbols.add(DocReferenceConverter.convertIfPossible(symbol, table.tableRelation().tableInfo()));
                } else {
                    // if symbol is used in orderBy, field must be loaded to cache anyway,
                    // so do not rewrite it to source lookup
                    outputSymbols.add(symbol);
                }
            }
            return normalSelect(table, context, outputSymbols);
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

            CollectPhase collectPhase;
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
                List<Symbol> orderByInputColumns = null;
                if (orderBy.isPresent()) {
                    List<Symbol> orderBySymbols = orderBy.get().orderBySymbols();
                    toCollect = new ArrayList<>(outputSymbols.size() + orderBySymbols.size());
                    toCollect.addAll(outputSymbols);
                    // note: can only de-dup order by symbols due to non-deterministic functions like select random(), random()
                    for (Symbol orderBySymbol : orderBySymbols) {
                        if (!toCollect.contains(orderBySymbol)) {
                            toCollect.add(orderBySymbol);
                        }
                    }
                    orderByInputColumns = new ArrayList<>();
                    for (Symbol symbol : orderBySymbols) {
                        orderByInputColumns.add(new InputColumn(toCollect.indexOf(symbol), symbol.valueType()));
                    }
                } else {
                    toCollect = new ArrayList<>(outputSymbols.size());
                    toCollect.addAll(outputSymbols);
                }

                List<Symbol> allOutputs = toInputColumns(toCollect);
                List<Symbol> finalOutputs = toInputColumns(outputSymbols);


                int limit = querySpec.limit().or(Constants.DEFAULT_SELECT_LIMIT);
                TopNProjection topNProjection = new TopNProjection(querySpec.offset() + limit, 0);
                topNProjection.outputs(allOutputs);
                collectPhase = CollectPhase.forQueriedTable(
                        plannerContext,
                        table,
                        toCollect,
                        ImmutableList.<Projection>of(topNProjection)
                );
                collectPhase.orderBy(orderBy.orNull());
                collectPhase.nodePageSizeHint(limit + querySpec.offset());

                // MERGE
                if (context.rootRelation() == table) {
                    TopNProjection tnp = new TopNProjection(limit, querySpec.offset());
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
                                orderByInputColumns,
                                ImmutableList.<Projection>of(tnp),
                                collectPhase.executionNodes().size(),
                                collectPhase.outputTypes()
                        );
                    }
                }
            } else {
                collectPhase = CollectPhase.forQueriedTable(
                        plannerContext,
                        table,
                        outputSymbols,
                        ImmutableList.<Projection>of()
                );
                if (context.rootRelation() == table) {
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
            return new CollectAndMerge(collectPhase, mergeNode, plannerContext.jobId());
        }

        private static List<Symbol> toInputColumns(List<Symbol> symbols) {
            List<Symbol> inputColumns = new ArrayList<>(symbols.size());
            for (int i = 0; i < symbols.size(); i++) {
                inputColumns.add(new InputColumn(i, symbols.get(i).valueType()));
            }
            return inputColumns;
        }
    }
}
