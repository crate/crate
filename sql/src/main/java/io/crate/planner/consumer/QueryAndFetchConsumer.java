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

import com.google.common.collect.ImmutableList;
import io.crate.Constants;
import io.crate.analyze.*;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.DocReferenceConverter;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.predicate.MatchPredicate;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.Planner;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.QueryAndFetch;
import io.crate.planner.projection.MergeProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolVisitor;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.MoreObjects.firstNonNull;

@Singleton
public class QueryAndFetchConsumer implements Consumer {

    private static final CollectPhaseOrderedProjectionBuilder ORDERED_PROJECTION_BUILDER =
            new CollectPhaseOrderedProjectionBuilder();
    private final Visitor visitor;

    @Inject
    public QueryAndFetchConsumer() {
        visitor = new Visitor();
    }

    @Override
    public PlannedAnalyzedRelation consume(AnalyzedRelation relation, ConsumerContext context) {
        return visitor.process(relation, context);
    }

    private static class Visitor extends AnalyzedRelationVisitor<ConsumerContext, PlannedAnalyzedRelation> {

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
            OrderBy orderBy = table.querySpec().orderBy();
            List<Symbol> orderBySymbols;
            if (orderBy == null) {
                orderBySymbols = Collections.emptyList();
            } else {
                orderBySymbols = orderBy.orderBySymbols();
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
            return normalSelect(table, table.querySpec().where(), context, outputSymbols);
        }

        @Override
        public PlannedAnalyzedRelation visitQueriedTable(QueriedTable table, ConsumerContext context) {
            if (table.querySpec().hasAggregates()) {
                return null;
            }
            if (table.querySpec().where().hasQuery()) {
                ensureNoLuceneOnlyPredicates(table.querySpec().where().query());
            }
            return normalSelect(table, table.querySpec().where(), context, table.querySpec().outputs());
        }

        @Override
        protected PlannedAnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, ConsumerContext context) {
            return null;
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
                                                     WhereClause whereClause,
                                                     ConsumerContext context,
                                                     List<Symbol> outputSymbols){
            QuerySpec querySpec = table.querySpec();
            TableInfo tableInfo = table.tableRelation().tableInfo();

            CollectPhase collectNode;
            MergePhase mergeNode = null;
            OrderBy orderBy = querySpec.orderBy();
            Planner.Context plannerContext = context.plannerContext();
            if (querySpec.isLimited() || orderBy != null) {
                /**
                 * select id, name, order by id, date
                 *
                 * toCollect:       [id, name, date]            // includes order by symbols, that aren't already selected
                 * allOutputs:      [in(0), in(1), in(2)]       // for topN projection on shards/collectNode
                 * orderByInputs:   [in(0), in(2)]              // for topN projection on shards/collectNode AND handler
                 * finalOutputs:    [in(0), in(1)]              // for topN output on handler -> changes output to what should be returned.
                 */

                List<Symbol> toCollect;
                List<Symbol> orderByInputColumns = null;
                if (orderBy != null){
                    List<Symbol> orderBySymbols = orderBy.orderBySymbols();
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

                int limit = firstNonNull(querySpec.limit(), Constants.DEFAULT_SELECT_LIMIT);

                CollectPhaseOrderedProjectionBuilderContext projectionBuilderContext =
                        new CollectPhaseOrderedProjectionBuilderContext(querySpec, orderByInputColumns, allOutputs);

                List<Projection> collectPhaseProjections = ORDERED_PROJECTION_BUILDER.process(table, projectionBuilderContext);
                collectNode = PlanNodeBuilder.collect(
                        plannerContext.jobId(),
                        tableInfo,
                        plannerContext,
                        whereClause,
                        toCollect,
                        collectPhaseProjections,
                        projectionBuilderContext.orderBy,
                        projectionBuilderContext.limit);


                // MERGE
                if (context.rootRelation() == table) {
                    TopNProjection tnp = new TopNProjection(limit, querySpec.offset());
                    tnp.outputs(finalOutputs);
                    if (orderBy == null) {
                        // no sorting needed
                        mergeNode = MergePhase.localMerge(
                                plannerContext.jobId(),
                                plannerContext.nextExecutionPhaseId(),
                                ImmutableList.<Projection>of(tnp),
                                collectNode
                        );
                    } else {
                        // no order by needed in TopN as we already sorted on collector
                        // and we merge sorted with SortedBucketMerger
                        mergeNode = MergePhase.sortedMerge(
                                plannerContext.jobId(),
                                plannerContext.nextExecutionPhaseId(),
                                orderBy,
                                allOutputs,
                                orderByInputColumns,
                                ImmutableList.<Projection>of(tnp),
                                collectNode
                        );
                    }
                }
            } else {
                collectNode = PlanNodeBuilder.collect(
                        plannerContext.jobId(),
                        tableInfo,
                        plannerContext,
                        whereClause,
                        outputSymbols,
                        ImmutableList.<Projection>of()
                );
                if (context.rootRelation() == table) {
                    mergeNode = MergePhase.localMerge(
                            plannerContext.jobId(),
                            plannerContext.nextExecutionPhaseId(),
                            ImmutableList.<Projection>of(),
                            collectNode);
                }
            }
            return new QueryAndFetch(collectNode, mergeNode, plannerContext.jobId());
        }

        private static List<Symbol> toInputColumns(List<Symbol> symbols) {
            List<Symbol> inputColumns = new ArrayList<>(symbols.size());
            for (int i = 0; i < symbols.size(); i++) {
                inputColumns.add(new InputColumn(i, symbols.get(i).valueType()));
            }
            return inputColumns;
        }
    }

    private static class CollectPhaseOrderedProjectionBuilderContext {
        OrderBy orderBy;
        int offset;
        Integer limit;
        List<Symbol> orderByInputColumns;
        List<Symbol> allOutputs;

        public CollectPhaseOrderedProjectionBuilderContext(QuerySpec querySpec,
                                                           List<Symbol> orderByInputColumns,
                                                           List<Symbol> allOutputs) {
            this.orderByInputColumns = orderByInputColumns;
            this.allOutputs = allOutputs;
            this.orderBy = querySpec.orderBy();
            this.offset = querySpec.offset();
            this.limit = firstNonNull(querySpec.limit(), Constants.DEFAULT_SELECT_LIMIT) + querySpec.offset();
        }
    }

    private static class CollectPhaseOrderedProjectionBuilder extends AnalyzedRelationVisitor<CollectPhaseOrderedProjectionBuilderContext, List<Projection>> {

        @Override
        public List<Projection> visitQueriedTable(QueriedTable table,
                                            CollectPhaseOrderedProjectionBuilderContext context) {
            // if we have an offset we have to get as much docs from every node as we have offset+limit
            // otherwise results will be wrong
            TopNProjection topNProjection;
            if (context.orderBy == null) {
                topNProjection = new TopNProjection(context.offset + context.limit, 0);
            } else {
                topNProjection = new TopNProjection(context.offset + context.limit, 0,
                        context.orderByInputColumns,
                        context.orderBy.reverseFlags(),
                        context.orderBy.nullsFirst()
                );
            }
            topNProjection.outputs(context.allOutputs);
            // this values are passed to the CollectPhase,a QueriedTable does not support these
            // at CollectPhase so null it
            context.orderBy = null;
            context.limit = null;
            return ImmutableList.<Projection>of(topNProjection);
        }

        @Override
        public List<Projection> visitQueriedDocTable(QueriedDocTable table,
                                               CollectPhaseOrderedProjectionBuilderContext context) {
            if (context.orderBy == null) {
                return ImmutableList.of();
            } else {
                MergeProjection mergeProjection = new MergeProjection(
                        context.allOutputs,
                        context.orderByInputColumns,
                        context.orderBy.reverseFlags(),
                        context.orderBy.nullsFirst()
                );
                TopNProjection topNProjection = new TopNProjection(context.offset + context.limit, 0);
                topNProjection.outputs(context.allOutputs);
                return ImmutableList.of(mergeProjection, topNProjection);
            }
        }

        @Override
        protected List<Projection> visitAnalyzedRelation(AnalyzedRelation relation,
                                                     CollectPhaseOrderedProjectionBuilderContext context) {
            throw new UnsupportedOperationException("relation not supported");
        }
    }
}
