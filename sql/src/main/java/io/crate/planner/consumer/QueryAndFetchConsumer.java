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
import io.crate.analyze.relations.TableRelation;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.DocReferenceConverter;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.aggregation.impl.SumAggregation;
import io.crate.operation.predicate.MatchPredicate;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.node.dql.QueryAndFetch;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.AggregationProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.*;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.LongType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.MoreObjects.firstNonNull;

public class QueryAndFetchConsumer implements Consumer {

    private static final Visitor VISITOR = new Visitor();

    @Override
    public boolean consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        Context ctx = new Context(context);
        context.rootRelation(VISITOR.process(context.rootRelation(), ctx));
        return ctx.result;
    }

    private static class Context {
        ConsumerContext consumerContext;
        boolean result = false;

        public Context(ConsumerContext context) {
            this.consumerContext = context;
        }
    }

    private static class Visitor extends AnalyzedRelationVisitor<Context, AnalyzedRelation> {

        @Override
        public AnalyzedRelation visitQueriedTable(QueriedTable table, Context context) {
            TableRelation tableRelation = table.tableRelation();
            if(table.querySpec().where().hasVersions()){
                context.consumerContext.validationException(new VersionInvalidException());
                return table;
            }
            TableInfo tableInfo = tableRelation.tableInfo();

            if (tableInfo.schemaInfo().systemSchema() && table.querySpec().where().hasQuery()) {
                ensureNoLuceneOnlyPredicates(table.querySpec().where().query());
            }
            if (table.querySpec().hasAggregates()) {
                context.result = true;
                return GlobalAggregateConsumer.globalAggregates(table, tableRelation, table.querySpec().where(), context.consumerContext);
            } else {
               context.result = true;
               return normalSelect(table, table.querySpec().where(), tableRelation, context);
            }
        }

        @Override
        public AnalyzedRelation visitInsertFromQuery(InsertFromSubQueryAnalyzedStatement insertFromSubQueryAnalyzedStatement,
                                                     Context context) {
            InsertFromSubQueryConsumer.planInnerRelation(insertFromSubQueryAnalyzedStatement, context, this);
            return insertFromSubQueryAnalyzedStatement;
        }

        @Override
        protected AnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, Context context) {
            return relation;
        }

        private void ensureNoLuceneOnlyPredicates(Symbol query) {
            NoPredicateVisitor noPredicateVisitor = new NoPredicateVisitor();
            noPredicateVisitor.process(query, null);
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

        private AnalyzedRelation normalSelect(QueriedTable table,
                                              WhereClause whereClause,
                                              TableRelation tableRelation,
                                              Context context){
            QuerySpec querySpec = table.querySpec();
            TableInfo tableInfo = tableRelation.tableInfo();

            List<Symbol> outputSymbols;
            if (tableInfo.schemaInfo().systemSchema()) {
                outputSymbols = tableRelation.resolve(querySpec.outputs());
            } else {
                outputSymbols = new ArrayList<>(querySpec.outputs().size());
                for (Symbol symbol : querySpec.outputs()) {
                    outputSymbols.add(DocReferenceConverter.convertIfPossible(tableRelation.resolve(symbol), tableInfo));
                }
            }
            CollectNode collectNode;
            MergeNode mergeNode = null;
            OrderBy orderBy = querySpec.orderBy();
            if (context.consumerContext.rootRelation() != table) {
                // insert directly from shards
                assert !querySpec.isLimited() : "insert from sub query with limit or order by is not supported. " +
                        "Analyzer should have thrown an exception already.";

                ImmutableList<Projection> projections = ImmutableList.<Projection>of();
                collectNode = PlanNodeBuilder.collect(tableInfo,
                        context.consumerContext.plannerContext(),
                        whereClause, outputSymbols, projections);
            } else if (querySpec.isLimited() || orderBy != null) {
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
                    List<Symbol> orderBySymbols = tableRelation.resolve(orderBy.orderBySymbols());
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

                List<Symbol> allOutputs = new ArrayList<>(toCollect.size());        // outputs from collector
                for (int i = 0; i < toCollect.size(); i++) {
                    allOutputs.add(new InputColumn(i, toCollect.get(i).valueType()));
                }
                List<Symbol> finalOutputs = new ArrayList<>(outputSymbols.size());  // final outputs on handler after sort
                for (int i = 0; i < outputSymbols.size(); i++) {
                    finalOutputs.add(new InputColumn(i, outputSymbols.get(i).valueType()));
                }

                // if we have an offset we have to get as much docs from every node as we have offset+limit
                // otherwise results will be wrong
                TopNProjection tnp;
                int limit = firstNonNull(querySpec.limit(), Constants.DEFAULT_SELECT_LIMIT);
                if (orderBy == null){
                    tnp = new TopNProjection(querySpec.offset() + limit, 0);
                } else {
                    tnp = new TopNProjection(querySpec.offset() + limit, 0,
                            orderByInputColumns,
                            orderBy.reverseFlags(),
                            orderBy.nullsFirst()
                    );
                }
                tnp.outputs(allOutputs);
                collectNode = PlanNodeBuilder.collect(tableInfo,
                        context.consumerContext.plannerContext(),
                        whereClause, toCollect, ImmutableList.<Projection>of(tnp));

                // MERGE
                tnp = new TopNProjection(limit, querySpec.offset());
                tnp.outputs(finalOutputs);
                if (orderBy == null) {
                    // no sorting needed
                    mergeNode = PlanNodeBuilder.localMerge(ImmutableList.<Projection>of(tnp), collectNode,
                            context.consumerContext.plannerContext());
                } else {
                    // no order by needed in TopN as we already sorted on collector
                    // and we merge sorted with SortedBucketMerger
                    mergeNode = PlanNodeBuilder.sortedLocalMerge(
                            ImmutableList.<Projection>of(tnp),
                            orderBy,
                            allOutputs,
                            orderByInputColumns,
                            collectNode,
                            context.consumerContext.plannerContext()
                    );
                }
            } else {
                collectNode = PlanNodeBuilder.collect(tableInfo,
                        context.consumerContext.plannerContext(),
                        whereClause, outputSymbols, ImmutableList.<Projection>of());
                mergeNode = PlanNodeBuilder.localMerge(
                        ImmutableList.<Projection>of(), collectNode,
                        context.consumerContext.plannerContext());
            }
            return new QueryAndFetch(collectNode, mergeNode);
        }
    }
}
