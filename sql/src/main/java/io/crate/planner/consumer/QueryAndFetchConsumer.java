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
import com.google.common.collect.Iterables;
import io.crate.Constants;
import io.crate.analyze.AnalysisMetaData;
import io.crate.analyze.OrderBy;
import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.analyze.where.WhereClauseContext;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.DocReferenceConverter;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.aggregation.impl.SumAggregation;
import io.crate.operation.predicate.MatchPredicate;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dml.QueryAndFetch;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.AggregationProjection;
import io.crate.planner.projection.ColumnIndexWriterProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.*;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.LongType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.MoreObjects.firstNonNull;

public class QueryAndFetchConsumer implements Consumer {

    private final Visitor visitor;

    public QueryAndFetchConsumer(AnalysisMetaData analysisMetaData){
        visitor = new Visitor(analysisMetaData);
    }


    @Override
    public boolean consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        Context ctx = new Context(context);
        context.rootRelation(visitor.process(context.rootRelation(), ctx));
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

        private final AnalysisMetaData analysisMetaData;

        public Visitor(AnalysisMetaData analysisMetaData){
            this.analysisMetaData = analysisMetaData;
        }

        @Override
        public AnalyzedRelation visitSelectAnalyzedStatement(SelectAnalyzedStatement statement, Context context) {
            if(statement.sources().size() != 1){
                return statement;
            }
            AnalyzedRelation sourceRelation = Iterables.getOnlyElement(statement.sources().entrySet()).getValue();
            if(!(sourceRelation instanceof TableRelation)){
                return statement;
            }

            TableRelation tableRelation = (TableRelation) sourceRelation;
            WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(analysisMetaData, tableRelation);
            WhereClauseContext whereClauseContext = whereClauseAnalyzer.analyze(tableRelation.resolve(statement.querySpec().where()));
            if(whereClauseContext.whereClause().version().isPresent()){
                context.consumerContext.validationException(new VersionInvalidException());
                return statement;
            }
            TableInfo tableInfo = tableRelation.tableInfo();

            if (tableInfo.schemaInfo().systemSchema() && whereClauseContext.whereClause().hasQuery()) {
                ensureNoLuceneOnlyPredicates(whereClauseContext.whereClause().query());
            }
            if (statement.querySpec().hasAggregates()) {
                context.result = true;
                return GlobalAggregateConsumer.globalAggregates(statement, tableRelation, whereClauseContext, null);
            } else {
               if(tableInfo.rowGranularity().ordinal() >= RowGranularity.DOC.ordinal() &&
                        tableInfo.getRouting(whereClauseContext.whereClause(), null).hasLocations() &&
                        !tableInfo.schemaInfo().systemSchema()){
                   return statement;
               }

               context.result = true;
               return QueryAndFetchConsumer.normalSelect(statement, whereClauseContext, tableRelation, null, analysisMetaData.functions());
            }
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
    }


    public static AnalyzedRelation normalSelect(SelectAnalyzedStatement statement,
                                                WhereClauseContext whereClauseContext,
                                                TableRelation tableRelation,
                                                @Nullable ColumnIndexWriterProjection indexWriterProjection,
                                                Functions functions){
        TableInfo tableInfo = tableRelation.tableInfo();
        WhereClause whereClause = whereClauseContext.whereClause();

        List<Symbol> outputSymbols;
        if (tableInfo.schemaInfo().systemSchema()) {
            outputSymbols = tableRelation.resolve(statement.querySpec().outputs());
        } else {
            outputSymbols = new ArrayList<>(statement.querySpec().outputs().size());
            for (Symbol symbol : statement.querySpec().outputs()) {
                outputSymbols.add(DocReferenceConverter.convertIfPossible(tableRelation.resolve(symbol), tableInfo));
            }
        }
        CollectNode collectNode;
        MergeNode mergeNode;
        OrderBy orderBy = statement.querySpec().orderBy();
        if (indexWriterProjection != null) {
            // insert directly from shards
            assert !statement.querySpec().isLimited() : "insert from sub query with limit or order by is not supported. " +
                    "Analyzer should have thrown an exception already.";

            ImmutableList<Projection> projections = ImmutableList.<Projection>of(indexWriterProjection);
            collectNode = PlanNodeBuilder.collect(tableInfo, whereClause, outputSymbols, projections);
            // use aggregation projection to merge node results (number of inserted rows)
            mergeNode = PlanNodeBuilder.localMerge(ImmutableList.<Projection>of(localMergeProjection(functions)), collectNode);
        } else if (statement.querySpec().isLimited() || orderBy != null) {
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
            int limit = firstNonNull(statement.querySpec().limit(), Constants.DEFAULT_SELECT_LIMIT);
            if (orderBy == null){
                tnp = new TopNProjection(statement.querySpec().offset() + limit, 0);
            } else {
                tnp = new TopNProjection(statement.querySpec().offset() + limit, 0,
                        orderByInputColumns,
                        orderBy.reverseFlags(),
                        orderBy.nullsFirst()
                );
            }
            tnp.outputs(allOutputs);
            collectNode = PlanNodeBuilder.collect(tableInfo, whereClause, toCollect, ImmutableList.<Projection>of(tnp));

            if (orderBy == null) {
                tnp = new TopNProjection(limit, statement.querySpec().offset());
            } else {
                tnp = new TopNProjection(limit, statement.querySpec().offset(),
                        orderByInputColumns,
                        orderBy.reverseFlags(),
                        orderBy.nullsFirst());
            }
            tnp.outputs(finalOutputs);
            mergeNode = PlanNodeBuilder.localMerge(ImmutableList.<Projection>of(tnp), collectNode);
        } else {
            collectNode = PlanNodeBuilder.collect(tableInfo, whereClause, outputSymbols, ImmutableList.<Projection>of());
            mergeNode = PlanNodeBuilder.localMerge(ImmutableList.<Projection>of(), collectNode);
        }
        return new QueryAndFetch(collectNode, mergeNode);
    }

    public static AggregationProjection localMergeProjection(Functions functions) {
        return new AggregationProjection(
                Arrays.asList(new Aggregation(
                        functions.getSafe(
                                new FunctionIdent(SumAggregation.NAME, Arrays.<DataType>asList(LongType.INSTANCE))
                        ).info(),
                        Arrays.<Symbol>asList(new InputColumn(0, DataTypes.LONG)),
                        Aggregation.Step.ITER,
                        Aggregation.Step.FINAL
                )
                )
        );
    }
}
