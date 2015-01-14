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

package io.crate.planner.v2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.crate.Constants;
import io.crate.analyze.AnalysisMetaData;
import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.RelationVisitor;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.analyze.where.WhereClauseContext;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.DocReferenceConverter;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.aggregation.impl.SumAggregation;
import io.crate.operation.predicate.MatchPredicate;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.PlannerContextBuilder;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dml.QueryAndFetchNode;
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

    private static class Visitor extends RelationVisitor<Context, AnalyzedRelation> {

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
            WhereClauseContext whereClauseContext = whereClauseAnalyzer.analyze(tableRelation.resolve(statement.whereClause()));
            TableInfo tableInfo = tableRelation.tableInfo();

            if (tableInfo.schemaInfo().systemSchema() && whereClauseContext.whereClause().hasQuery()) {
                ensureNoLuceneOnlyPredicates(whereClauseContext.whereClause().query());
            }
            if (statement.hasAggregates()) {
                context.result = true;
                return GlobalAggregateConsumer.globalAggregates(statement, null);
            } else {
               if(tableInfo.rowGranularity().ordinal() >= RowGranularity.DOC.ordinal() &&
                        tableInfo.getRouting(whereClauseContext.whereClause()).hasLocations() &&
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


    public static AnalyzedRelation normalSelect(SelectAnalyzedStatement statement, WhereClauseContext whereClauseContext,
                                                TableRelation tableRelation, ColumnIndexWriterProjection indexWriterProjection, Functions functions){
        TableInfo tableInfo = tableRelation.tableInfo();
        WhereClause whereClause = whereClauseContext.whereClause();
        PlannerContextBuilder contextBuilder = new PlannerContextBuilder()
                .output(tableRelation.resolve(statement.outputSymbols()))
                .orderBy(tableRelation.resolveAndValidateOrderBy(statement.orderBy().orderBySymbols()));
        ImmutableList<Projection> projections;
        if (statement.isLimited()) {
            // if we have an offset we have to get as much docs from every node as we have offset+limit
            // otherwise results will be wrong
            TopNProjection tnp = new TopNProjection(
                    statement.offset() + statement.limit(),
                    0,
                    contextBuilder.orderBy(),
                    statement.orderBy().reverseFlags(),
                    statement.orderBy().nullsFirst()
            );
            tnp.outputs(contextBuilder.outputs());
            projections = ImmutableList.<Projection>of(tnp);
        } else if(indexWriterProjection != null) {
            // no limit, projection (index writer) will run on shard/CollectNode
            projections = ImmutableList.<Projection>of(indexWriterProjection);
        } else {
            projections = ImmutableList.of();
        }

        List<Symbol> toCollect;
        if (tableInfo.schemaInfo().systemSchema()) {
            toCollect = contextBuilder.toCollect();
        } else {
            toCollect = new ArrayList<>();
            for (Symbol symbol : contextBuilder.toCollect()) {
                toCollect.add(DocReferenceConverter.convertIfPossible(symbol, tableInfo));
            }
        }

        CollectNode collectNode = PlanNodeBuilder.collect(tableInfo, whereClause, toCollect, projections);
        ImmutableList.Builder<Projection> projectionBuilder = ImmutableList.builder();

        if (indexWriterProjection == null || statement.isLimited()) {
            // limit set, apply topN projection
            TopNProjection tnp = new TopNProjection(
                    firstNonNull(statement.limit(), Constants.DEFAULT_SELECT_LIMIT),
                    statement.offset(),
                    contextBuilder.orderBy(),
                    statement.orderBy().reverseFlags(),
                    statement.orderBy().nullsFirst()
            );
            tnp.outputs(contextBuilder.outputs());
            projectionBuilder.add(tnp);
        }
        if (indexWriterProjection != null && statement.isLimited()) {
            // limit set, context projection (index writer) will run on handler
            projectionBuilder.add(indexWriterProjection);
        } else if (indexWriterProjection != null && !statement.isLimited()) {
            // no limit -> no topN projection, use aggregation projection to merge node results
            projectionBuilder.add(localMergeProjection(functions));
        }
        MergeNode localMergeNode = PlanNodeBuilder.localMerge(projectionBuilder.build(),collectNode);
        return new QueryAndFetchNode(
                collectNode,
                localMergeNode
        );
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
