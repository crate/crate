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
import io.crate.analyze.AnalysisMetaData;
import io.crate.analyze.InsertFromSubQueryAnalyzedStatement;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.node.dml.InsertFromSubQuery;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.AggregationProjection;
import io.crate.planner.projection.ColumnIndexWriterProjection;
import io.crate.planner.projection.Projection;
import org.elasticsearch.common.settings.ImmutableSettings;


public class InsertFromSubQueryConsumer implements Consumer {

    private final Visitor visitor;

    public InsertFromSubQueryConsumer(AnalysisMetaData analysisMetaData){
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
        boolean insertVisited = false;
        ColumnIndexWriterProjection indexWriterProjection;

        public Context(ConsumerContext context){
            this.consumerContext = context;
        }
    }

    private static class Visitor extends AnalyzedRelationVisitor<Context, AnalyzedRelation> {

        private final AnalysisMetaData analysisMetaData;

        public Visitor(AnalysisMetaData analysisMetaData){
            this.analysisMetaData = analysisMetaData;
        }

        @Override
        public AnalyzedRelation visitInsertFromQuery(InsertFromSubQueryAnalyzedStatement insertFromSubQueryAnalyzedStatement, Context context) {

            ColumnIndexWriterProjection indexWriterProjection = new ColumnIndexWriterProjection(
                    insertFromSubQueryAnalyzedStatement.tableInfo().ident().name(),
                    insertFromSubQueryAnalyzedStatement.tableInfo().primaryKey(),
                    insertFromSubQueryAnalyzedStatement.columns(),
                    insertFromSubQueryAnalyzedStatement.onDuplicateKeyAssignments(),
                    insertFromSubQueryAnalyzedStatement.primaryKeyColumnIndices(),
                    insertFromSubQueryAnalyzedStatement.partitionedByIndices(),
                    insertFromSubQueryAnalyzedStatement.routingColumn(),
                    insertFromSubQueryAnalyzedStatement.routingColumnIndex(),
                    ImmutableSettings.EMPTY,
                    insertFromSubQueryAnalyzedStatement.tableInfo().isPartitioned()
            );

            context.insertVisited = true;
            context.indexWriterProjection = indexWriterProjection;

            AnalyzedRelation innerRelation = insertFromSubQueryAnalyzedStatement.subQueryRelation();
            if (innerRelation instanceof PlannedAnalyzedRelation) {
                PlannedAnalyzedRelation analyzedRelation = (PlannedAnalyzedRelation)innerRelation;
                analyzedRelation.addProjection(indexWriterProjection);

                MergeNode mergeNode = null;
                if (analyzedRelation.resultIsDistributed()) {
                    // add local merge Node which aggregates the distributed results
                    AggregationProjection aggregationProjection = QueryAndFetchConsumer.localMergeProjection(this.analysisMetaData.functions());
                    mergeNode = PlanNodeBuilder.localMerge(ImmutableList.<Projection>of(aggregationProjection),
                                                           analyzedRelation.resultNode());
                }
                context.result = true;
                return new InsertFromSubQuery(((PlannedAnalyzedRelation) innerRelation).plan(), mergeNode);
            } else {
                return insertFromSubQueryAnalyzedStatement;
            }
        }

        @Override
        protected AnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, Context context) {
            return relation;
        }

    }

    public static <C, R> void planInnerRelation(InsertFromSubQueryAnalyzedStatement insertFromSubQueryAnalyzedStatement,
                                                C context, AnalyzedRelationVisitor<C,R> visitor) {
        if (insertFromSubQueryAnalyzedStatement.subQueryRelation() instanceof PlannedAnalyzedRelation) {
            // inner relation is already Planned
            return;
        }
        R innerRelation = visitor.process(insertFromSubQueryAnalyzedStatement.subQueryRelation(), context);
        if (innerRelation != null && innerRelation instanceof PlannedAnalyzedRelation) {
            insertFromSubQueryAnalyzedStatement.subQueryRelation((PlannedAnalyzedRelation)innerRelation);
        }
    }


}
