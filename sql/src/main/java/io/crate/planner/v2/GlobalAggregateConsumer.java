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
import io.crate.analyze.AnalysisMetaData;
import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.RelationVisitor;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.analyze.where.WhereClauseContext;
import io.crate.exceptions.VersionInvalidException;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.PlannerContextBuilder;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.GlobalAggregateNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.*;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Symbol;

import java.util.ArrayList;
import java.util.List;

import static io.crate.planner.symbol.Field.unwrap;

public class GlobalAggregateConsumer implements Consumer {

    private final Visitor visitor;

    public GlobalAggregateConsumer(AnalysisMetaData analysisMetaData) {
        visitor = new Visitor(analysisMetaData);
    }

    @Override
    public boolean consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        AnalyzedRelation analyzedRelation = visitor.process(rootRelation, context);
        if (analyzedRelation != null) {
            context.rootRelation(analyzedRelation);
            return true;
        }
        return false;
    }

    private static class Visitor extends RelationVisitor<ConsumerContext, PlannedAnalyzedRelation> {

        private final AnalysisMetaData analysisMetaData;

        public Visitor(AnalysisMetaData analysisMetaData){
            this.analysisMetaData = analysisMetaData;
        }

        @Override
        public PlannedAnalyzedRelation visitSelectAnalyzedStatement(SelectAnalyzedStatement statement, ConsumerContext context) {
            if (statement.hasGroupBy() || !statement.hasAggregates()) {
                return null;
            }
            TableRelation tableRelation = ConsumingPlanner.getSingleTableRelation(statement.sources());

            WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(analysisMetaData, tableRelation);
            WhereClauseContext whereClauseContext = whereClauseAnalyzer.analyze(statement.whereClause());
            WhereClause whereClause = whereClauseContext.whereClause();
            if(whereClause.version().isPresent()){
                context.validationException(new VersionInvalidException());
                return null;
            }
            return globalAggregates(statement, null);
        }

        @Override
        protected PlannedAnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, ConsumerContext context) {
            return null;
        }
    }

    public static PlannedAnalyzedRelation globalAggregates(SelectAnalyzedStatement statement, ColumnIndexWriterProjection indexWriterProjection){
        TableRelation tableRelation = ConsumingPlanner.getSingleTableRelation(statement.sources());
        if (tableRelation == null) {
            return null;
        }
        // global aggregate: collect and partial aggregate on C and final agg on H
        PlannerContextBuilder contextBuilder = new PlannerContextBuilder(2).output(unwrap(statement.outputSymbols()));

        // havingClause could be a Literal or Function.
        // if its a Literal and value is false, we'll never reach this point (no match),
        // otherwise (true value) having can be ignored
        Symbol havingClause = null;
        Symbol having = statement.havingClause();
        if (having != null && having instanceof Function) {
            havingClause = contextBuilder.having(tableRelation.resolve(having));
        }

        AggregationProjection ap = new AggregationProjection();
        ap.aggregations(contextBuilder.aggregations());
        CollectNode collectNode = PlanNodeBuilder.collect(
                tableRelation.tableInfo(),
                tableRelation.resolve(statement.whereClause()),
                contextBuilder.toCollect(),
                ImmutableList.<Projection>of(ap)
        );
        contextBuilder.nextStep();

        //// the handler stuff
        List<Projection> projections = new ArrayList<>();
        projections.add(new AggregationProjection(contextBuilder.aggregations()));

        if (havingClause != null) {
            FilterProjection fp = new FilterProjection((Function)havingClause);
            fp.outputs(contextBuilder.passThroughOutputs());
            projections.add(fp);
        }

        if (contextBuilder.aggregationsWrappedInScalar || havingClause != null) {
            // will filter out optional having symbols which are not selected
            TopNProjection topNProjection = new TopNProjection(1, 0);
            topNProjection.outputs(contextBuilder.outputs());
            projections.add(topNProjection);
        }
        if (indexWriterProjection != null) {
            projections.add(indexWriterProjection);
        }
        MergeNode localMergeNode = PlanNodeBuilder.localMerge(projections, collectNode);
        return new GlobalAggregateNode(collectNode, localMergeNode);
    }
}
