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
import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.RelationVisitor;
import io.crate.analyze.relations.TableRelation;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.PlannerContextBuilder;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.GlobalAggregateNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.AggregationProjection;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.tree.QualifiedName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GlobalAggregateConsumer implements Consumer {

    private final Visitor visitor;

    public GlobalAggregateConsumer() {
        visitor = new Visitor();
    }

    @Override
    public boolean consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        AnalyzedRelation analyzedRelation = visitor.process(rootRelation, null);
        if (analyzedRelation != null) {
            context.rootRelation(analyzedRelation);
            return true;
        }
        return false;
    }

    private static class Visitor extends RelationVisitor<Void, PlannedAnalyzedRelation> {

        @Override
        public PlannedAnalyzedRelation visitSelectAnalyzedStatement(SelectAnalyzedStatement selectAnalyzedStatement, Void context) {
            if (selectAnalyzedStatement.hasGroupBy() || !selectAnalyzedStatement.hasAggregates()) {
                return null;
            }
            Map<QualifiedName, AnalyzedRelation> sources = selectAnalyzedStatement.sources();
            if (sources.size() != 1) {
                return null;
            }
            AnalyzedRelation sourceRelation = Iterables.getOnlyElement(sources.values());
            if (!(sourceRelation instanceof TableRelation)) {
                return null;
            }

            TableRelation tableRelation = (TableRelation) sourceRelation;
            PlannerContextBuilder contextBuilder = new PlannerContextBuilder(2)
                    .output(tableRelation.normalizeOutputs(selectAnalyzedStatement.outputSymbols()));

            Symbol havingClause = null;
            Symbol having = selectAnalyzedStatement.havingClause();
            if (having != null && having instanceof Function) {
                havingClause = contextBuilder.having(tableRelation.normalize(having));
            }

            AggregationProjection aggregationProjection = new AggregationProjection(contextBuilder.aggregations());
            CollectNode collectNode = PlanNodeBuilder.collect(
                    tableRelation.tableInfo(),
                    tableRelation.normalizeWhereClause(selectAnalyzedStatement.whereClause()),
                    contextBuilder.toCollect(),
                    ImmutableList.<Projection>of(aggregationProjection)
            );
            contextBuilder.nextStep();

            List<Projection> handlerProjections = new ArrayList<>();
            handlerProjections.add(new AggregationProjection(contextBuilder.aggregations()));

            if (havingClause != null) {
                FilterProjection fp = new FilterProjection((Function) havingClause);
                fp.outputs(contextBuilder.passThroughOutputs());
                handlerProjections.add(fp);
            }

            if (contextBuilder.aggregationsWrappedInScalar || havingClause != null) {
                // will filter out optional having symbols which are not selected
                TopNProjection topNProjection = new TopNProjection(1, 0);
                topNProjection.outputs(contextBuilder.outputs());
                handlerProjections.add(topNProjection);
            }
            MergeNode mergeNode = PlanNodeBuilder.localMerge(handlerProjections, collectNode);
            return new GlobalAggregateNode(collectNode, mergeNode);
        }
    }
}
