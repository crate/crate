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

import io.crate.analyze.QueriedTable;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.Routing;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.planner.Planner;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dql.CountNode;
import io.crate.planner.node.dql.CountPlan;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Collections;
import java.util.List;

import static com.google.common.base.MoreObjects.firstNonNull;

public class CountConsumer implements Consumer {

    private static final Visitor VISITOR = new Visitor();

    @Override
    public boolean consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        AnalyzedRelation analyzedRelation = VISITOR.process(rootRelation, context);
        if (analyzedRelation != null) {
            context.rootRelation(analyzedRelation);
            return true;
        }
        return false;
    }

    private static class Visitor extends AnalyzedRelationVisitor<ConsumerContext, PlannedAnalyzedRelation> {

        @Override
        public PlannedAnalyzedRelation visitQueriedTable(QueriedTable table, ConsumerContext context) {
            QuerySpec querySpec = table.querySpec();
            if (!querySpec.hasAggregates() || querySpec.groupBy()!=null) {
                return null;
            }
            TableInfo tableInfo = table.tableRelation().tableInfo();
            if (tableInfo.schemaInfo().systemSchema()) {
                return null;
            }
            if (!hasOnlyGlobalCount(querySpec.outputs())) {
                return null;
            }
            if(querySpec.where().hasVersions()){
                context.validationException(new VersionInvalidException());
                return null;
            }

            if (firstNonNull(querySpec.limit(), 1) < 1 ||
                    querySpec.offset() > 0){
                return new NoopPlannedAnalyzedRelation(table);
            }

            Routing routing = tableInfo.getRouting(querySpec.where(), null);
            Planner.Context plannerContext = context.plannerContext();
            CountNode countNode = new CountNode(plannerContext.nextExecutionNodeId(), routing, querySpec.where());
            MergeNode mergeNode = new MergeNode(
                    plannerContext.nextExecutionNodeId(),
                    "count-merge",
                    countNode.executionNodes().size());
            mergeNode.inputTypes(Collections.<DataType>singletonList(DataTypes.LONG));
            mergeNode.projections(Collections.<Projection>singletonList(
                    CountAggregation.PARTIAL_COUNT_AGGREGATION_PROJECTION));
            return new CountPlan(countNode, mergeNode);
        }

        @Override
        protected PlannedAnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, ConsumerContext context) {
            return null;
        }

        private boolean hasOnlyGlobalCount(List<Symbol> symbols) {
            if (symbols.size() != 1) {
                return false;
            }
            Symbol symbol = symbols.get(0);
            if (!(symbol instanceof Function)) {
                return false;
            }
            Function function = (Function) symbol;
            return function.info().equals(CountAggregation.COUNT_STAR_FUNCTION);
        }
    }
}
