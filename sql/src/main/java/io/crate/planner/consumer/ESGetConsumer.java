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


import io.crate.analyze.OrderBy;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dql.ESGetNode;

public class ESGetConsumer implements Consumer {

    private static final Visitor VISITOR = new Visitor();

    @Override
    public PlannedAnalyzedRelation consume(AnalyzedRelation relation, ConsumerContext context) {
        return VISITOR.process(relation, context);
    }

    private static class Visitor extends RelationPlanningVisitor {

        @Override
        public PlannedAnalyzedRelation visitQueriedDocTable(QueriedDocTable table, ConsumerContext context) {
            if (context.rootRelation() != table) {
                return null;
            }

            if (table.querySpec().hasAggregates()
                    || table.querySpec().groupBy() != null
                    || !table.querySpec().where().docKeys().isPresent()) {
                return null;
            }

            DocTableInfo tableInfo = table.tableRelation().tableInfo();
            if (tableInfo.isAlias()) {
                return null;
            }

            if(table.querySpec().where().docKeys().get().withVersions()){
                context.validationException(new VersionInvalidException());
                return null;
            }
            Integer limit = table.querySpec().limit();
            if (limit != null){
                if (limit == 0){
                    return new NoopPlannedAnalyzedRelation(table, context.plannerContext().jobId());
                }
            }

            OrderBy orderBy = table.querySpec().orderBy();
            if (orderBy != null){
                table.tableRelation().validateOrderBy(orderBy);
            }
            return new ESGetNode(context.plannerContext().nextExecutionPhaseId(), tableInfo, table.querySpec(), context.plannerContext().jobId());
        }
    }
}
