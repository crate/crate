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
import io.crate.analyze.QueriedTable;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dql.ESGetNode;

public class ESGetConsumer implements Consumer {

    private static final Visitor VISITOR = new Visitor();

    @Override
    public PlannedAnalyzedRelation consume(AnalyzedRelation relation, ConsumerContext context) {
        return VISITOR.process(relation, context);
    }

    private static class Visitor extends AnalyzedRelationVisitor<ConsumerContext, PlannedAnalyzedRelation> {

        @Override
        public PlannedAnalyzedRelation visitQueriedTable(QueriedTable table, ConsumerContext context) {
            if (context.rootRelation() != table) {
                return null;
            }

            if (table.querySpec().hasAggregates() || table.querySpec().groupBy() != null) {
                return null;
            }
            TableInfo tableInfo = table.tableRelation().tableInfo();
            if (tableInfo.isAlias()
                    || tableInfo.schemaInfo().systemSchema()
                    || tableInfo.rowGranularity() != RowGranularity.DOC) {
                return null;
            }

            if (!table.querySpec().where().docKeys().isPresent()) {
                return null;
            }

            if(table.querySpec().where().docKeys().get().withVersions()){
                context.validationException(new VersionInvalidException());
                return null;
            }
            Integer limit = table.querySpec().limit();
            if (limit != null){
                if (limit == 0){
                    return new NoopPlannedAnalyzedRelation(table);
                }
            }

            OrderBy orderBy = table.querySpec().orderBy();
            if (orderBy != null){
                table.tableRelation().validateOrderBy(orderBy);
            }
            return new ESGetNode(context.plannerContext().nextExecutionNodeId(), tableInfo, table.querySpec());
        }

        @Override
        protected PlannedAnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, ConsumerContext context) {
            return null;
        }
    }
}
