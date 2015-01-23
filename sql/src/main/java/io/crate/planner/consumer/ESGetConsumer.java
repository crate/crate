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

import io.crate.analyze.AnalysisMetaData;
import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.analyze.where.WhereClauseContext;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.ESGetNode;

public class ESGetConsumer implements Consumer {

    private final Visitor visitor;

    public ESGetConsumer(AnalysisMetaData analysisMetaData) {
        this.visitor = new Visitor(analysisMetaData);
    }

    @Override
    public boolean consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        PlannedAnalyzedRelation relation = visitor.process(rootRelation, context);
        if (relation == null) {
            return false;
        }
        context.rootRelation(relation);
        return true;
    }

    private static class Visitor extends AnalyzedRelationVisitor<ConsumerContext, PlannedAnalyzedRelation> {

        private final AnalysisMetaData analysisMetaData;

        public Visitor(AnalysisMetaData analysisMetaData) {
            this.analysisMetaData = analysisMetaData;
        }

        @Override
        public PlannedAnalyzedRelation visitSelectAnalyzedStatement(SelectAnalyzedStatement statement, ConsumerContext context) {
            if (statement.querySpec().hasAggregates() || statement.querySpec().groupBy()!=null) {
                return null;
            }
            TableRelation tableRelation = ConsumingPlanner.getSingleTableRelation(statement.sources());
            if (tableRelation == null) {
                return null;
            }
            TableInfo tableInfo = tableRelation.tableInfo();
            if (tableInfo.isAlias()
                    || tableInfo.schemaInfo().systemSchema()
                    || tableInfo.rowGranularity() != RowGranularity.DOC) {
                return null;
            }

            WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(analysisMetaData, tableRelation);
            WhereClauseContext whereClauseContext = whereClauseAnalyzer.analyze(statement.querySpec().where());

            if(whereClauseContext.whereClause().version().isPresent()){
                context.validationException(new VersionInvalidException());
                return null;
            }

            if (whereClauseContext.ids().size() == 0 || whereClauseContext.routingValues().size() == 0) {
                return null;
            }

            String indexName;
            if (tableInfo.isPartitioned()) {
                /**
                 * Currently the WhereClauseAnalyzer throws an Error if the table is partitioned and the
                 * query in the whereClause results in different queries for multiple partitions
                 * e.g.:   where (id = 1 and pcol = 'a') or (id = 2 and pcol = 'b')
                 *
                 * The assertion here is just a safety-net, because once the WhereClauseAnalyzer allows
                 * multiple different whereClauses for partitions the logic here would have to be changed.
                 */
                assert whereClauseContext.whereClause().partitions().size() == 1 : "Ambiguous partitions for ESGet";
                indexName = whereClauseContext.whereClause().partitions().get(0);
            } else {
                indexName = tableInfo.ident().name();
            }
            return new ESGetNode(
                    indexName,
                    tableRelation.resolve(statement.querySpec().outputs()),
                    whereClauseContext.ids(),
                    whereClauseContext.routingValues(),
                    tableRelation.resolveAndValidateOrderBy(statement.querySpec().orderBy()),
                    statement.querySpec().orderBy().reverseFlags(),
                    statement.querySpec().orderBy().nullsFirst(),
                    statement.querySpec().limit(),
                    statement.querySpec().offset(),
                    tableInfo.partitionedByColumns()
            );
        }

        @Override
        protected PlannedAnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, ConsumerContext context) {
            return null;
        }
    }
}
