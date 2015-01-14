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

import io.crate.analyze.AnalysisMetaData;
import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.RelationVisitor;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.validator.SortSymbolValidator;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.analyze.where.WhereClauseContext;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.QueryThenFetchNode;
import io.crate.planner.symbol.Symbol;

public class QueryThenFetchConsumer implements Consumer {

    private final Visitor visitor;

    public QueryThenFetchConsumer(AnalysisMetaData analysisMetaData) {
        visitor = new Visitor(analysisMetaData);
    }

    @Override
    public boolean consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        PlannedAnalyzedRelation plannedAnalyzedRelation = visitor.process(rootRelation, context);
        if (plannedAnalyzedRelation == null) {
            return false;
        }
        context.rootRelation(plannedAnalyzedRelation);
        return true;
    }

    private static class Visitor extends RelationVisitor<ConsumerContext, PlannedAnalyzedRelation> {

        private final AnalysisMetaData analysisMetaData;

        public Visitor(AnalysisMetaData analysisMetaData) {
            this.analysisMetaData = analysisMetaData;
        }

        @Override
        public PlannedAnalyzedRelation visitSelectAnalyzedStatement(SelectAnalyzedStatement statement, ConsumerContext context) {
            if (statement.hasAggregates() || statement.hasGroupBy() || statement.hasSysExpressions()) {
                return null;
            }
            TableRelation tableRelation = ConsumingPlanner.getSingleTableRelation(statement.sources());
            if (tableRelation == null) {
                return null;
            }
            TableInfo tableInfo = tableRelation.tableInfo();
            if (tableInfo.schemaInfo().systemSchema() || tableInfo.rowGranularity() != RowGranularity.DOC) {
                return null;
            }
            WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(analysisMetaData, tableRelation);
            WhereClauseContext whereClauseContext = whereClauseAnalyzer.analyze(statement.whereClause());

            if(whereClauseContext.whereClause().version().isPresent()){
                context.validationException(new VersionInvalidException());
                return null;
            }

            for (Symbol symbol : statement.orderBy().orderBySymbols()) {
                /**
                 * this verifies that there are no partitioned by columns in the ORDER BY clause.
                 * TODO: instead of throwing errors this should be added as warnings/info to the ConsumerContext
                 * to indicate why this Consumer can't produce a plan node
                 */
                SortSymbolValidator.validate(symbol, tableInfo.partitionedBy());
            }
            return new QueryThenFetchNode(
                    tableInfo.getRouting(whereClauseContext.whereClause(), null),
                    tableRelation.resolve(statement.outputSymbols()),
                    tableRelation.resolve(statement.orderBy().orderBySymbols()),
                    statement.orderBy().reverseFlags(),
                    statement.orderBy().nullsFirst(),
                    statement.limit(),
                    statement.offset(),
                    whereClauseContext.whereClause(),
                    tableInfo.partitionedByColumns()
            );
        }

        @Override
        protected PlannedAnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, ConsumerContext context) {
            return null;
        }
    }
}
