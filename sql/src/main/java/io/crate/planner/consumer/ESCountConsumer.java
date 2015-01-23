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
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.analyze.where.WhereClauseContext;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.planner.Planner;
import io.crate.planner.node.dql.ESCountNode;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Symbol;

import java.util.List;

public class ESCountConsumer implements Consumer {

    private final Visitor visitor;

    public ESCountConsumer(AnalysisMetaData analysisMetaData) {
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

    private static class Visitor extends AnalyzedRelationVisitor<ConsumerContext, PlannedAnalyzedRelation> {

        private final AnalysisMetaData analysisMetaData;

        public Visitor(AnalysisMetaData analysisMetaData) {
            this.analysisMetaData = analysisMetaData;
        }

        @Override
        public PlannedAnalyzedRelation visitSelectAnalyzedStatement(SelectAnalyzedStatement selectAnalyzedStatement, ConsumerContext context) {
            if (!selectAnalyzedStatement.querySpec().hasAggregates() || selectAnalyzedStatement.querySpec().groupBy()!=null) {
                return null;
            }
            if (selectAnalyzedStatement.hasSysExpressions()) {
                return null;
            }
            TableRelation tableRelation = ConsumingPlanner.getSingleTableRelation(selectAnalyzedStatement.sources());
            if (tableRelation == null) {
                return null;
            }
            TableInfo tableInfo = tableRelation.tableInfo();
            if (tableInfo.schemaInfo().systemSchema()) {
                return null;
            }
            if (!hasOnlyGlobalCount(selectAnalyzedStatement.querySpec().outputs())) {
                return null;
            }
            WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(analysisMetaData, tableRelation);
            WhereClauseContext whereClauseContext = whereClauseAnalyzer.analyze(selectAnalyzedStatement.querySpec().where());
            if(whereClauseContext.whereClause().version().isPresent()){
                context.validationException(new VersionInvalidException());
                return null;
            }
            return new ESCountNode(Planner.indices(tableInfo, whereClauseContext.whereClause()),
                    whereClauseContext.whereClause());
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
