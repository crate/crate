/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.analyze;

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.*;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.sql.tree.DefaultTraversalVisitor;
import io.crate.sql.tree.Delete;
import io.crate.sql.tree.Node;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

@Singleton
public class DeleteStatementAnalyzer extends DefaultTraversalVisitor<AnalyzedStatement, Analysis> {

    static final DefaultTraversalVisitor<Void, InnerAnalysisContext> innerAnalyzer =
        new DefaultTraversalVisitor<Void, InnerAnalysisContext>() {
            @Override
            public Void visitDelete(Delete node, InnerAnalysisContext context) {
                context.deleteAnalyzedStatement.whereClauses.add(
                        context.whereClauseAnalyzer.analyze(
                                context.expressionAnalyzer.generateWhereClause(node.getWhere(),
                                        context.expressionAnalysisContext, context.tableRelation)));
                return null;
            }
    };

    private AnalysisMetaData analysisMetaData;
    private RelationAnalyzer relationAnalyzer;

    @Inject
    public DeleteStatementAnalyzer(AnalysisMetaData analysisMetaData,
                                   RelationAnalyzer relationAnalyzer) {
        this.analysisMetaData = analysisMetaData;
        this.relationAnalyzer = relationAnalyzer;
    }

    public AnalyzedStatement analyze(Node node, Analysis analysis) {
        analysis.expectsAffectedRows(true);
        return process(node, analysis);
    }

    private static class InnerAnalysisContext {
        private final ExpressionAnalyzer expressionAnalyzer;
        private final TableRelation tableRelation;
        private final ExpressionAnalysisContext expressionAnalysisContext;
        private final DeleteAnalyzedStatement deleteAnalyzedStatement;
        private final WhereClauseAnalyzer whereClauseAnalyzer;

        InnerAnalysisContext(TableRelation tableRelation,
                             ExpressionAnalyzer expressionAnalyzer,
                             ExpressionAnalysisContext expressionAnalysisContext,
                             DeleteAnalyzedStatement deleteAnalyzedStatement,
                             WhereClauseAnalyzer whereClauseAnalyzer
                             ) {
            this.expressionAnalyzer = expressionAnalyzer;
            this.tableRelation = tableRelation;
            this.expressionAnalysisContext = expressionAnalysisContext;
            this.deleteAnalyzedStatement = deleteAnalyzedStatement;
            this.whereClauseAnalyzer = whereClauseAnalyzer;
        }
    }

    @Override
    public AnalyzedStatement visitDelete(Delete node, Analysis context) {
        int numNested = 1;

        RelationAnalysisContext relationAnalysisContext = new RelationAnalysisContext(context.parameterContext());

        AnalyzedRelation analyzedRelation = relationAnalyzer.process(node.getRelation(), relationAnalysisContext);
        if (Relations.isReadOnly(analyzedRelation)) {
            throw new UnsupportedOperationException(String.format(
                    "relation \"%s\" is read-only and cannot be deleted", analyzedRelation));
        }
        assert analyzedRelation instanceof TableRelation;
        DeleteAnalyzedStatement deleteAnalyzedStatement = new DeleteAnalyzedStatement((TableRelation) analyzedRelation);
        InnerAnalysisContext innerAnalysisContext = new InnerAnalysisContext(
                deleteAnalyzedStatement.analyzedRelation(),
                new ExpressionAnalyzer(analysisMetaData, context.parameterContext(),
                        new FullQualifedNameFieldProvider(relationAnalysisContext.sources())),
                new ExpressionAnalysisContext(),
                deleteAnalyzedStatement,
                new WhereClauseAnalyzer(analysisMetaData, deleteAnalyzedStatement.analyzedRelation())
        );

        if (context.parameterContext().hasBulkParams()) {
            numNested = context.parameterContext().bulkParameters.length;
        }
        for (int i = 0; i < numNested; i++) {
            context.parameterContext().setBulkIdx(i);
            innerAnalyzer.process(node, innerAnalysisContext);
        }
        return deleteAnalyzedStatement;
    }
}
