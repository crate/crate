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
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.RelationAnalysisContext;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.analyze.relations.Relations;
import io.crate.sql.tree.DefaultTraversalVisitor;
import io.crate.sql.tree.Delete;

public class DeleteStatementAnalyzer extends DefaultTraversalVisitor<AnalyzedStatement, Void> {

    final DefaultTraversalVisitor<Void, InnerAnalysisContext> innerAnalyzer =
        new DefaultTraversalVisitor<Void, InnerAnalysisContext>() {

            @Override
            public Void visitDelete(Delete node, InnerAnalysisContext context) {
                context.deleteAnalyzedStatement.whereClauses.add(
                        context.expressionAnalyzer.generateWhereClause(node.getWhere(), context.expressionAnalysisContext));
                return null;
            }
    };

    private AnalysisMetaData analysisMetaData;
    private ParameterContext parameterContext;

    public DeleteStatementAnalyzer(AnalysisMetaData analysisMetaData, ParameterContext parameterContext) {
        this.analysisMetaData = analysisMetaData;
        this.parameterContext = parameterContext;
    }

    private static class InnerAnalysisContext {
        ExpressionAnalyzer expressionAnalyzer;
        ExpressionAnalysisContext expressionAnalysisContext;
        DeleteAnalyzedStatement deleteAnalyzedStatement;

        public InnerAnalysisContext(ExpressionAnalyzer expressionAnalyzer,
                                    ExpressionAnalysisContext expressionAnalysisContext,
                                    DeleteAnalyzedStatement deleteAnalyzedStatement) {

            this.expressionAnalyzer = expressionAnalyzer;
            this.expressionAnalysisContext = expressionAnalysisContext;
            this.deleteAnalyzedStatement = deleteAnalyzedStatement;
        }
    }

    @Override
    public AnalyzedStatement visitDelete(Delete node, Void context) {
        int numNested = 1;

        RelationAnalyzer relationAnalyzer = new RelationAnalyzer(analysisMetaData);
        RelationAnalysisContext relationAnalysisContext = new RelationAnalysisContext();

        AnalyzedRelation analyzedRelation = relationAnalyzer.process(node.getRelation(), relationAnalysisContext);
        if (Relations.isReadOnly(analyzedRelation)) {
            throw new UnsupportedOperationException(String.format(
                    "relation \"%s\" is read-only and cannot be deleted", analyzedRelation));
        }

        DeleteAnalyzedStatement deleteAnalyzedStatement = new DeleteAnalyzedStatement(parameterContext, analyzedRelation);
        InnerAnalysisContext innerAnalysisContext = new InnerAnalysisContext(
                new ExpressionAnalyzer(analysisMetaData, parameterContext, relationAnalysisContext.sources(), false),
                new ExpressionAnalysisContext(),
                deleteAnalyzedStatement
        );

        if (parameterContext.hasBulkParams()) {
            numNested = parameterContext.bulkParameters.length;
        }
        for (int i = 0; i < numNested; i++) {
            parameterContext.setBulkIdx(i);
            innerAnalyzer.process(node, innerAnalysisContext);
        }
        return deleteAnalyzedStatement;
    }
}
