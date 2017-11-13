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
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.FullQualifiedNameFieldProvider;
import io.crate.analyze.relations.RelationAnalysisContext;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.analyze.relations.StatementAnalysisContext;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.Functions;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.Delete;
import io.crate.sql.tree.ParameterExpression;

import java.util.function.Function;

class DeleteAnalyzer {

    private static final String VERSION_SEARCH_EX_MSG =
        "_version is not allowed in delete queries without specifying a primary key";
    private static final UnsupportedFeatureException VERSION_SEARCH_EX = new UnsupportedFeatureException(
        VERSION_SEARCH_EX_MSG);

    private final Functions functions;
    private RelationAnalyzer relationAnalyzer;

    DeleteAnalyzer(Functions functions, RelationAnalyzer relationAnalyzer) {
        this.functions = functions;
        this.relationAnalyzer = relationAnalyzer;
    }

    public AnalyzedDeleteStatement analyze(Delete delete, ParamTypeHints typeHints, TransactionContext txnContext) {
        StatementAnalysisContext stmtCtx = new StatementAnalysisContext(typeHints, Operation.DELETE, txnContext);
        RelationAnalysisContext relationCtx = stmtCtx.startRelation();
        AnalyzedRelation relation = relationAnalyzer.analyze(delete.getRelation(), stmtCtx);
        stmtCtx.endRelation();

        if (!(relation instanceof DocTableRelation)) {
            throw new UnsupportedOperationException("Cannot delete from relations other than base tables");
        }
        DocTableRelation table = (DocTableRelation) relation;
        EvaluatingNormalizer normalizer = new EvaluatingNormalizer(functions, RowGranularity.CLUSTER, null, table);
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            functions,
            txnContext,
            typeHints,
            new FullQualifiedNameFieldProvider(
                relationCtx.sources(),
                relationCtx.parentSources(),
                txnContext.sessionContext().defaultSchema()
            ),
            null
        );
        Symbol query = normalizer.normalize(
            expressionAnalyzer.generateQuerySymbol(delete.getWhere(), new ExpressionAnalysisContext()),
            txnContext
        );
        return new AnalyzedDeleteStatement(table, query);
    }

    /**
     * @deprecated This analyze variant uses the parameters and is bulk aware.
     *             Use {@link #analyze(Delete, ParamTypeHints, TransactionContext)} instead
     */
    @Deprecated
    public AnalyzedStatement analyze(Delete node, Analysis analysis) {
        int numNested = 1;

        Function<ParameterExpression, Symbol> convertParamFunction = analysis.parameterContext();
        TransactionContext transactionContext = analysis.transactionContext();
        StatementAnalysisContext statementAnalysisContext = new StatementAnalysisContext(
            convertParamFunction,
            Operation.DELETE,
            transactionContext);
        RelationAnalysisContext relationAnalysisContext = statementAnalysisContext.startRelation();
        AnalyzedRelation relation = relationAnalyzer.analyze(node.getRelation(), statementAnalysisContext);
        if (!(relation instanceof DocTableRelation)) {
            throw new UnsupportedOperationException("DELETE only works on base-table relations");
        }
        DocTableRelation table = (DocTableRelation) relation;
        EvaluatingNormalizer normalizer = new EvaluatingNormalizer(functions, RowGranularity.CLUSTER, null, table);
        DeleteAnalyzedStatement deleteAnalyzedStatement = new DeleteAnalyzedStatement(table);
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            functions,
            transactionContext,
            convertParamFunction,
            new FullQualifiedNameFieldProvider(
                relationAnalysisContext.sources(),
                relationAnalysisContext.parentSources(),
                transactionContext.sessionContext().defaultSchema()),
            null);
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext();
        WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(functions, table);

        if (analysis.parameterContext().hasBulkParams()) {
            numNested = analysis.parameterContext().numBulkParams();
        }
        for (int i = 0; i < numNested; i++) {
            analysis.parameterContext().setBulkIdx(i);
            Symbol query = expressionAnalyzer.generateQuerySymbol(node.getWhere(), expressionAnalysisContext);
            Symbol normalizedQuery = normalizer.normalize(query, transactionContext);
            WhereClause whereClause = validate(whereClauseAnalyzer.analyze(normalizedQuery, transactionContext));
            deleteAnalyzedStatement.whereClauses.add(whereClause);
        }

        statementAnalysisContext.endRelation();
        return deleteAnalyzedStatement;
    }

    private WhereClause validate(WhereClause whereClause) {
        if (!whereClause.docKeys().isPresent() && Symbols.containsColumn(whereClause.query(), DocSysColumns.VERSION)) {
            throw VERSION_SEARCH_EX;
        }
        return whereClause;
    }
}
