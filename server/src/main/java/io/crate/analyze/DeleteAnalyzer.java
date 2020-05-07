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
import io.crate.analyze.expressions.SubqueryAnalyzer;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.FullQualifiedNameFieldProvider;
import io.crate.analyze.relations.RelationAnalysisContext;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.analyze.relations.StatementAnalysisContext;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.Delete;

import java.util.Objects;

final class DeleteAnalyzer {

    private final Functions functions;
    private final RelationAnalyzer relationAnalyzer;

    DeleteAnalyzer(Functions functions, RelationAnalyzer relationAnalyzer) {
        this.functions = functions;
        this.relationAnalyzer = relationAnalyzer;
    }

    public AnalyzedDeleteStatement analyze(Delete delete, ParamTypeHints typeHints, CoordinatorTxnCtx txnContext) {
        StatementAnalysisContext stmtCtx = new StatementAnalysisContext(typeHints, Operation.DELETE, txnContext);
        final RelationAnalysisContext relationCtx = stmtCtx.startRelation();
        AnalyzedRelation relation = relationAnalyzer.analyze(delete.getRelation(), stmtCtx);
        stmtCtx.endRelation();

        MaybeAliasedStatement maybeAliasedStatement = MaybeAliasedStatement.analyze(relation);
        relation = maybeAliasedStatement.nonAliasedRelation();

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
                txnContext.sessionContext().searchPath().currentSchema()
            ),
            new SubqueryAnalyzer(relationAnalyzer, new StatementAnalysisContext(typeHints, Operation.READ, txnContext))
        );
        Symbol query = Objects.requireNonNullElse(
            expressionAnalyzer.generateQuerySymbol(delete.getWhere(), new ExpressionAnalysisContext()),
            Literal.BOOLEAN_TRUE
        );
        query = maybeAliasedStatement.maybeMapFields(query);

        Symbol normalizedQuery = normalizer.normalize(query, txnContext);
        return new AnalyzedDeleteStatement(table, normalizedQuery);
    }
}
