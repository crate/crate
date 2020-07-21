/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.testing;

import io.crate.action.sql.Option;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.SubqueryAnalyzer;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.FieldResolver;
import io.crate.analyze.relations.FullQualifiedNameFieldProvider;
import io.crate.analyze.relations.ParentRelations;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.analyze.relations.StatementAnalysisContext;
import io.crate.auth.user.User;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.table.Operation;
import io.crate.sql.parser.SqlParser;
import org.elasticsearch.common.inject.AbstractModule;

import javax.annotation.Nullable;
import java.util.Map;

import static io.crate.testing.TestingHelpers.createNodeContext;

import static org.mockito.Mockito.mock;

public class SqlExpressions {

    private final ExpressionAnalyzer expressionAnalyzer;
    private final ExpressionAnalysisContext expressionAnalysisCtx;
    private final CoordinatorTxnCtx coordinatorTxnCtx;
    private final EvaluatingNormalizer normalizer;
    public final NodeContext nodeCtx;

    public SqlExpressions(Map<RelationName, AnalyzedRelation> sources, AbstractModule... additionalModules) {
        this(sources, null, User.CRATE_USER, additionalModules);
    }

    public SqlExpressions(Map<RelationName, AnalyzedRelation> sources,
                          @Nullable FieldResolver fieldResolver,
                          AbstractModule... additionalModules) {
        this(sources, fieldResolver, User.CRATE_USER, additionalModules);
    }

    public SqlExpressions(Map<RelationName, AnalyzedRelation> sources,
                          @Nullable FieldResolver fieldResolver,
                          User user,
                          AbstractModule... additionalModules) {
        this.nodeCtx = createNodeContext(additionalModules);
        coordinatorTxnCtx = new CoordinatorTxnCtx(new SessionContext(Option.NONE, user));
        expressionAnalyzer = new ExpressionAnalyzer(
            coordinatorTxnCtx,
            nodeCtx,
            ParamTypeHints.EMPTY,
            new FullQualifiedNameFieldProvider(
                sources,
                ParentRelations.NO_PARENTS,
                coordinatorTxnCtx.sessionContext().searchPath().currentSchema()),
            new SubqueryAnalyzer(
                new RelationAnalyzer(nodeCtx, mock(Schemas.class)),
                new StatementAnalysisContext(ParamTypeHints.EMPTY, Operation.READ, coordinatorTxnCtx)
            )
        );
        normalizer = new EvaluatingNormalizer(nodeCtx, RowGranularity.DOC, null, fieldResolver);
        expressionAnalysisCtx = new ExpressionAnalysisContext();
    }

    public Symbol asSymbol(String expression) {
        return expressionAnalyzer.convert(SqlParser.createExpression(expression), expressionAnalysisCtx);
    }

    public ExpressionAnalysisContext context() {
        return expressionAnalysisCtx;
    }

    public Symbol normalize(Symbol symbol) {
        return normalizer.normalize(symbol, coordinatorTxnCtx);
    }

    public void setDefaultSchema(String schema) {
        this.coordinatorTxnCtx.sessionContext().setSearchPath(schema);
    }

    public void setSearchPath(String... schemas) {
        this.coordinatorTxnCtx.sessionContext().setSearchPath(schemas);
    }
}
