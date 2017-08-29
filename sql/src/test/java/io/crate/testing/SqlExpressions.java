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

import io.crate.action.sql.SessionContext;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.FieldResolver;
import io.crate.analyze.relations.FullQualifiedNameFieldProvider;
import io.crate.analyze.relations.ParentRelations;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.metadata.Functions;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.predicate.PredicateModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.operation.tablefunctions.TableFunctionModule;
import io.crate.operation.user.User;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.QualifiedName;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;

public class SqlExpressions {

    private final ExpressionAnalyzer expressionAnalyzer;
    private final ExpressionAnalysisContext expressionAnalysisCtx;
    private final Injector injector;
    private final TransactionContext transactionContext;
    private final EvaluatingNormalizer normalizer;
    private final Functions functions;

    public SqlExpressions(Map<QualifiedName, AnalyzedRelation> sources) {
        this(sources, null, null, null);
    }

    public SqlExpressions(Map<QualifiedName, AnalyzedRelation> sources, Object[] parameters) {
        this(sources, null, parameters, null);
    }

    public SqlExpressions(Map<QualifiedName, AnalyzedRelation> sources,
                          @Nullable FieldResolver fieldResolver) {
        this(sources, fieldResolver, null, null);
    }

    public SqlExpressions(Map<QualifiedName, AnalyzedRelation> sources,
                          @Nullable FieldResolver fieldResolver,
                          @Nullable Object[] parameters,
                          User user,
                          AbstractModule... additionalModules) {
        ModulesBuilder modulesBuilder = new ModulesBuilder()
            .add(new OperatorModule())
            .add(new AggregationImplModule())
            .add(new ScalarFunctionModule())
            .add(new TableFunctionModule())
            .add(new PredicateModule());
        if (additionalModules != null) {
            for (AbstractModule module : additionalModules) {
                modulesBuilder.add(module);
            }
        }
        injector = modulesBuilder.createInjector();
        functions = injector.getInstance(Functions.class);
        SessionContext sessionContext = SessionContext.create(user);
        expressionAnalyzer = new ExpressionAnalyzer(
            functions,
            sessionContext,
            parameters == null
                ? ParamTypeHints.EMPTY
                : new ParameterContext(new RowN(parameters), Collections.<Row>emptyList()),
            new FullQualifiedNameFieldProvider(sources, ParentRelations.NO_PARENTS, sessionContext.defaultSchema()),
            null
        );
        normalizer = new EvaluatingNormalizer(functions, RowGranularity.DOC, null, fieldResolver);
        expressionAnalysisCtx = new ExpressionAnalysisContext();
        transactionContext = new TransactionContext(sessionContext);
    }

    public Symbol asSymbol(String expression) {
        return expressionAnalyzer.convert(SqlParser.createExpression(expression), expressionAnalysisCtx);
    }

    public Symbol normalize(Symbol symbol) {
        return normalizer.normalize(symbol, transactionContext);
    }

    public <T> T getInstance(Class<T> clazz) {
        return injector.getInstance(clazz);
    }

    public Functions functions() {
        return functions;
    }

    public void setDefaultSchema(String schema) {
        this.transactionContext.sessionContext().setDefaultSchema(schema);
    }
}
