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

import io.crate.analyze.AnalysisMetaData;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.FullQualifedNameFieldProvider;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.*;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.predicate.PredicateModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.QualifiedName;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;

import java.util.Map;

import static org.mockito.Mockito.mock;

public class SqlExpressions {

    private final ExpressionAnalyzer expressionAnalyzer;
    private final ExpressionAnalysisContext expressionAnalysisCtx;
    private final Injector injector;
    private final AnalysisMetaData analysisMetaData;

    public SqlExpressions(Map<QualifiedName, AnalyzedRelation> sources) {
        ModulesBuilder modulesBuilder = new ModulesBuilder()
                .add(new OperatorModule())
                .add(new ScalarFunctionModule())
                .add(new PredicateModule());
        injector = modulesBuilder.createInjector();
        NestedReferenceResolver referenceResolver = new NestedReferenceResolver() {
            @Override
            public ReferenceImplementation<?> getImplementation(ReferenceInfo refInfo) {
                throw new UnsupportedOperationException("getImplementation not implemented");
            }
        };
        Schemas schemas = mock(Schemas.class);
        analysisMetaData = new AnalysisMetaData(injector.getInstance(Functions.class), schemas, referenceResolver);
        expressionAnalyzer =  new ExpressionAnalyzer(
                analysisMetaData,
                new ParameterContext(new Object[0], new Object[0][], null),
                new FullQualifedNameFieldProvider(sources),
                null);
        expressionAnalysisCtx = new ExpressionAnalysisContext();
    }

    public Symbol asSymbol(String expression) {
        return expressionAnalyzer.convert(SqlParser.createExpression(expression), expressionAnalysisCtx);
    }

    public Symbol normalize(Symbol symbol) {
        return expressionAnalyzer.normalize(symbol);
    }

    public <T> T getInstance(Class<T> clazz) {
        return injector.getInstance(clazz);
    }

    public AnalysisMetaData analysisMD() {
        return analysisMetaData;
    }
}
