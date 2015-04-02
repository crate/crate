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

package io.crate.analyze.expressions;


import com.google.common.collect.ImmutableMap;
import io.crate.analyze.AnalysisMetaData;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.FullQualifedNameFieldProvider;
import io.crate.analyze.relations.TableRelation;
import io.crate.metadata.*;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.operator.OperatorModule;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Field;
import io.crate.planner.symbol.Function;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Additional tests for the ExpressionAnalyzer.
 * Most of the remaining stuff is tested via {@link io.crate.analyze.SelectStatementAnalyzerTest} and other *AnalyzerTest classes.
 */
public class ExpressionAnalyzerTest extends CrateUnitTest {

    private AnalysisMetaData mockedAnalysisMetaData;
    private ParameterContext emptyParameterContext;
    private ImmutableMap<QualifiedName, AnalyzedRelation> dummySources;
    private ExpressionAnalysisContext context;
    private AnalysisMetaData analysisMetaData;

    @Before
    public void prepare() throws Exception {
        mockedAnalysisMetaData = mock(AnalysisMetaData.class);
        emptyParameterContext = new ParameterContext(new Object[0], new Object[0][], null);
        dummySources = ImmutableMap.of(new QualifiedName("foo"), mock(AnalyzedRelation.class));
        context = new ExpressionAnalysisContext();

        Injector injector = new ModulesBuilder()
                .add(new OperatorModule())
                .createInjector();

        analysisMetaData = new AnalysisMetaData(
                injector.getInstance(Functions.class),
                mock(ReferenceInfos.class),
                new ReferenceResolver() {
                    @Override
                    public ReferenceImplementation getImplementation(ReferenceIdent ident) {
                        return null;
                    }
                });
    }

    @Test
    public void testUnsupportedExpressionNullIf() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Unsupported expression NULLIF(1, 3)");
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(mockedAnalysisMetaData, emptyParameterContext, new FullQualifedNameFieldProvider(dummySources));
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext();

        expressionAnalyzer.convert(SqlParser.createExpression("NULLIF ( 1 , 3 )"), expressionAnalysisContext);
    }

    @Test
    public void testUnsupportedExpressionCurrentDate() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Unsupported expression current_time");
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(mockedAnalysisMetaData, emptyParameterContext, new FullQualifedNameFieldProvider(dummySources));
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext();

        expressionAnalyzer.convert(SqlParser.createExpression("current_time"), expressionAnalysisContext);
    }

    @Test
    public void testInSelfJoinCaseFunctionsThatLookTheSameMustNotReuseFunctionAllocation() throws Exception {
        TableInfo tableInfo = mock(TableInfo.class);
        when(tableInfo.getReferenceInfo(new ColumnIdent("id"))).thenReturn(
                new ReferenceInfo(new ReferenceIdent(new TableIdent("doc", "t"), "id"), RowGranularity.DOC, DataTypes.INTEGER));
        TableRelation tr1 = new TableRelation(tableInfo);
        TableRelation tr2 = new TableRelation(tableInfo);

        Map<QualifiedName, AnalyzedRelation> sources = ImmutableMap.<QualifiedName, AnalyzedRelation>of(
                new QualifiedName("t1"), tr1,
                new QualifiedName("t2"), tr2
        );
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
                analysisMetaData, emptyParameterContext, new FullQualifedNameFieldProvider(sources));
        Function andFunction = (Function)expressionAnalyzer.convert(
                SqlParser.createExpression("not t1.id = 1 and not t2.id = 1"), context);

        Field t1Id = ((Field) ((Function) ((Function) andFunction.arguments().get(0)).arguments().get(0)).arguments().get(0));
        Field t2Id = ((Field) ((Function) ((Function) andFunction.arguments().get(1)).arguments().get(0)).arguments().get(0));
        assertTrue(t1Id.relation() != t2Id.relation());
    }
}