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
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.QualifiedName;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.mockito.Mockito.mock;

/**
 * Additional tests for the ExpressionAnalyzer.
 * Most of the remaining stuff is tested via {@link io.crate.analyze.SelectStatementAnalyzerTest} and other *AnalyzerTest classes.
 */
public class ExpressionAnalyzerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private AnalysisMetaData analysisMetaData;
    private ParameterContext emptyParameterContext;
    private ImmutableMap<QualifiedName, AnalyzedRelation> dummySources;

    @Before
    public void setUp() throws Exception {
        analysisMetaData = mock(AnalysisMetaData.class);
        emptyParameterContext = new ParameterContext(new Object[0], new Object[0][]);
        dummySources = ImmutableMap.of(new QualifiedName("foo"), mock(AnalyzedRelation.class));
    }

    @Test
    public void testUnsupportedExpressionNullIf() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Unsupported expression NULLIF(1, 3)");
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(analysisMetaData, emptyParameterContext, new FullQualifedNameFieldProvider(dummySources));
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext();

        expressionAnalyzer.convert(SqlParser.createExpression("NULLIF ( 1 , 3 )"), expressionAnalysisContext);
    }

    @Test
    public void testUnsupportedExpressionCurrentDate() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Unsupported expression current_time");
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(analysisMetaData, emptyParameterContext, new FullQualifedNameFieldProvider(dummySources));
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext();

        expressionAnalyzer.convert(SqlParser.createExpression("current_time"), expressionAnalysisContext);
    }
}