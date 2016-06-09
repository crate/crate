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
import io.crate.action.sql.FetchProperties;
import io.crate.action.sql.SQLBaseRequest;
import io.crate.analyze.AnalysisMetaData;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.FullQualifedNameFieldProvider;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.*;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static io.crate.testing.TestingHelpers.getFunctions;
import static io.crate.testing.TestingHelpers.isField;
import static org.hamcrest.Matchers.*;
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
        dummySources = ImmutableMap.of(new QualifiedName("foo"), (AnalyzedRelation) new DummyRelation());
        context = new ExpressionAnalysisContext();

        analysisMetaData = new AnalysisMetaData(
                getFunctions(),
                mock(ReferenceInfos.class),
                new NestedReferenceResolver() {
                    @Override
                    public ReferenceImplementation getImplementation(ReferenceInfo refInfo) {
                        return null;
                    }
                });
    }

    @Test
    public void testUnsupportedExpressionNullIf() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Unsupported expression NULLIF(1, 3)");
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
                mockedAnalysisMetaData, emptyParameterContext, new FullQualifedNameFieldProvider(dummySources), null);
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext();

        expressionAnalyzer.convert(SqlParser.createExpression("NULLIF ( 1 , 3 )"), expressionAnalysisContext);
    }

    @Test
    public void testUnsupportedExpressionCurrentDate() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Unsupported expression current_time");
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
                mockedAnalysisMetaData, emptyParameterContext, new FullQualifedNameFieldProvider(dummySources), null);
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext();

        expressionAnalyzer.convert(SqlParser.createExpression("current_time"), expressionAnalysisContext);
    }

    @Test
    public void testQuotedSubscriptExpression() throws Exception {
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
                mockedAnalysisMetaData,
                new ParameterContext(new Object[0], new Object[0][], null, SQLBaseRequest.HEADER_FLAG_ALLOW_QUOTED_SUBSCRIPT, FetchProperties.DEFAULT),
                new FullQualifedNameFieldProvider(dummySources),
                null);
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext();

        Field field1 = (Field) expressionAnalyzer.convert(SqlParser.createExpression("obj['x']"), expressionAnalysisContext);
        Field field2 = (Field) expressionAnalyzer.convert(SqlParser.createExpression("\"obj['x']\""), expressionAnalysisContext);
        assertEquals(field1, field2);

        Field field3 = (Field) expressionAnalyzer.convert(SqlParser.createExpression("\"myObj['x']\""), expressionAnalysisContext);
        assertEquals("myObj['x']", field3.path().toString());
        Field field4 = (Field) expressionAnalyzer.convert(SqlParser.createExpression("\"myObj['x']['AbC']\""), expressionAnalysisContext);
        assertEquals("myObj['x']['AbC']", field4.path().toString());
    }

    @Test
    public void testSubscriptSplitPatternMatcher() throws Exception {
        assertEquals("\"foo\".\"bar\"['x']['y']", ExpressionAnalyzer.getQuotedSubscriptLiteral("foo.bar['x']['y']"));
        assertEquals("\"foo\"['x']['y']", ExpressionAnalyzer.getQuotedSubscriptLiteral("foo['x']['y']"));
        assertEquals("\"foo\"['x']", ExpressionAnalyzer.getQuotedSubscriptLiteral("foo['x']"));
        assertEquals("\"myFoo\"['xY']", ExpressionAnalyzer.getQuotedSubscriptLiteral("myFoo['xY']"));

        assertNull(ExpressionAnalyzer.getQuotedSubscriptLiteral("foo"));
        assertNull(ExpressionAnalyzer.getQuotedSubscriptLiteral("foo.."));
        assertNull(ExpressionAnalyzer.getQuotedSubscriptLiteral(".foo."));
        assertNull(ExpressionAnalyzer.getQuotedSubscriptLiteral("foo.['x']"));
        assertNull(ExpressionAnalyzer.getQuotedSubscriptLiteral("obj"));
        assertNull(ExpressionAnalyzer.getQuotedSubscriptLiteral("obj.x"));
        assertNull(ExpressionAnalyzer.getQuotedSubscriptLiteral("obj[x][y]"));
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
                analysisMetaData, emptyParameterContext, new FullQualifedNameFieldProvider(sources), null);
        Function andFunction = (Function)expressionAnalyzer.convert(
                SqlParser.createExpression("not t1.id = 1 and not t2.id = 1"), context);

        Field t1Id = ((Field) ((Function) ((Function) andFunction.arguments().get(0)).arguments().get(0)).arguments().get(0));
        Field t2Id = ((Field) ((Function) ((Function) andFunction.arguments().get(1)).arguments().get(0)).arguments().get(0));
        assertTrue(t1Id.relation() != t2Id.relation());
    }

    @Test
    public void testSwapFunctionLeftSide() throws Exception {
        SqlExpressions expressions = new SqlExpressions(T3.SOURCES);
        Function cmp = (Function)expressions.normalize(expressions.asSymbol("8 + 5 > t1.x"));
        // the comparison was swapped so the field is on the left side
        assertThat(cmp.info().ident().name(), is("op_<"));
        assertThat(cmp.arguments().get(0), isField("x"));
    }

    @Test
    public void testNonDeterministicFunctionsAlwaysNew() throws Exception {
        ExpressionAnalysisContext localContext = new ExpressionAnalysisContext();
        FunctionInfo info1 = new FunctionInfo(
                new FunctionIdent("inc", Arrays.<DataType>asList(DataTypes.BOOLEAN)),
                DataTypes.INTEGER,
                FunctionInfo.Type.SCALAR,
                false,
                false
        );
        Function fn1 = localContext.allocateFunction(info1, Arrays.<Symbol>asList(Literal.BOOLEAN_FALSE));
        Function fn2 = localContext.allocateFunction(info1, Arrays.<Symbol>asList(Literal.BOOLEAN_FALSE));
        Function fn3 = localContext.allocateFunction(info1, Arrays.<Symbol>asList(Literal.BOOLEAN_TRUE));

        // different instances
        assertThat(fn1, allOf(
                not(sameInstance(fn2)),
                not(sameInstance(fn3))

        ));
        // but equal
        assertThat(fn1, is(equalTo(fn2)));
        assertThat(fn1, is(not(equalTo(fn3))));
    }

    private static class DummyRelation implements AnalyzedRelation {

        public final Set<ColumnIdent> supportedReference = new HashSet<>();

        public DummyRelation() {
            supportedReference.add(ColumnIdent.fromPath("obj.x"));
            supportedReference.add(ColumnIdent.fromPath("myObj.x"));
            supportedReference.add(ColumnIdent.fromPath("myObj.x.AbC"));
        }

        @Override
        public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
            return null;
        }

        @Override
        public Field getField(Path path) {
            ColumnIdent columnIdent = (ColumnIdent) path;
            if (supportedReference.contains(columnIdent)) {
                return new Field(this, columnIdent, DataTypes.STRING);
            }
            return null;
        }

        @Override
        public Field getField(Path path, Operation operation) throws UnsupportedOperationException {
            return getField(path);
        }

        @Override
        public List<Field> fields() {
            return null;
        }
    }
}
