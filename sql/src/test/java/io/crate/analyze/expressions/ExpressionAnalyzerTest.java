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


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.action.sql.Option;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.FullQualifiedNameFieldProvider;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.ArrayLiteral;
import io.crate.sql.tree.FunctionCall;
import io.crate.sql.tree.LongLiteral;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.StringLiteral;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.DummyRelation;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;
import io.crate.types.DataTypes;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;

import static io.crate.testing.SymbolMatchers.isField;
import static io.crate.testing.TestingHelpers.getFunctions;
import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Additional tests for the ExpressionAnalyzer.
 * Most of the remaining stuff is tested via {@link io.crate.analyze.SelectStatementAnalyzerTest} and other *AnalyzerTest classes.
 */
public class ExpressionAnalyzerTest extends CrateUnitTest {

    private ImmutableMap<QualifiedName, AnalyzedRelation> dummySources;
    private ExpressionAnalysisContext context;
    private ParamTypeHints paramTypeHints;
    private Functions functions;

    @Before
    public void prepare() throws Exception {
        paramTypeHints = ParamTypeHints.EMPTY;
        DummyRelation dummyRelation = new DummyRelation("obj.x", "myObj.x", "myObj.x.AbC");
        dummySources = ImmutableMap.of(new QualifiedName("foo"), dummyRelation);
        context = new ExpressionAnalysisContext();
        functions = getFunctions();
    }

    @Test
    public void testUnsupportedExpressionCurrentDate() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Unsupported expression current_time");
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            functions, SessionContext.SYSTEM_SESSION, paramTypeHints,
            new FullQualifiedNameFieldProvider(dummySources, Collections.emptyMap()),
            null);
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext();

        expressionAnalyzer.convert(SqlParser.createExpression("current_time"), expressionAnalysisContext);
    }

    @Test
    public void testQuotedSubscriptExpression() throws Exception {
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            functions,
            new SessionContext(0, EnumSet.of(Option.ALLOW_QUOTED_SUBSCRIPT), null, null, s -> {}, t -> {}),
            paramTypeHints,
            new FullQualifiedNameFieldProvider(dummySources, Collections.emptyMap()),
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
    public void testAnalyzeSubscriptFunctionCall() throws Exception {
        // Test when use subscript function is used explicitly then it's handled (and validated)
        // the same way it's handled when the subscript operator `[]` is used
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            functions,
            new SessionContext(0, EnumSet.of(Option.ALLOW_QUOTED_SUBSCRIPT), null, null, s -> {}, t -> {}),
            paramTypeHints,
            new FullQualifiedNameFieldProvider(dummySources, Collections.emptyMap()),
            null);
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext();
        FunctionCall subscriptFunctionCall = new FunctionCall(
            new QualifiedName("subscript"),
            ImmutableList.of(
                new ArrayLiteral(ImmutableList.of(new StringLiteral("obj"))),
                new LongLiteral("1")));

        Function function = (Function) expressionAnalyzer.convert(subscriptFunctionCall, expressionAnalysisContext);
        assertEquals("subscript(_array('obj'), 1)", function.toString());
    }

    @Test
    public void testInSelfJoinCaseFunctionsThatLookTheSameMustNotReuseFunctionAllocation() throws Exception {
        TableInfo tableInfo = mock(TableInfo.class);
        when(tableInfo.getReference(new ColumnIdent("id"))).thenReturn(
            new Reference(new ReferenceIdent(new TableIdent("doc", "t"), "id"), RowGranularity.DOC, DataTypes.INTEGER));
        when(tableInfo.ident()).thenReturn(new TableIdent("doc", "t"));
        TableRelation tr1 = new TableRelation(tableInfo);
        TableRelation tr2 = new TableRelation(tableInfo);

        Map<QualifiedName, AnalyzedRelation> sources = ImmutableMap.of(
            new QualifiedName("t1"), tr1,
            new QualifiedName("t2"), tr2
        );
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            functions,
            SessionContext.SYSTEM_SESSION,
            paramTypeHints,
            new FullQualifiedNameFieldProvider(sources, Collections.emptyMap()),
            null
        );
        Function andFunction = (Function) expressionAnalyzer.convert(
            SqlParser.createExpression("not t1.id = 1 and not t2.id = 1"), context);

        Field t1Id = ((Field) ((Function) ((Function) andFunction.arguments().get(0)).arguments().get(0)).arguments().get(0));
        Field t2Id = ((Field) ((Function) ((Function) andFunction.arguments().get(1)).arguments().get(0)).arguments().get(0));
        assertTrue(t1Id.relation() != t2Id.relation());
    }

    @Test
    public void testSwapFunctionLeftSide() throws Exception {
        SqlExpressions expressions = new SqlExpressions(T3.SOURCES);
        Function cmp = (Function) expressions.normalize(expressions.asSymbol("8 + 5 > t1.x"));
        // the comparison was swapped so the field is on the left side
        assertThat(cmp.info().ident().name(), is("op_<"));
        assertThat(cmp.arguments().get(0), isField("x"));
    }

    @Test
    public void testBetweenIsRewrittenToLteAndGte() throws Exception {
        SqlExpressions expressions = new SqlExpressions(T3.SOURCES);
        Symbol symbol = expressions.asSymbol("10 between 1 and 10");
        assertThat(symbol, isSQL("((10 >= 1) AND (10 <= 10))"));
    }

    @Test
    public void testBetweenNullIsRewrittenToLteAndGte() throws Exception {
        SqlExpressions expressions = new SqlExpressions(T3.SOURCES);
        Symbol symbol = expressions.asSymbol("10 between 1 and NULL");
        assertThat(symbol, isSQL("((10 >= 1) AND (10 <= NULL))"));
    }

    @Test
    public void testNonDeterministicFunctionsAlwaysNew() throws Exception {
        ExpressionAnalysisContext localContext = new ExpressionAnalysisContext();
        FunctionInfo info1 = new FunctionInfo(
            new FunctionIdent("inc", Collections.singletonList(DataTypes.BOOLEAN)),
            DataTypes.INTEGER,
            FunctionInfo.Type.SCALAR,
            FunctionInfo.NO_FEATURES
        );
        Function fn1 = localContext.allocateFunction(info1, Collections.singletonList(Literal.BOOLEAN_FALSE));
        Function fn2 = localContext.allocateFunction(info1, Collections.singletonList(Literal.BOOLEAN_FALSE));
        Function fn3 = localContext.allocateFunction(info1, Collections.singletonList(Literal.BOOLEAN_TRUE));

        // different instances
        assertThat(fn1, allOf(
            not(sameInstance(fn2)),
            not(sameInstance(fn3))

        ));
        // but equal
        assertThat(fn1, is(equalTo(fn2)));
        assertThat(fn1, is(not(equalTo(fn3))));
    }
}
