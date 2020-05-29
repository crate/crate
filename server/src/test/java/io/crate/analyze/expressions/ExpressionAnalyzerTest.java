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
import io.crate.analyze.relations.AliasedAnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.FullQualifiedNameFieldProvider;
import io.crate.analyze.relations.ParentRelations;
import io.crate.analyze.relations.TableRelation;
import io.crate.auth.user.User;
import io.crate.exceptions.ConversionException;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.GtOperator;
import io.crate.expression.operator.LikeOperators;
import io.crate.expression.operator.LtOperator;
import io.crate.expression.operator.any.AnyOperators;
import io.crate.expression.scalar.cast.ImplicitCastFunction;
import io.crate.expression.scalar.conditional.CoalesceFunction;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.ParameterSymbol;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.RelationName;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.ArrayLiteral;
import io.crate.sql.tree.FunctionCall;
import io.crate.sql.tree.LongLiteral;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.StringLiteral;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.DummyRelation;
import io.crate.testing.SQLExecutor;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;
import io.crate.types.DataTypes;
import org.joda.time.Period;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.SymbolMatchers.isReference;
import static io.crate.testing.TestingHelpers.getFunctions;
import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Additional tests for the ExpressionAnalyzer.
 * Most of the remaining stuff is tested via {@link io.crate.analyze.SelectStatementAnalyzerTest} and other *AnalyzerTest classes.
 */
public class ExpressionAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private ImmutableMap<RelationName, AnalyzedRelation> dummySources;
    private CoordinatorTxnCtx coordinatorTxnCtx;
    private ExpressionAnalysisContext context;
    private ParamTypeHints paramTypeHints;
    private Functions functions;
    private SQLExecutor executor;
    private SqlExpressions expressions;

    @Before
    public void prepare() throws Exception {
        paramTypeHints = ParamTypeHints.EMPTY;
        DummyRelation dummyRelation = new DummyRelation("obj.x", "myObj.x", "myObj.x.AbC");
        dummySources = ImmutableMap.of(dummyRelation.relationName(), dummyRelation);
        coordinatorTxnCtx = CoordinatorTxnCtx.systemTransactionContext();
        context = new ExpressionAnalysisContext();
        functions = getFunctions();
        executor = SQLExecutor.builder(clusterService)
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T2_DEFINITION)
            .addTable(T3.T5_DEFINITION)
            .addTable("create table tarr (xs array(integer))")
            .build();
        expressions = new SqlExpressions(Collections.emptyMap());
    }

    @Test
    public void testUnsupportedExpressionCurrentDate() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Unsupported expression current_time");
        SessionContext sessionContext = SessionContext.systemSessionContext();
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            functions, coordinatorTxnCtx, paramTypeHints,
            new FullQualifiedNameFieldProvider(
                dummySources,
                ParentRelations.NO_PARENTS,
                sessionContext.searchPath().currentSchema()),
            null);
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext();

        expressionAnalyzer.convert(SqlParser.createExpression("current_time"), expressionAnalysisContext);
    }

    @Test
    public void testQuotedSubscriptExpression() throws Exception {
        SessionContext sessionContext = new SessionContext(
            EnumSet.of(Option.ALLOW_QUOTED_SUBSCRIPT), User.CRATE_USER);
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            functions,
            new CoordinatorTxnCtx(sessionContext),
            paramTypeHints,
            new FullQualifiedNameFieldProvider(
                dummySources,
                ParentRelations.NO_PARENTS,
                sessionContext.searchPath().currentSchema()),
            null);
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext();

        ScopedSymbol field1 = (ScopedSymbol) expressionAnalyzer.convert(SqlParser.createExpression("obj['x']"), expressionAnalysisContext);
        ScopedSymbol field2 = (ScopedSymbol) expressionAnalyzer.convert(SqlParser.createExpression("\"obj['x']\""), expressionAnalysisContext);
        assertEquals(field1, field2);

        ScopedSymbol field3 = (ScopedSymbol) expressionAnalyzer.convert(SqlParser.createExpression("\"myObj['x']\""), expressionAnalysisContext);
        assertEquals("myObj['x']", field3.column().toString());
        ScopedSymbol field4 = (ScopedSymbol) expressionAnalyzer.convert(SqlParser.createExpression("\"myObj['x']['AbC']\""), expressionAnalysisContext);
        assertEquals("myObj['x']['AbC']", field4.column().toString());
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
        SessionContext sessionContext = new SessionContext(
            EnumSet.of(Option.ALLOW_QUOTED_SUBSCRIPT), User.CRATE_USER);
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            functions,
            new CoordinatorTxnCtx(sessionContext),
            paramTypeHints,
            new FullQualifiedNameFieldProvider(
                dummySources,
                ParentRelations.NO_PARENTS,
                sessionContext.searchPath().currentSchema()),
            null);
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext();
        FunctionCall subscriptFunctionCall = new FunctionCall(
            new QualifiedName("subscript"),
            ImmutableList.of(
                new ArrayLiteral(ImmutableList.of(new StringLiteral("obj"))),
                new LongLiteral("1")));

        Symbol symbol = expressionAnalyzer.convert(subscriptFunctionCall, expressionAnalysisContext);
        assertThat(symbol, isLiteral("obj"));
    }

    @Test
    public void testInSelfJoinCaseFunctionsThatLookTheSameMustNotReuseFunctionAllocation() throws Exception {
        TableInfo t1 = executor.resolveTableInfo("t1");
        TableRelation relation = new TableRelation(t1);

        RelationName a1 = new RelationName(null, "a1");
        RelationName a2 = new RelationName(null, "a2");
        Map<RelationName, AnalyzedRelation> sources = ImmutableMap.of(
            a1, new AliasedAnalyzedRelation(relation, a1),
            a2, new AliasedAnalyzedRelation(relation, a2)
        );
        SessionContext sessionContext = SessionContext.systemSessionContext();
        CoordinatorTxnCtx coordinatorTxnCtx = new CoordinatorTxnCtx(sessionContext);
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            functions,
            coordinatorTxnCtx,
            paramTypeHints,
            new FullQualifiedNameFieldProvider(sources, ParentRelations.NO_PARENTS, sessionContext.searchPath().currentSchema()),
            null
        );
        Function andFunction = (Function) expressionAnalyzer.convert(
            SqlParser.createExpression("not a1.x = 1 and not a2.x = 1"), context);

        ScopedSymbol t1Id = ((ScopedSymbol) ((Function) ((Function) andFunction.arguments().get(0)).arguments().get(0)).arguments().get(0));
        ScopedSymbol t2Id = ((ScopedSymbol) ((Function) ((Function) andFunction.arguments().get(1)).arguments().get(0)).arguments().get(0));
        assertThat(t1Id.relation(), is(not(t2Id.relation())));
    }

    @Test
    public void testSwapFunctionLeftSide() throws Exception {
        Function cmp = (Function) expressions.normalize(executor.asSymbol("8 + 5 > t1.x"));
        // the comparison was swapped so the field is on the left side
        assertThat(cmp.info().ident().name(), is("op_<"));
        assertThat(cmp.arguments().get(0), isReference("x"));
    }

    @Test
    public void testBetweenIsRewrittenToLteAndGte() throws Exception {
        Symbol symbol = expressions.asSymbol("10 between 1 and 10");
        assertThat(symbol, isSQL("true"));
    }

    @Test
    public void testBetweenNullIsRewrittenToLteAndGte() throws Exception {
        Symbol symbol = expressions.asSymbol("10 between 1 and NULL");
        assertThat(symbol, isSQL("NULL"));
    }

    @Test
    public void testNonDeterministicFunctionsAlwaysNew() throws Exception {
        ExpressionAnalysisContext localContext = new ExpressionAnalysisContext();
        String functionName = CoalesceFunction.NAME;
        Symbol fn1 = ExpressionAnalyzer.allocateFunction(
            functionName,
            List.of(Literal.BOOLEAN_FALSE),
            null,
            localContext,
            functions,
            CoordinatorTxnCtx.systemTransactionContext());
        Symbol fn2 = ExpressionAnalyzer.allocateFunction(
            functionName,
            List.of(Literal.BOOLEAN_FALSE),
            null,
            localContext,
            functions,
            CoordinatorTxnCtx.systemTransactionContext());
        Symbol fn3 = ExpressionAnalyzer.allocateFunction(
            functionName,
            List.of(Literal.BOOLEAN_TRUE),
            null,
            localContext,
            functions,
            CoordinatorTxnCtx.systemTransactionContext());

        // different instances
        assertThat(fn1, allOf(
            not(sameInstance(fn2)),
            not(sameInstance(fn3))

        ));
        // but equal
        assertThat(fn1, is(equalTo(fn2)));
        assertThat(fn1, is(not(equalTo(fn3))));
    }

    @Test
    public void testInPredicateWithSubqueryIsRewrittenToAnyEq() {
        Symbol symbol = executor.asSymbol("t1.x in (select t2.y from t2)");
        assertThat(symbol, isSQL("(doc.t1.x = ANY((SELECT y FROM (doc.t2))))"));
    }

    @Test
    public void testEarlyConstantFolding() {
        assertThat(expressions.asSymbol("1 = (1 = (1 = 1))"), isLiteral(true));
    }

    @Test
    public void testLiteralCastsAreFlattened() {
        Symbol symbol = expressions.asSymbol("cast(cast(1 as long) as double)");
        assertThat(symbol, isLiteral(1.0));
    }

    @Test
    public void testParameterSymbolCastsAreFlattened() {
        Function comparisonFunction = (Function) executor.asSymbol("doc.t2.i = $1");
        assertThat(comparisonFunction.arguments().get(1), is(instanceOf(ParameterSymbol.class)));
        assertThat(comparisonFunction.arguments().get(1).valueType(), is(DataTypes.INTEGER));
    }

    @Test
    public void testColumnsCannotBeCastedToLiteralType() {
        Function symbol = (Function) executor.asSymbol("doc.t2.i = 1.1");
        assertThat(symbol.arguments().get(0), isReference("i"));
        assertThat(symbol.arguments().get(0).valueType(), is(DataTypes.INTEGER));
    }

    @Test
    public void testIncompatibleLiteralThrowsException() {
        expectedException.expect(ConversionException.class);
        expectedException.expectMessage("Cannot cast `2147483648` of type `bigint` to type `integer`");
        executor.asSymbol("doc.t2.i = 1 + " + Integer.MAX_VALUE);
    }

    @Test
    public void testFunctionsCanBeCasted() {
        Function symbol2 = (Function) executor.asSymbol("doc.t5.w = doc.t2.i + 1.2");
        assertThat(symbol2, isFunction(EqOperator.NAME));
        assertThat(symbol2.arguments().get(0), isReference("w"));
        assertThat(
            symbol2.arguments().get(1),
            isFunction(
                ImplicitCastFunction.NAME,
                List.of(DataTypes.INTEGER, DataTypes.STRING)
            )
        );
    }

    @Test
    public void testColumnsCanBeCastedWhenOnBothSidesOfOperator() {
        Function symbol = (Function) executor.asSymbol("doc.t5.i < doc.t5.w");
        assertThat(symbol, isFunction(LtOperator.NAME));
        assertThat(
            symbol.arguments().get(0),
            isFunction(
                ImplicitCastFunction.NAME,
                List.of(DataTypes.INTEGER, DataTypes.STRING)
            )
        );
        assertThat(symbol.arguments().get(1).valueType(), is(DataTypes.LONG));
    }

    @Test
    public void testLiteralIsCastedToColumnValue() {
        Function symbol = (Function) executor.asSymbol("5::long < doc.t1.i");
        assertThat(symbol, isFunction(GtOperator.NAME));
        assertThat(symbol.arguments().get(0).valueType(), is(DataTypes.INTEGER));
        assertThat(symbol.arguments().get(1).valueType(), is(DataTypes.INTEGER));
        assertThat(symbol.arguments().get(1), isLiteral(5));
    }

    @Test
    public void testTimestampTypesCanBeCastedToLong() {
        Symbol symbol = executor.asSymbol("doc.t5.ts::long");
        assertThat(symbol.valueType(), is(DataTypes.LONG));

        symbol = executor.asSymbol("doc.t5.ts_z::long");
        assertThat(symbol.valueType(), is(DataTypes.LONG));
    }

    @Test
    public void testBetweenIsEagerlyEvaluatedIfPossible() throws Exception {
        Symbol x = expressions.asSymbol("5 between 1 and 10");
        assertThat(x, isLiteral(true));
    }

    @Test
    public void testParameterExpressionInAny() throws Exception {
        Symbol s = expressions.asSymbol("5 = ANY(?)");
        assertThat(s, isFunction(AnyOperators.Names.EQ, isLiteral(5L), instanceOf(ParameterSymbol.class)));
    }

    @Test
    public void testParameterExpressionInLikeAny() throws Exception {
        Symbol s = expressions.asSymbol("5 LIKE ANY(?)");
        assertThat(s, isFunction(LikeOperators.ANY_LIKE, isLiteral(5L), instanceOf(ParameterSymbol.class)));
    }

    @Test
    public void testAnyWithArrayOnBothSidesResultsInNiceErrorMessage() {
        expectedException.expectMessage("Cannot cast `xs` of type `integer_array` to type `bigint`");
        executor.analyze("select * from tarr where xs = ANY([10, 20])");
    }

    @Test
    public void testCallingUnknownFunctionWithExplicitSchemaRaisesNiceError() {
        expectedException.expectMessage("unknown function: foo.bar(bigint)");
        executor.analyze("select foo.bar(1)");
    }

    @Test
    public void testTypeAliasCanBeUsedInCastNotation() {
        Symbol symbol = expressions.asSymbol("10::int2");
        assertThat(symbol.valueType(), is(DataTypes.SHORT));
    }

    @Test
    public void windowDefinitionOrderedByArrayTypeIsUnsupported() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot ORDER BY 'xs': invalid data type 'integer_array'");
        executor.analyze("select count(*) over(order by xs) from tarr");
    }

    @Test
    public void windowDefinitionPartitionedByArrayTypeIsUnsupported() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot PARTITION BY 'xs': invalid data type 'integer_array'");
        executor.analyze("select count(*) over(partition by xs) from tarr");
    }

    @Test
    public void testInterval() throws Exception {
        Literal literal = (Literal) expressions.asSymbol("INTERVAL '1' MONTH");
        assertThat(literal.valueType(), is(DataTypes.INTERVAL));
        Period period = (Period) literal.value();
        assertThat(period, is(new Period().withMonths(1)));
    }

    @Test
    public void testIntervalConversion() throws Exception {
        Literal literal = (Literal) expressions.asSymbol("INTERVAL '1' HOUR to SECOND");
        assertThat(literal.valueType(), is(DataTypes.INTERVAL));
        Period period = (Period) literal.value();
        assertThat(period, is(new Period().withSeconds(1)));
    }

    @Test
    public void testIntervalInvalidStartEnd() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Startfield must be less significant than Endfield");
        expressions.asSymbol("INTERVAL '1' MONTH TO YEAR");
    }
}
