/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.analyze.expressions;


import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.SymbolMatchers.isReference;
import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.joda.time.Period;
import org.junit.Before;
import org.junit.Test;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.relations.AliasedAnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.FullQualifiedNameFieldProvider;
import io.crate.analyze.relations.ParentRelations;
import io.crate.analyze.relations.TableRelation;
import io.crate.common.collections.Lists2;
import io.crate.exceptions.ConversionException;
import io.crate.expression.operator.EqOperator;
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
import io.crate.metadata.RelationName;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.parser.SqlParser;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.Asserts;
import io.crate.testing.SQLExecutor;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;
import io.crate.types.DataTypes;

/**
 * Additional tests for the ExpressionAnalyzer.
 * Most of the remaining stuff is tested via {@link io.crate.analyze.SelectStatementAnalyzerTest} and other *AnalyzerTest classes.
 */
public class ExpressionAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private ExpressionAnalysisContext context;
    private ParamTypeHints paramTypeHints;
    private SQLExecutor executor;
    private SqlExpressions expressions;

    @Before
    public void prepare() throws Exception {
        paramTypeHints = ParamTypeHints.EMPTY;
        context = new ExpressionAnalysisContext();
        executor = SQLExecutor.builder(clusterService)
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T2_DEFINITION)
            .addTable(T3.T5_DEFINITION)
            .addTable("create table tarr (xs array(integer))")
            .addTable("create table nested_obj (" +
                      "o object as (a object as (b object as (c int)))," +
                      "\"myObj\" object as (x object as (\"AbC\" int))" +
                      ")")
            .build();
        expressions = new SqlExpressions(Collections.emptyMap());
    }

    @Test
    public void testExpressionCurrentDateWorks() {
        var symbol = expressions.asSymbol("current_date");

        assertThat(symbol.valueType(), is(DataTypes.DATE));
    }

    @Test
    public void testAnalyzeSubscriptFunctionCall() throws Exception {
        // Test when use subscript function is used explicitly then it's handled (and validated)
        // the same way it's handled when the subscript operator `[]` is used
        var symbol = executor.asSymbol("subscript(nested_obj.\"myObj\", 'x')");
        assertThat(symbol, isReference("myObj['x']"));
    }

    @Test
    public void testInSelfJoinCaseFunctionsThatLookTheSameMustNotReuseFunctionAllocation() throws Exception {
        TableInfo t1 = executor.resolveTableInfo("t1");
        TableRelation relation = new TableRelation(t1);

        RelationName a1 = new RelationName(null, "a1");
        RelationName a2 = new RelationName(null, "a2");
        Map<RelationName, AnalyzedRelation> sources = Map.of(
            a1, new AliasedAnalyzedRelation(relation, a1),
            a2, new AliasedAnalyzedRelation(relation, a2)
        );
        SessionContext sessionContext = SessionContext.systemSessionContext();
        CoordinatorTxnCtx coordinatorTxnCtx = new CoordinatorTxnCtx(sessionContext);
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            coordinatorTxnCtx,
            expressions.nodeCtx,
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
        assertThat(cmp.name(), is("op_<"));
        assertThat(cmp.arguments().get(0), isReference("x"));
    }

    @Test
    public void testBetweenIsRewrittenToLteAndGte() throws Exception {
        Symbol symbol = executor.asSymbol("2 between t1.x and 20");
        assertThat(symbol, isSQL("(doc.t1.x <= 2)"));
    }

    @Test
    public void test_between_rewrite_if_first_arg_is_a_reference() throws Exception {
        Symbol symbol = executor.asSymbol("t1.x between 10 and 20");
        assertThat(symbol, isSQL("((doc.t1.x >= 10) AND (doc.t1.x <= 20))"));
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
            CoordinatorTxnCtx.systemTransactionContext(),
            expressions.nodeCtx);
            Symbol fn2 = ExpressionAnalyzer.allocateFunction(
            functionName,
            List.of(Literal.BOOLEAN_FALSE),
            null,
            localContext,
            CoordinatorTxnCtx.systemTransactionContext(),
            expressions.nodeCtx);
        Symbol fn3 = ExpressionAnalyzer.allocateFunction(
            functionName,
            List.of(Literal.BOOLEAN_TRUE),
            null,
            localContext,
            CoordinatorTxnCtx.systemTransactionContext(),
            expressions.nodeCtx);

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
    public void testColumnsAreCastedToLiteralType() {
        Function symbol = (Function) executor.asSymbol("doc.t2.i = 1.1");
        assertThat(symbol, isSQL("(_cast(doc.t2.i, 'double precision') = 1.1)"));
    }

    @Test
    public void testFunctionsCanBeCasted() {
        Function symbol2 = (Function) executor.asSymbol("doc.t5.w = doc.t2.i + 1::smallint");
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
        assertThat(s, isFunction(AnyOperators.Type.EQ.opName(), isLiteral(5), instanceOf(ParameterSymbol.class)));
    }

    @Test
    public void testParameterExpressionInLikeAny() throws Exception {
        Symbol s = expressions.asSymbol("5 LIKE ANY(?)");
        assertThat(s, isFunction(LikeOperators.ANY_LIKE, isLiteral(5), instanceOf(ParameterSymbol.class)));
    }

    @Test
    public void testAnyWithArrayOnBothSidesResultsInNiceErrorMessage() {
        expectedException.expectMessage("Unknown function: (doc.tarr.xs = ANY(_array(10, 20)))," +
                                        " no overload found for matching argument types: (integer_array, integer_array).");
        executor.analyze("select * from tarr where xs = ANY([10, 20])");
    }

    @Test
    public void testCallingUnknownFunctionWithExplicitSchemaRaisesNiceError() {
        expectedException.expectMessage("Unknown function: foo.bar(1)");
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

    @Test
    public void test_quoted_subscript() {
        var symbol = executor.asSymbol("nested_obj.\"o['a']['b']['c']\"");
        assertThat(symbol, isReference("o['a']['b']['c']"));

        symbol = executor.asSymbol("nested_obj.\"myObj['x']['AbC']\"");
        assertThat(symbol, isReference("myObj['x']['AbC']"));
    }

    @Test
    public void test_partial_quoted_subscript() {
        var symbol = executor.asSymbol("nested_obj.\"o['a']['b']\"['c']");
        assertThat(symbol, isReference("o['a']['b']['c']"));

        symbol = executor.asSymbol("nested_obj.\"myObj['x']\"['AbC']");
        assertThat(symbol, isReference("myObj['x']['AbC']"));
    }

    @Test
    public void test_resolve_eq_function_for_text_types_with_length_limit_and_unbound() throws IOException {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (str varchar(3))")
            .build();
        var eq = (Function) e.asSymbol("tbl.str = 'abc'");
        assertThat(
            Lists2.map(eq.arguments(), Symbol::valueType),
            contains(DataTypes.STRING, DataTypes.STRING));
    }

    @Test
    public void test_object_cast_errors_contain_child_information() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (obj object as (x int))")
            .build();
        Asserts.assertThrows(
            () -> e.asSymbol("obj = {x = 'foo'}"),
            ConversionException.class,
            "Cannot cast object element `x` with value `foo` to type `integer`"
        );
    }
}
