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

import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.Asserts.exactlyInstanceOf;
import static io.crate.testing.Asserts.isLiteral;
import static io.crate.testing.Asserts.isReference;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.joda.time.Period;
import org.joda.time.PeriodType;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.relations.AliasedAnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.FullQualifiedNameFieldProvider;
import io.crate.analyze.relations.ParentRelations;
import io.crate.analyze.relations.TableRelation;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.ConversionException;
import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.LikeOperators;
import io.crate.expression.operator.LtOperator;
import io.crate.expression.operator.any.AnyEqOperator;
import io.crate.expression.scalar.ArraySliceFunction;
import io.crate.expression.scalar.ArrayUnnestFunction;
import io.crate.expression.scalar.cast.ImplicitCastFunction;
import io.crate.expression.scalar.conditional.CoalesceFunction;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.ParameterSymbol;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.IndexType;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;
import io.crate.types.BitStringType;
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
        context = new ExpressionAnalysisContext(CoordinatorSessionSettings.systemDefaults());
        executor = SQLExecutor.builder(clusterService)
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T2_DEFINITION)
            .addTable(T3.T5_DEFINITION)
            .addTable("create table tarr (xs array(integer))")
            .addTable("create table quoted_subscript (\"a\"\"\" int[])")
            .addTable("create table nested_obj (" +
                      "o object as (a object as (b object as (c int)))," +
                      "o_arr array(object as (x int, o_arr_nested array(object as (y int))))," +
                      "\"myObj\" object as (x object as (\"AbC\" int))" +
                      ")")
            .build();
        expressions = new SqlExpressions(Collections.emptyMap());
    }

    @Test
    public void test_can_access_array_element_from_subscript_on_object_array() {
        var symbol = executor.asSymbol("o_arr['x'][1]");
        assertThat(symbol).isFunction(
            "subscript",
            isReference("o_arr['x']"),
            isLiteral(1)
        );

        symbol = executor.asSymbol("o_arr['o_arr_nested']['y'][1]");
        assertThat(symbol).isFunction(
            "subscript",
            isReference("o_arr['o_arr_nested']['y']"),
            isLiteral(1)
        );

        assertThatThrownBy(() -> executor.asSymbol("o_arr['o_arr_nested']['y'][1][1]"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Nested array access is not supported");
    }

    @Test
    public void testExpressionCurrentDateWorks() {
        var symbol = expressions.asSymbol("current_date");

        assertThat(symbol.valueType()).isEqualTo(DataTypes.DATE);
    }

    @Test
    public void testAnalyzeSubscriptFunctionCall() throws Exception {
        // Test when use subscript function is used explicitly then it's handled (and validated)
        // the same way it's handled when the subscript operator `[]` is used
        var symbol = executor.asSymbol("subscript(nested_obj.\"myObj\", 'x')");
        assertThat(symbol).isReference().hasName("myObj['x']");
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
        var sessionSettings = CoordinatorSessionSettings.systemDefaults();
        CoordinatorTxnCtx coordinatorTxnCtx = new CoordinatorTxnCtx(sessionSettings);
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            coordinatorTxnCtx,
            expressions.nodeCtx,
            paramTypeHints,
            new FullQualifiedNameFieldProvider(sources,
                                               ParentRelations.NO_PARENTS,
                                               sessionSettings.searchPath().currentSchema()),
            null
        );
        Function andFunction = (Function) expressionAnalyzer.convert(
            SqlParser.createExpression("not a1.x = 1 and not a2.x = 1"), context);

        ScopedSymbol t1Id = ((ScopedSymbol) ((Function) ((Function) andFunction.arguments().get(0)).arguments().get(0)).arguments().get(0));
        ScopedSymbol t2Id = ((ScopedSymbol) ((Function) ((Function) andFunction.arguments().get(1)).arguments().get(0)).arguments().get(0));
        assertThat(t1Id.relation()).isNotEqualTo(t2Id.relation());
    }

    @Test
    public void testSwapFunctionLeftSide() throws Exception {
        Function cmp = (Function) expressions.normalize(executor.asSymbol("8 + 5 > t1.x"));
        // the comparison was swapped so the field is on the left side
        assertThat(cmp.name()).isEqualTo("op_<");
        assertThat(cmp.arguments().get(0)).isReference().hasName("x");
    }

    @Test
    public void testBetweenIsRewrittenToLteAndGte() throws Exception {
        Symbol symbol = executor.asSymbol("2 between t1.x and 20");
        assertThat(symbol).isSQL("(doc.t1.x <= 2)");
    }

    @Test
    public void test_between_rewrite_if_first_arg_is_a_reference() throws Exception {
        Symbol symbol = executor.asSymbol("t1.x between 10 and 20");
        assertThat(symbol).isSQL("((doc.t1.x >= 10) AND (doc.t1.x <= 20))");
    }

    @Test
    public void testBetweenNullIsRewrittenToLteAndGte() throws Exception {
        Symbol symbol = expressions.asSymbol("10 between 1 and NULL");
        assertThat(symbol).isSQL("NULL");
    }

    @Test
    public void testNonDeterministicFunctionsAlwaysNew() throws Exception {
        CoordinatorTxnCtx txnCtx = CoordinatorTxnCtx.systemTransactionContext();
        ExpressionAnalysisContext localContext = new ExpressionAnalysisContext(txnCtx.sessionSettings());
        String functionName = CoalesceFunction.NAME;
        Symbol fn1 = ExpressionAnalyzer.allocateFunction(
            functionName,
            List.of(Literal.BOOLEAN_FALSE),
            null,
            localContext,
            txnCtx,
            expressions.nodeCtx);
        Symbol fn2 = ExpressionAnalyzer.allocateFunction(
            functionName,
            List.of(Literal.BOOLEAN_FALSE),
            null,
            localContext,
            txnCtx,
            expressions.nodeCtx);
        Symbol fn3 = ExpressionAnalyzer.allocateFunction(
            functionName,
            List.of(Literal.BOOLEAN_TRUE),
            null,
            localContext,
            txnCtx,
            expressions.nodeCtx);

        // different instances
        assertThat(fn1)
            .isNotSameAs(fn2)
            .isNotSameAs(fn3);
        // but equal
        assertThat(fn1).isEqualTo(fn2);
        assertThat(fn1).isNotEqualTo(fn3);
    }

    @Test
    public void testInPredicateWithSubqueryIsRewrittenToAnyEq() {
        Symbol symbol = executor.asSymbol("t1.x in (select t2.y from t2)");
        assertThat(symbol).isSQL("(doc.t1.x = ANY((SELECT y FROM (doc.t2))))");
    }

    @Test
    public void testEarlyConstantFolding() {
        assertThat(expressions.asSymbol("1 = (1 = (1 = 1))")).isLiteral(true);
    }

    @Test
    public void testLiteralCastsAreFlattened() {
        Symbol symbol = expressions.asSymbol("cast(cast(1 as long) as double)");
        assertThat(symbol).isLiteral(1.0);
    }

    @Test
    public void testParameterSymbolCastsAreFlattened() {
        Function comparisonFunction = (Function) executor.asSymbol("doc.t2.i = $1");
        assertThat(comparisonFunction.arguments().get(1)).isExactlyInstanceOf(ParameterSymbol.class);
        assertThat(comparisonFunction.arguments().get(1).valueType()).isEqualTo(DataTypes.INTEGER);
    }

    @Test
    public void testColumnsAreCastedToLiteralType() {
        Function symbol = (Function) executor.asSymbol("doc.t2.i = 1.1");
        assertThat(symbol).isSQL("(doc.t2.i = 1.1)");
        assertThat(symbol).isFunction(
            EqOperator.NAME,
            lhs -> {
                assertThat(lhs).isFunction("_cast");
                assertThat(lhs.valueType()).isEqualTo(DataTypes.DOUBLE);
            },
            rhs -> assertThat(rhs).isLiteral(1.1)
        );
    }

    @Test
    public void testFunctionsCanBeCasted() {
        Function symbol2 = (Function) executor.asSymbol("doc.t5.w = doc.t2.i + 1::smallint");
        assertThat(symbol2).isFunction(EqOperator.NAME);
        assertThat(symbol2.arguments().get(0)).isReference().hasName("w");
        assertThat(symbol2.arguments().get(1))
            .isFunction(ImplicitCastFunction.NAME, List.of(DataTypes.INTEGER, DataTypes.STRING));
    }

    @Test
    public void testColumnsCanBeCastedWhenOnBothSidesOfOperator() {
        Function symbol = (Function) executor.asSymbol("doc.t5.i < doc.t5.w");
        assertThat(symbol).isFunction(LtOperator.NAME);
        assertThat(symbol.arguments().get(0))
            .isFunction(ImplicitCastFunction.NAME, List.of(DataTypes.INTEGER, DataTypes.STRING));
        assertThat(symbol.arguments().get(1)).hasDataType(DataTypes.LONG);
    }

    @Test
    public void testTimestampTypesCanBeCastedToLong() {
        Symbol symbol = executor.asSymbol("doc.t5.ts::long");
        assertThat(symbol).hasDataType(DataTypes.LONG);

        symbol = executor.asSymbol("doc.t5.ts_z::long");
        assertThat(symbol).hasDataType(DataTypes.LONG);
    }

    @Test
    public void testBetweenIsEagerlyEvaluatedIfPossible() throws Exception {
        Symbol x = expressions.asSymbol("5 between 1 and 10");
        assertThat(x).isLiteral(true);
    }

    @Test
    public void testParameterExpressionInAny() throws Exception {
        Symbol s = expressions.asSymbol("5 = ANY(?)");
        assertThat(s).isFunction(AnyEqOperator.NAME, isLiteral(5), exactlyInstanceOf(ParameterSymbol.class));
    }

    @Test
    public void testParameterExpressionInLikeAny() throws Exception {
        Symbol s = expressions.asSymbol("5 LIKE ANY(?)");
        assertThat(s).isFunction(LikeOperators.ANY_LIKE, isLiteral("5"), exactlyInstanceOf(ParameterSymbol.class));
    }

    @Test
    public void testAnyWithArrayOnBothSidesResultsInNiceErrorMessage() {
        assertThatThrownBy(() -> executor.analyze("select * from tarr where xs = ANY([10, 20])"))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessageStartingWith("Unknown function: (doc.tarr.xs = ANY(_array(10, 20)))," +
                        " no overload found for matching argument types: (integer_array, integer_array).");
    }

    @Test
    public void testCallingUnknownFunctionWithExplicitSchemaRaisesNiceError() {
        assertThatThrownBy(() -> executor.analyze("select foo.bar(1)"))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessage("Unknown function: foo.bar(1)");
    }

    @Test
    public void testTypeAliasCanBeUsedInCastNotation() {
        Symbol symbol = expressions.asSymbol("10::int2");
        assertThat(symbol).hasDataType(DataTypes.SHORT);
    }

    @Test
    public void windowDefinitionOrderedByArrayTypeIsUnsupported() {
        assertThatThrownBy(() -> executor.analyze("select count(*) over(order by xs) from tarr"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Cannot ORDER BY 'xs': invalid data type 'integer_array'.");
    }

    @Test
    public void windowDefinitionPartitionedByArrayTypeIsUnsupported() {
        assertThatThrownBy(() -> executor.analyze("select count(*) over(partition by xs) from tarr"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Cannot PARTITION BY 'xs': invalid data type 'integer_array'.");
    }

    @Test
    public void testInterval() throws Exception {
        Symbol literal = expressions.asSymbol("INTERVAL '1' MONTH");
        assertThat(literal).isLiteral(new Period().withMonths(1).withPeriodType(PeriodType.yearMonthDayTime()),
                                      DataTypes.INTERVAL);
    }

    @Test
    public void testIntervalConversion() throws Exception {
        Symbol literal = expressions.asSymbol("INTERVAL '1' HOUR to SECOND");
        assertThat(literal).isLiteral(new Period().withSeconds(1).withPeriodType(PeriodType.yearMonthDayTime()),
                                      DataTypes.INTERVAL);
    }

    @Test
    public void testIntervalInvalidStartEnd() throws Exception {
        assertThatThrownBy(() -> expressions.asSymbol("INTERVAL '1' MONTH TO YEAR"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Startfield must be less significant than Endfield");
    }

    @Test
    public void test_quoted_subscript_expression_with_base_column_name_containing_quotes() {
        var symbol = executor.asSymbol("quoted_subscript.\"a\"\"[1]\"");
        assertThat(symbol).isFunction(
            "subscript",
            isReference("a\""),
            isLiteral(1));
    }

    @Test
    public void test_quoted_subscript() {
        var symbol = executor.asSymbol("nested_obj.\"o['a']['b']['c']\"");
        assertThat(symbol).isReference().hasName("o['a']['b']['c']");

        symbol = executor.asSymbol("nested_obj.\"myObj['x']['AbC']\"");
        assertThat(symbol).isReference().hasName("myObj['x']['AbC']");
    }

    /**
     * bug: https://github.com/crate/crate/issues/13845
     */
    @Test
    public void test_invalid_quoted_subscript() {
        assertThatThrownBy(() -> executor.asSymbol("\"\"\"a[1]\"\"\""))
            .isExactlyInstanceOf(ColumnUnknownException.class)
            .hasMessage("Column \"a[1]\" unknown");
    }

    @Test
    public void test_partial_quoted_subscript() {
        var symbol = executor.asSymbol("nested_obj.\"o['a']['b']\"['c']");
        assertThat(symbol).isReference().hasName("o['a']['b']['c']");

        symbol = executor.asSymbol("nested_obj.\"myObj['x']\"['AbC']");
        assertThat(symbol).isReference().hasName("myObj['x']['AbC']");
    }

    @Test
    public void test_resolve_eq_function_for_text_types_with_length_limit_and_unbound() throws IOException {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (str varchar(3))")
            .build();
        var eq = (Function) e.asSymbol("tbl.str = 'abc'");
        assertThat(eq.arguments()).satisfiesExactly(
            s -> assertThat(s).hasDataType(DataTypes.STRING),
            s -> assertThat(s).hasDataType(DataTypes.STRING));
    }

    @Test
    public void test_resolve_eq_function_for_bit_types_with_different_length() throws IOException {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (b bit(3))")
            .build();
        var eq = (Function) e.asSymbol("tbl.b = B'1'");
        assertThat(eq.arguments()).satisfiesExactly(
            s -> assertThat(s).hasDataType(new BitStringType(3)),
            s -> assertThat(s).hasDataType(new BitStringType(3)));
    }

    @Test
    public void test_object_cast_errors_contain_child_information() throws Exception {
        var e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (obj object as (x int))")
            .build();
        assertThatThrownBy(() -> e.asSymbol("obj = {x = 'foo'}"))
            .isExactlyInstanceOf(ConversionException.class)
            .hasMessageContaining("Cannot cast object element `x` with value `foo` to type `integer`");
    }

    @Test
    public void testAnalyzeArraySliceFunctionCall() {
        ReferenceIdent arrayRefIdent = new ReferenceIdent(new RelationName("doc", "tarr"), "xs");
        SimpleReference arrayRef = new SimpleReference(arrayRefIdent,
                                           RowGranularity.DOC,
                                           DataTypes.INTEGER_ARRAY,
                                           ColumnPolicy.DYNAMIC,
                                           IndexType.PLAIN,
                                           true,
                                           true,
                                           1, null);
        CoordinatorTxnCtx txnCtx = CoordinatorTxnCtx.systemTransactionContext();
        ExpressionAnalysisContext localContext = new ExpressionAnalysisContext(txnCtx.sessionSettings());
        Symbol function = ExpressionAnalyzer.allocateFunction(
            ArraySliceFunction.NAME,
            List.of(arrayRef, Literal.of(1), Literal.of(3)),
            null,
            localContext,
            txnCtx,
            expressions.nodeCtx
        );

        var result = executor.asSymbol("tarr.xs[1:3]");

        assertThat(result).isEqualTo(function);
    }

    @Test
    public void limit_and_offset_only_accept_integer_types_for_cast() {
        assertThatThrownBy(() -> executor.analyze("SELECT 1 LIMIT '127.0.0.1'::ip"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Cannot cast to a datatype that is not convertable to `integer`");
        assertThatThrownBy(() -> executor.analyze("SELECT 1 LIMIT TRY_CAST('{\"name\"=\"Arthur\"}' AS object)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Cannot cast to a datatype that is not convertable to `integer`");
        assertThatThrownBy(() -> executor.analyze("SELECT 1 OFFSET CAST(? AS ip)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Cannot cast to a datatype that is not convertable to `integer`");
        assertThatThrownBy(() -> executor.analyze("SELECT 1 OFFSET TRY_CAST('{\"name\"=\"Arthur\"}' AS object)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Cannot cast to a datatype that is not convertable to `integer`");
    }

    @Test
    public void test_any_automatically_levels_array_dimensions() throws Exception {
        assertThat(executor.asSymbol("1 = ANY([1, 2])")).isLiteral(true);
        assertThat(executor.asSymbol("3 = ANY([1, 2])")).isLiteral(false);

        assertThat(executor.asSymbol("[1, 2] = ANY([[3, 4], [1, 2]])")).isLiteral(true);
        assertThat(executor.asSymbol("[1, 3] = ANY([[3, 4], [1, 2]])")).isLiteral(false);

        assertThat(executor.asSymbol("1 = ANY([ [1, 2, 3], [4, 5] ])")).isLiteral(true);
        assertThat(executor.asSymbol("8 = ANY([ [1, 2, 3], [4, 5] ])")).isLiteral(false);

        assertThat(executor.asSymbol("1 = ANY([ [[1, 2, 3], [1, 2]], [[4, 5], [5]] ])")).isLiteral(true);
        assertThat(executor.asSymbol("[1] = ANY([ [[1, 2, 3], [1, 2]], [[4, 5], [5]] ])")).isLiteral(false);
        assertThat(executor.asSymbol("[1, 2] = ANY([ [[1, 2, 3], [1, 2]], [[4, 5], [5]] ])")).isLiteral(true);

        assertThat(executor.asSymbol("t1.x = ANY([ [1, 2, 3], [4, 5] ])")).isFunction(
            AnyEqOperator.NAME,
            x -> assertThat(x).isReference().hasName("x"),
            x -> assertThat(x)
                .as("array_unnest is eagerly normalized to array literal")
                .isLiteral(List.of(1, 2, 3, 4, 5))
        );

        assertThat(executor.asSymbol("1 = ANY(o_arr['o_arr_nested']['y'])")).isFunction(
            AnyEqOperator.NAME,
            x -> assertThat(x).isLiteral(1),
            x -> assertThat(x).isFunction(
                ArrayUnnestFunction.NAME,
                y -> assertThat(y).isReference().hasName("o_arr['o_arr_nested']['y']")
            )
        );
    }
}
