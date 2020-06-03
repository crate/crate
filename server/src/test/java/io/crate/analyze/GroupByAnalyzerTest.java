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

package io.crate.analyze;

import io.crate.analyze.relations.AliasedAnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.expression.operator.LikeOperators;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static io.crate.testing.SymbolMatchers.isAlias;
import static io.crate.testing.SymbolMatchers.isField;
import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.SymbolMatchers.isReference;
import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

@SuppressWarnings("ConstantConditions")
public class GroupByAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor sqlExecutor;

    @Before
    public void prepare() throws IOException {
        sqlExecutor = SQLExecutor.builder(clusterService)
            .enableDefaultTables()
            .addTable("create table foo.users (" +
                      " id long primary key," +
                      " name string," +
                      " age integer," +
                      " height integer)")
            .build();
    }

    private <T extends AnalyzedStatement> T analyze(String statement) {
        //noinspection unchecked
        return (T) sqlExecutor.<AnalyzedRelation>analyze(statement);
    }

    @Test
    public void testGroupBySubscriptMissingOutput() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("'load['5']' must appear in the GROUP BY clause");
        analyze("select load['5'] from sys.nodes group by load['1']");
    }

    public void testGroupKeyNotInResultColumnList() throws Exception {
        QueriedSelectRelation relation = analyze("select count(*) from sys.nodes group by name");
        assertThat(relation.groupBy().size(), is(1));
        assertThat(relation.outputs(), contains(isFunction("count")));
    }

    @Test
    public void testGroupByOnAlias() throws Exception {
        QueriedSelectRelation relation = analyze("select count(*), name as n from sys.nodes group by n");
        assertThat(relation.groupBy().size(), is(1));
        assertThat(relation.outputs(), contains(
            isFunction("count"),
            isAlias("n", isReference("name"))
        ));
        assertEquals(relation.groupBy().get(0), relation.outputs().get(1));
    }

    @Test
    public void testGroupByOnOrdinal() throws Exception {
        // just like in postgres access by ordinal starts with 1
        QueriedSelectRelation relation = analyze("select count(*), name as n from sys.nodes group by 2");
        assertThat(relation.groupBy().size(), is(1));
        assertEquals(relation.groupBy().get(0), relation.outputs().get(1));
    }

    @Test
    public void testGroupByOnOrdinalAggregation() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Aggregate functions are not allowed in GROUP BY");
        analyze("select count(*), name as n from sys.nodes group by 1");
    }

    @Test
    public void testGroupByWithDistinctAggregation() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Aggregate functions are not allowed in GROUP BY");
        analyze("select count(DISTINCT name) from sys.nodes group by 1");
    }

    @Test
    public void testGroupByScalarAliasedWithRealColumnNameFailsIfScalarColumnIsNotGrouped() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
            "'(1 / height)' must appear in the GROUP BY clause");
        analyze("select 1/height as age from foo.users group by age");
    }

    @Test
    public void testGroupByScalarAliasAndValueInScalar() {
        QueriedSelectRelation relation =
            analyze("select 1/age as age from foo.users group by age order by age");
        assertThat(relation.groupBy().isEmpty(), is(false));
        List<Symbol> groupBySymbols = relation.groupBy();
        assertThat(((Reference) groupBySymbols.get(0)).column().fqn(), is("age"));
    }

    @Test
    public void testGroupByScalarAlias() {
        // grouping by what's under the alias, the 1/age values
        QueriedSelectRelation relation = analyze("select 1/age as theAlias from foo.users group by theAlias");
        assertThat(relation.groupBy().isEmpty(), is(false));
        List<Symbol> groupBySymbols = relation.groupBy();
        Symbol groupBy = groupBySymbols.get(0);
        assertThat(groupBy, isAlias("thealias", isFunction("divide")));
    }

    @Test
    public void testGroupByColumnInScalar() {
        // grouping by height values
        QueriedSelectRelation relation = analyze("select 1/age as height from foo.users group by age");
        assertThat(relation.groupBy().isEmpty(), is(false));
        List<Symbol> groupBySymbols = relation.groupBy();
        assertThat(((Reference) groupBySymbols.get(0)).column().fqn(), is("age"));
    }

    @Test
    public void testGroupByScalar() {
        QueriedSelectRelation relation = analyze("select 1/age from foo.users group by 1/age;");
        assertThat(relation.groupBy().isEmpty(), is(false));
        List<Symbol> groupBySymbols = relation.groupBy();
        Symbol groupBy = groupBySymbols.get(0);
        assertThat(groupBy, instanceOf(Function.class));
    }

    @Test
    public void testGroupByAliasedLiteral() {
        QueriedSelectRelation relation = analyze("select 58 as fiftyEight from foo.users group by fiftyEight;");
        assertThat(relation.groupBy().isEmpty(), is(false));
        List<Symbol> groupBySymbols = relation.groupBy();
        assertThat(groupBySymbols.get(0), isAlias("fiftyeight", isLiteral(58)));
    }

    @Test
    public void testGroupByLiteralAliasedWithRealColumnNameGroupsByColumnValue() {
        QueriedSelectRelation relation = analyze("select 58 as age from foo.users group by age;");
        assertThat(relation.groupBy().isEmpty(), is(false));
        List<Symbol> groupBySymbols = relation.groupBy();
        ReferenceIdent groupByIdent = ((Reference) groupBySymbols.get(0)).ident();
        assertThat(groupByIdent.columnIdent().fqn(), is("age"));
        assertThat(groupByIdent.tableIdent().fqn(), is("foo.users"));
    }

    @Test
    public void testNegateAliasRealColumnGroupByAlias() {
        QueriedSelectRelation relation = analyze("select age age, - age age from foo.users group by age;");
        assertThat(relation.groupBy().isEmpty(), is(false));
        List<Symbol> groupBySymbols = relation.groupBy();
        ReferenceIdent groupByIdent = ((Reference) groupBySymbols.get(0)).ident();
        assertThat(groupByIdent.columnIdent().fqn(), is("age"));
        assertThat(groupByIdent.tableIdent().fqn(), is("foo.users"));
    }

    @Test
    public void testGroupBySubscript() throws Exception {
        QueriedSelectRelation relation = analyze("select load['1'], count(*) from sys.nodes group by load['1']");
        assertThat(relation.limit(), nullValue());

        assertThat(relation.groupBy(), notNullValue());
        assertThat(relation.outputs().size(), is(2));
        assertThat(relation.groupBy().size(), is(1));
        assertThat(relation.groupBy().get(0), isReference("load['1']"));
    }

    @Test
    public void testSelectGroupByOrderByWithColumnMissingFromSelect() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("ORDER BY expression 'id' must appear in the select clause " +
                                        "when grouping or global aggregation is used");
        analyze("select name, count(id) from users group by name order by id");
    }

    @Test
    public void testSelectGroupByOrderByWithAggregateFunctionInOrderByClause() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("ORDER BY function 'max(count(upper(name)))' is not allowed. " +
                                        "Only scalar functions can be used");
        analyze("select name, count(id) from users group by name order by max(count(upper(name)))");
    }

    @Test
    public void testSelectAggregationMissingGroupBy() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("'name' must appear in the GROUP BY clause");
        analyze("select name, count(id) from users");
    }

    @Test
    public void testSelectGlobalDistinctAggregationMissingGroupBy() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("'name' must appear in the GROUP BY clause");
        analyze("select distinct name, count(id) from users");
    }


    @Test
    public void testSelectDistinctWithGroupBy() {
        QueriedSelectRelation relation = analyze("select distinct max(id) from users group by name order by 1");
        assertThat(relation.isDistinct(), is(true));
        assertThat(relation,
            isSQL("SELECT max(doc.users.id) GROUP BY doc.users.name ORDER BY max(doc.users.id)"));
    }

    @Test
    public void testSelectDistinctWithGroupByLimitAndOffset() {
        QueriedSelectRelation relation =
            analyze("select distinct max(id) from users group by name order by 1 limit 5 offset 10");
        assertThat(relation.isDistinct(), is(true));
        assertThat(relation,
            isSQL("SELECT max(doc.users.id) GROUP BY doc.users.name " +
                  "ORDER BY max(doc.users.id) LIMIT 5::bigint OFFSET 10::bigint"));
    }

    @Test
    public void testSelectDistinctWithGroupByOnJoin() {
        QueriedSelectRelation relation =
            analyze("select DISTINCT max(users.id) from users " +
                    "  inner join users_multi_pk on users.id = users_multi_pk.id " +
                    "group by users.name order by 1");
        assertThat(relation.isDistinct(), is(true));
        assertThat(relation,
            isSQL("SELECT max(doc.users.id) GROUP BY doc.users.name ORDER BY max(doc.users.id)"));
    }

    @Test
    public void testSelectDistinctWithGroupByOnSubSelectOuter() {
        QueriedSelectRelation relation = analyze("select distinct max(id) from (" +
                                            "  select * from users order by name limit 10" +
                                            ") t group by name order by 1");
        assertThat(relation.isDistinct(), is(true));
        assertThat(relation,
            isSQL("SELECT max(t.id) " +
                  "GROUP BY t.name " +
                  "ORDER BY max(t.id)"));
    }

    @Test
    public void testSelectDistinctWithGroupByOnSubSelectInner() {
        AnalyzedRelation relation =
            analyze("select * from (" +
                    "  select distinct id from users group by id, name order by 1" +
                    ") t order by 1 desc");
        assertThat(relation, instanceOf(QueriedSelectRelation.class));
        QueriedSelectRelation outerRelation = (QueriedSelectRelation) relation;
        assertThat(outerRelation.outputs(), contains(isField("id")));

        assertThat(outerRelation.groupBy(), Matchers.empty());
        assertThat(outerRelation.orderBy().orderBySymbols(), contains(isField("id")));

        AliasedAnalyzedRelation aliasedRelation = (AliasedAnalyzedRelation) outerRelation.from().get(0);
        QueriedSelectRelation innerRelation = (QueriedSelectRelation) aliasedRelation.relation();
        assertThat(innerRelation.isDistinct(), is(true));
        assertThat(innerRelation.groupBy(), contains(isReference("id"), isReference("name")));
    }

    @Test
    public void testGroupByOnLiteral() throws Exception {
        QueriedSelectRelation relation = analyze(
            "select [1,2,3], count(*) from users group by 1");
        assertThat(relation.outputs(), isSQL("[1, 2, 3], count(*)"));
        assertThat(relation.groupBy(), isSQL("[1, 2, 3]"));
    }

    @Test
    public void testGroupByOnNullLiteral() throws Exception {
        QueriedSelectRelation relation = analyze(
            "select null, count(*) from users group by 1");
        assertThat(relation.outputs(), isSQL("NULL, count(*)"));
        assertThat(relation.groupBy(), isSQL("NULL"));
    }

    @Test
    public void testGroupWithInvalidOrdinal() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("GROUP BY position 2 is not in select list");
        analyze("select name from users u group by 2");
    }

    @Test
    public void testGroupWithInvalidLiteral() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot use 'abc' in GROUP BY clause");
        analyze("select max(id) from users u group by 'abc'");
    }

    @Test
    public void testGroupByOnInvalidNegateLiteral() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("GROUP BY position -4 is not in select list");
        analyze("select count(*), name from sys.nodes group by -4");
    }

    @Test
    public void testGroupWithInvalidNullLiteral() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot use NULL in GROUP BY clause");
        analyze("select max(id) from users u group by NULL");
    }

    @Test
    public void testGroupByHaving() throws Exception {
        QueriedSelectRelation relation = analyze("select sum(floats) from users group by name having name like 'Slartibart%'");
        assertThat(relation.having(), isFunction(LikeOperators.OP_LIKE));
        Function havingFunction = (Function) relation.having();
        assertThat(havingFunction.arguments().size(), is(2));
        assertThat(havingFunction.arguments().get(0), isReference("name"));
        assertThat(havingFunction.arguments().get(1), isLiteral("Slartibart%"));
    }

    @Test
    public void testGroupByHavingAliasForRealColumn() {
        QueriedSelectRelation relation = analyze(
            "select id as name from users group by id, name having name != null;");

        Symbol havingClause = relation.having();
        assertThat(havingClause, isLiteral(null));
    }

    @Test
    public void testGroupByHavingNormalize() throws Exception {
        QueriedSelectRelation rel = analyze("select sum(floats) from users group by name having 1 > 4");
        assertThat(rel.having(), isLiteral(false));
    }

    @Test
    public void testGroupByHavingOtherColumnInAggregate() throws Exception {
        QueriedSelectRelation relation = analyze("select sum(floats), name from users group by name having max(bytes) = 4::char");
        assertThat(relation.having(), isFunction("op_="));
        Function havingFunction = (Function) relation.having();
        assertThat(havingFunction.arguments().size(), is(2));
        assertThat(havingFunction.arguments().get(0), isFunction("max"));
        Function maxFunction = (Function) havingFunction.arguments().get(0);

        assertThat(maxFunction.arguments().get(0), isReference("bytes"));
        assertThat(havingFunction.arguments().get(1), isLiteral((byte) 4));
    }

    @Test
    public void testGroupByHavingOtherColumnOutsideAggregate() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot use column bytes outside of an Aggregation in HAVING clause");
        analyze("select sum(floats) from users group by name having bytes = 4");
    }

    @Test
    public void testGroupByHavingOtherColumnOutsideAggregateInFunction() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot use column bytes outside of an Aggregation in HAVING clause");
        analyze("select sum(floats), name from users group by name having (bytes + 1)  = 4");
    }

    @Test
    public void testGroupByHavingByGroupKey() throws Exception {
        QueriedSelectRelation relation = analyze(
            "select sum(floats), name from users group by name having name like 'Slartibart%'");
        assertThat(relation.having(), isFunction(LikeOperators.OP_LIKE));
        Function havingFunction = (Function) relation.having();
        assertThat(havingFunction.arguments().size(), is(2));
        assertThat(havingFunction.arguments().get(0), isReference("name"));
        assertThat(havingFunction.arguments().get(1), isLiteral("Slartibart%"));
    }

    @Test
    public void testGroupByHavingComplex() throws Exception {
        QueriedSelectRelation relation = analyze("select sum(floats), name from users " +
                                            "group by name having 1=0 or sum(bytes) in (42, 43, 44) and name not like 'Slartibart%'");
        Function andFunction = (Function) relation.having();
        assertThat(andFunction, is(notNullValue()));
        assertThat(andFunction.info().ident().name(), is("op_and"));
        assertThat(andFunction.arguments().size(), is(2));

        assertThat(andFunction.arguments().get(0), isFunction("any_="));
        assertThat(andFunction.arguments().get(1), isFunction("op_not"));
    }

    @Test
    public void testGroupByHavingRecursiveFunction() {
        QueriedSelectRelation relation = analyze(
            "select sum(floats), name " +
            "from users " +
            "group by name " +
            "having sum(power(power(id::double, id::double), id::double)) > 0");
        assertThat(
            relation.having(),
            isSQL("(sum(power(power(cast(doc.users.id AS double precision), cast(doc.users.id AS double precision)), cast(doc.users.id AS double precision))) > 0.0)")
        );
    }
}
