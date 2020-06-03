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
import io.crate.analyze.relations.DocTableRelation;
import io.crate.exceptions.AmbiguousColumnException;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static io.crate.testing.RelationMatchers.isDocTable;
import static io.crate.testing.SymbolMatchers.isAlias;
import static io.crate.testing.SymbolMatchers.isField;
import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.SymbolMatchers.isReference;
import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class SubSelectAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor executor;
    private DocTableInfo t1Info;

    @Before
    public void prepare() throws IOException {
        executor = SQLExecutor.builder(clusterService).enableDefaultTables().build();
        t1Info = executor.schemas().getTableInfo(new RelationName("doc", "t1"));
    }

    private <T extends AnalyzedRelation> T analyze(String stmt) {
        return executor.analyze(stmt);
    }

    @Test
    public void testSimpleSubSelect() throws Exception {
        QueriedSelectRelation relation = analyze(
            "select aliased_sub.x / aliased_sub.i from (select x, i from t1) as aliased_sub");
        assertThat(relation.outputs(), contains(isFunction("divide", isField("x"), isField("i"))));
        assertThat(relation.from().get(0).outputs(), contains(isField("x"), isField("i")));
    }

    @Test
    public void testSimpleSubSelectWithMixedCases() throws Exception {
        AnalyzedRelation relation = analyze(
            "select aliased_sub.A from (select a from t1) as aliased_sub");
        assertThat(relation.outputs().size(), is(1));
        assertThat(relation.outputs().get(0), isField("a"));
    }

    @Test
    public void testSubSelectWithoutAlias() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("subquery in FROM clause must have an alias");
        analyze("select id from (select a as id from t1)");
    }

    @Test
    public void testSubSelectWithNestedAlias() throws Exception {
        QueriedSelectRelation relation = analyze(
            "select tt.aa, (tt.xi + 1::int)" +
            " from (select (x + i) as xi, concat(a, a) as aa, i from t1) as tt");
        assertThat(relation.outputs(), contains(
            isField("aa"),
            isFunction("add", isField("xi"), isLiteral(1))
        ));
        QueriedSelectRelation innerQSR = (QueriedSelectRelation) ((AliasedAnalyzedRelation) relation.from().get(0)).relation();
        assertThat(
            innerQSR.from(),
            contains(isDocTable(new RelationName("doc", "t1"))));
    }

    @Test
    public void testNestedSubSelect() throws Exception {
        AnalyzedRelation relation = analyze(
            "select aliased_sub.a from (select nested_sub.a from (select a from t1) as nested_sub) as aliased_sub");
        assertThat(relation.outputs().size(), is(1));
        assertThat(relation.outputs().get(0), isField("a"));
    }

    @Test
    public void testSubSelectWithJoins() throws Exception {
        QueriedSelectRelation relation = analyze(
            "select aliased_sub.a, aliased_sub.b from (select t1.a, t2.b from t1, t2) as aliased_sub");
        AliasedAnalyzedRelation aliasRel = (AliasedAnalyzedRelation) relation.from().get(0);
        QueriedSelectRelation innerRelation = (QueriedSelectRelation) aliasRel.relation();
        assertThat(innerRelation.from().size(), is(2));
        assertThat(innerRelation.outputs(), contains(
            isReference("a"),
            isReference("b")
        ));
    }

    @Test
    public void testSubSelectWithJoinsAmbiguousColumn() throws Exception {
        expectedException.expect(AmbiguousColumnException.class);
        expectedException.expectMessage("Column \"i\" is ambiguous");
        analyze("select aliased_sub.i, aliased_sub.b from (select t1.i, t2.i, t2.b from t1, t2) as aliased_sub");
    }

    @Test
    public void testJoinOnSubSelects() throws Exception {
        QueriedSelectRelation relation = analyze("select * from " +
                                                 " (select a, i from t1 order by a limit 5) t1 " +
                                                 "left join" +
                                                 " (select b, i from t2 where b > '10') t2 " +
                                                 "on t1.i = t2.i where t1.a > '50' and t2.b > '100' " +
                                                 "limit 10");
        assertThat(relation,
                   isSQL("SELECT t1.a, t1.i, t2.b, t2.i WHERE ((t1.a > '50') AND (t2.b > '100')) LIMIT 10::bigint"));
        assertThat(relation.joinPairs().get(0).condition(),
                   isSQL("(t1.i = t2.i)"));
        assertThat(((AliasedAnalyzedRelation) relation.from().get(0)).relation(),
                   isSQL("SELECT doc.t1.a, doc.t1.i ORDER BY doc.t1.a LIMIT 5::bigint"));
        assertThat(((AliasedAnalyzedRelation) relation.from().get(1)).relation(),
                   isSQL("SELECT doc.t2.b, doc.t2.i WHERE (doc.t2.b > '10')"));
    }

    @Test
    public void testJoinOnSubSelectsWithOrderByAndLimitNotPushedDown() throws Exception {
        QueriedSelectRelation relation = analyze("select * from " +
                                                 " (select a, i from t1 order by a limit 5) t1 " +
                                                 "left join" +
                                                 " (select b, i from t2 where b > '10') t2 " +
                                                 "on t1.i = t2.i where t1.a > '50' and t2.b > '100' " +
                                                 "order by 2 limit 10");
        assertThat(relation,
            isSQL("SELECT t1.a, t1.i, t2.b, t2.i WHERE ((t1.a > '50') AND (t2.b > '100')) ORDER BY t1.i LIMIT 10::bigint"));
        assertThat(relation.joinPairs().get(0).condition(),
            isSQL("(t1.i = t2.i)"));
        assertThat(((AliasedAnalyzedRelation) relation.from().get(0)).relation(),
            isSQL("SELECT doc.t1.a, doc.t1.i ORDER BY doc.t1.a LIMIT 5::bigint"));
        assertThat(((AliasedAnalyzedRelation) relation.from().get(1)).relation(),
            isSQL("SELECT doc.t2.b, doc.t2.i WHERE (doc.t2.b > '10')"));
    }

    @Test
    public void testJoinOnSubSelectsWithGlobalAggregationsAndLimitNotPushedDown() throws Exception {
        QueriedSelectRelation relation = analyze("select count(*) from " +
                                                 " (select a, i from (" +
                                                 "     select * from t1 order by a desc limit 5) a" +
                                                 "  order by a limit 10) t1 " +
                                                 "join" +
                                                 " (select b, i from t2 where b > '10') t2 " +
                                                 "on t1.i = t2.i " +
                                                 "order by 1 limit 10");
        assertThat(relation.outputs(), contains(isFunction("count")));
        assertThat(relation.orderBy().orderBySymbols(), contains(isFunction("count")));
        assertThat(relation.limit(), isLiteral(10L));

        assertThat(relation.joinPairs().get(0).condition(), isFunction("op_=", isField("i"), isField("i")));

        AliasedAnalyzedRelation t1Alias = (AliasedAnalyzedRelation) relation.from().get(0);
        QueriedSelectRelation t1Sub = ((QueriedSelectRelation) t1Alias.relation());
        assertThat(t1Sub.outputs(), contains(isField("a"), isField("i")));
        assertThat(t1Sub.orderBy().orderBySymbols(), contains(isField("a")));
        assertThat(t1Sub.limit(), isLiteral(10L));

        AliasedAnalyzedRelation t1NestedAlias = (AliasedAnalyzedRelation) t1Sub.from().get(0);
        QueriedSelectRelation t1 = ((QueriedSelectRelation) t1NestedAlias.relation());
        assertThat(t1.orderBy().orderBySymbols(), contains(isReference("a")));
        assertThat(t1.limit(), isLiteral(5L));

        AliasedAnalyzedRelation t2Alias = (AliasedAnalyzedRelation) relation.from().get(1);
        QueriedSelectRelation t2sub = (QueriedSelectRelation) t2Alias.relation();
        assertThat(t2sub.where(), isFunction("op_>", isReference("b"), isLiteral("10")));
    }

    @Test
    public void testJoinOnSubSelectsWithOrder() throws Exception {
        QueriedSelectRelation relation = analyze("select * from " +
                                                 " (select a, i from t1 order by a) t1, " +
                                                 " (select b, i from t2 where b > '10') t2 " +
                                                 "where t1.a > '50' and t2.b > '100' " +
                                                 "order by 2 limit 10");
        assertThat(relation.outputs(), contains(isField("a"), isField("i"), isField("b"), isField("i")));
        assertThat(relation.where(), isSQL("((t1.a > '50') AND (t2.b > '100'))"));
        assertThat(relation.orderBy().orderBySymbols(), contains(isField("i")));
        assertThat(relation.limit(), isLiteral(10L));

        AliasedAnalyzedRelation t1Alias = (AliasedAnalyzedRelation) relation.from().get(0);
        QueriedSelectRelation t1 = ((QueriedSelectRelation) t1Alias.relation());
        assertThat(t1.where(), isSQL("true"));
        assertThat(t1.orderBy(), isSQL("doc.t1.a"));

        AliasedAnalyzedRelation t2Alias = (AliasedAnalyzedRelation) relation.from().get(1);
        QueriedSelectRelation t2 = ((QueriedSelectRelation) t2Alias.relation());
        assertThat(t2.where(), isFunction("op_>", isReference("b"), isLiteral("10")));
        assertThat(t2.orderBy(), Matchers.nullValue());
    }

    @Test
    public void testSubSelectWithOuterJoinAndAggregations() throws Exception {
        QueriedSelectRelation relation = analyze("select * from " +
                                                 " (select max(a) ma, i from t1 group by i) t1 " +
                                                 "left join" +
                                                 " (select max(b) mb, i from t2 group by i having i > 10::int) t2 " +
                                                 "on t1.i = t2.i where t1.ma > '50' and t2.mb > '100'");
        assertThat(relation.outputs(), isSQL("t1.ma, t1.i, t2.mb, t2.i"));
        assertThat(relation.joinPairs().get(0).condition(), isSQL("(t1.i = t2.i)"));
        assertThat(relation.where(), isSQL("((t1.ma > '50') AND (t2.mb > '100'))"));

        AliasedAnalyzedRelation t1Alias = (AliasedAnalyzedRelation) relation.from().get(0);
        QueriedSelectRelation t1Sel = (QueriedSelectRelation) t1Alias.relation();
        assertThat(t1Sel.outputs(), contains(
            isAlias("ma", isFunction("max")),
            isReference("i")
        ));
        assertThat(t1Sel.groupBy(), isSQL("doc.t1.i"));
        assertThat(t1Sel.having(), Matchers.nullValue());

        AliasedAnalyzedRelation t2Alias = (AliasedAnalyzedRelation) relation.from().get(1);
        QueriedSelectRelation t2Sel = (QueriedSelectRelation) t2Alias.relation();
        assertThat(t2Sel.outputs(), contains(
            isAlias("mb", isFunction("max", isReference("b"))),
            isReference("i")
        ));
        assertThat(t2Sel.groupBy(), isSQL("doc.t2.i"));
        assertThat(t2Sel.having(), isSQL("(doc.t2.i > 10)"));
    }

    @Test
    public void testPreserveAliasOnSubSelectInSelectList() throws Exception {
        QueriedSelectRelation relation = analyze("SELECT " +
                                                 "   (select min(t1.x) from t1) as min_col," +
                                                 "   (select 10) + (select 20) as add_subquery "+
                                                 "FROM (select * from t1) tt1");

        assertThat(relation.outputs(), contains(
            isAlias("min_col", instanceOf(SelectSymbol.class)),
            isAlias("add_subquery", isFunction("add"))
        ));
    }

    @Test
    public void testPreserveAliasesOnSubSelect() throws Exception {
        QueriedSelectRelation relation = analyze("SELECT tt1.x as a1, min(tt1.x) as a2 " +
                                                 "FROM (select * from t1) as tt1 " +
                                                 "GROUP BY a1");
        assertThat(relation.outputs(), contains(
            isAlias("a1", isField("x")),
            isAlias("a2", isFunction("min"))
        ));
        QueriedSelectRelation queriedTable =
            (QueriedSelectRelation) ((AliasedAnalyzedRelation) relation.from().get(0)).relation();
        assertThat(((DocTableRelation) queriedTable.from().get(0)).tableInfo(), is(t1Info));
    }
}
