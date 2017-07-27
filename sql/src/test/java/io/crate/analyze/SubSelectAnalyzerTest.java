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

import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.exceptions.AmbiguousColumnAliasException;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isField;
import static io.crate.testing.T3.T1_INFO;
import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.is;

public class SubSelectAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor executor;

    @Before
    public void prepare() {
        executor = SQLExecutor.builder(clusterService).enableDefaultTables().build();
    }

    private SelectAnalyzedStatement analyze(String stmt) {
        return executor.analyze(stmt);
    }

    @Test
    public void testSimpleSubSelect() throws Exception {
        SelectAnalyzedStatement statement = analyze(
            "select aliased_sub.x / aliased_sub.i from (select x, i from t1) as aliased_sub");
        QueriedDocTable relation = (QueriedDocTable) statement.relation();
        assertThat(relation.fields().size(), is(1));
        assertThat(relation.fields().get(0), isField("(x / i)"));
        assertThat(relation.tableRelation().tableInfo(), is(T1_INFO));
    }

    @Test
    public void testSimpleSubSelectWithMixedCases() throws Exception {
        SelectAnalyzedStatement statement = analyze(
            "select aliased_sub.A from (select a from t1) as aliased_sub");
        QueriedDocTable relation = (QueriedDocTable) statement.relation();
        assertThat(relation.fields().size(), is(1));
        assertThat(relation.fields().get(0), isField("a"));
    }

    @Test
    public void testSubSelectWithoutAlias() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("subquery in FROM must have an alias");
        analyze("select id from (select a as id from t1)");
    }

    @Test
    public void testSubSelectWithNestedAlias() throws Exception {
        SelectAnalyzedStatement statement = analyze(
            "select tt.aa, (tt.xi + 1)" +
            " from (select (x + i) as xi, concat(a, a) as aa, i from t1) as tt");
        QueriedDocTable relation = (QueriedDocTable) statement.relation();
        assertThat(relation.fields().size(), is(2));
        assertThat(relation.fields().get(0), isField("aa"));
        assertThat(relation.fields().get(1), isField("(xi + 1)"));
        assertThat(relation.tableRelation().tableInfo(), is(T1_INFO));
    }

    @Test
    public void testNestedSubSelect() throws Exception {
        SelectAnalyzedStatement statement = analyze(
            "select aliased_sub.a from (select nested_sub.a from (select a from t1) as nested_sub) as aliased_sub");
        QueriedDocTable relation = (QueriedDocTable) statement.relation();
        assertThat(relation.fields().size(), is(1));
        assertThat(relation.fields().get(0), isField("a"));
    }

    @Test
    public void testSubSelectWithJoins() throws Exception {
        SelectAnalyzedStatement statement = analyze(
            "select aliased_sub.a, aliased_sub.b from (select t1.a, t2.b from t1, t2) as aliased_sub");
        MultiSourceSelect relation = (MultiSourceSelect) statement.relation();
        assertThat(relation.sources().size(), is(2));
        assertThat(relation.fields().size(), is(2));
        assertThat(relation.fields().get(0), isField("a"));
        assertThat(relation.fields().get(1), isField("b"));
    }

    @Test
    public void testSubSelectWithJoinsAmbiguousColumn() throws Exception {
        expectedException.expect(AmbiguousColumnAliasException.class);
        expectedException.expectMessage("Column alias \"i\" is ambiguous");
        analyze("select aliased_sub.i, aliased_sub.b from (select t1.i, t2.i, t2.b from t1, t2) as aliased_sub");
    }

    @Test
    public void testJoinOnSubSelects() throws Exception {
        SelectAnalyzedStatement statement = analyze("select * from " +
                                                    " (select a, i from t1 order by a limit 5) t1 " +
                                                    "left join" +
                                                    " (select b, i from t2 where b > 10) t2 " +
                                                    "on t1.i = t2.i where t1.a > 50 and t2.b > 100 " +
                                                    "limit 10");
        MultiSourceSelect relation = (MultiSourceSelect) statement.relation();
        assertThat(relation.querySpec(),
                   isSQL("SELECT io.crate.analyze.QueriedSelectRelation.a, " +
                         "io.crate.analyze.QueriedSelectRelation.i, doc.t2.b, doc.t2.i LIMIT 10"));
        assertThat(relation.joinPairs().get(0).condition(),
                   isSQL("(io.crate.analyze.QueriedSelectRelation.i = doc.t2.i)"));
        assertThat(((QueriedRelation)relation.sources().get(new QualifiedName("t1"))).querySpec(),
                   isSQL("SELECT doc.t1.i, doc.t1.a WHERE (doc.t1.a > '50')"));
        assertThat(((QueriedSelectRelation)relation.sources().get(new QualifiedName("t1"))).subRelation().querySpec(),
                   isSQL("SELECT doc.t1.a, doc.t1.i ORDER BY doc.t1.a LIMIT 5"));
        assertThat(((QueriedRelation)relation.sources().get(new QualifiedName("t2"))).querySpec(),
                   isSQL("SELECT doc.t2.b, doc.t2.i WHERE ((doc.t2.b > '10') AND (doc.t2.b > '100'))"));
    }

    @Test
    public void testJoinOnSubSelectsWithOrderByAndLimitNotPushedDown() throws Exception {
        SelectAnalyzedStatement statement = analyze("select * from " +
                                                    " (select a, i from t1 order by a limit 5) t1 " +
                                                    "left join" +
                                                    " (select b, i from t2 where b > 10) t2 " +
                                                    "on t1.i = t2.i where t1.a > 50 and t2.b > 100 " +
                                                    "order by 2 limit 10");
        MultiSourceSelect relation = (MultiSourceSelect) statement.relation();
        assertThat(relation.querySpec(),
            isSQL("SELECT io.crate.analyze.QueriedSelectRelation.a, " +
                  "io.crate.analyze.QueriedSelectRelation.i, doc.t2.b, doc.t2.i " +
                  "ORDER BY io.crate.analyze.QueriedSelectRelation.i LIMIT 10"));
        assertThat(relation.joinPairs().get(0).condition(),
            isSQL("(io.crate.analyze.QueriedSelectRelation.i = doc.t2.i)"));
        assertThat(((QueriedRelation)relation.sources().get(new QualifiedName("t1"))).querySpec(),
            isSQL("SELECT doc.t1.i, doc.t1.a WHERE (doc.t1.a > '50')"));
        assertThat(((QueriedSelectRelation)relation.sources().get(new QualifiedName("t1"))).subRelation().querySpec(),
            isSQL("SELECT doc.t1.a, doc.t1.i ORDER BY doc.t1.a LIMIT 5"));
        assertThat(((QueriedRelation)relation.sources().get(new QualifiedName("t2"))).querySpec(),
            isSQL("SELECT doc.t2.b, doc.t2.i WHERE ((doc.t2.b > '10') AND (doc.t2.b > '100'))"));
    }

    @Test
    public void testJoinOnSubSelectsWithOrderByPushedDown() throws Exception {
        SelectAnalyzedStatement statement = analyze("select * from " +
                                                    " (select a, i from t1 order by a) t1, " +
                                                    " (select b, i from t2 where b > 10) t2 " +
                                                    "where t1.a > 50 and t2.b > 100 " +
                                                    "order by 2 limit 10");
        MultiSourceSelect relation = (MultiSourceSelect) statement.relation();
        assertThat(relation.querySpec(),
            isSQL("SELECT doc.t1.a, doc.t1.i, doc.t2.b, doc.t2.i ORDER BY doc.t1.i LIMIT 10"));
        assertThat(((QueriedRelation)relation.sources().get(new QualifiedName("t1"))).querySpec(),
            isSQL("SELECT doc.t1.i, doc.t1.a WHERE (doc.t1.a > '50') ORDER BY doc.t1.i LIMIT 10"));
        assertThat(((QueriedRelation)relation.sources().get(new QualifiedName("t2"))).querySpec(),
            isSQL("SELECT doc.t2.b, doc.t2.i WHERE ((doc.t2.b > '100') AND (doc.t2.b > '10')) LIMIT 10"));
    }

    @Test
    public void testSubSelectWithOuterJoinAndAggregations() throws Exception {
        SelectAnalyzedStatement statement = analyze("select * from " +
                                                    " (select max(a) ma, i from t1 group by i) t1 " +
                                                    "left join" +
                                                    " (select max(b) mb, i from t2 group by i having i > 10) t2 " +
                                                    "on t1.i = t2.i where t1.ma > 50 and t2.mb > 100");
        MultiSourceSelect relation = (MultiSourceSelect) statement.relation();
        assertThat(relation.querySpec(), isSQL("SELECT doc.t1.ma, doc.t1.i, doc.t2.mb, doc.t2.i"));
        assertThat(relation.joinPairs().get(0).condition(), isSQL("(doc.t1.i = doc.t2.i)"));
        assertThat(((QueriedRelation)relation.sources().get(new QualifiedName("t1"))).querySpec(),
                   isSQL("SELECT doc.t1.i, max(doc.t1.a) GROUP BY doc.t1.i HAVING (max(doc.t1.a) > '50')"));
        assertThat(((QueriedRelation)relation.sources().get(new QualifiedName("t2"))).querySpec(),
                   isSQL("SELECT max(doc.t2.b), doc.t2.i GROUP BY doc.t2.i " +
                         "HAVING ((doc.t2.i > 10) AND (max(doc.t2.b) > '100'))"));
    }

    @Test
    public void testPreserveAliasOnSubSelectInSelectList() throws Exception {
        SelectAnalyzedStatement statement = analyze("SELECT " +
                                                    "   (select min(t1.x) from t1) as min_col," +
                                                    "   (select 10) + (select 20) as add_subquery "+
                                                    "FROM (select * from t1) tt1");
        QueriedDocTable relation = (QueriedDocTable) statement.relation();
        assertThat(relation.fields().size(), is(2));
        assertThat(relation.fields().get(0), isField("min_col"));
        assertThat(relation.fields().get(1), isField("add_subquery"));
        assertThat(relation.tableRelation().tableInfo(), is(T1_INFO));
    }

    @Test
    public void testPreserveAliasedOnSubSelect() throws Exception {
        SelectAnalyzedStatement statement = analyze("SELECT tt1.x as a1, min(tt1.x) as a2 " +
                                                    "FROM (select * from t1) as tt1 " +
                                                    "GROUP BY a1");
        QueriedDocTable relation = (QueriedDocTable) statement.relation();
        assertThat(relation.fields().size(), is(2));
        assertThat(relation.fields().get(0), isField("a1"));
        assertThat(relation.fields().get(1), isField("a2"));
        assertThat(relation.tableRelation().tableInfo(), is(T1_INFO));
    }
}
