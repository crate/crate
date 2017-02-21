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

package io.crate.analyze.relations;

import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.RelationSource;
import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;
import org.junit.Before;
import org.junit.Test;

import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.*;

public class RelationNormalizerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor executor;

    @Before
    public void prepare() {
        executor = SQLExecutor.builder(clusterService).enableDefaultTables().build();
    }
    private QueriedRelation normalize(String stmt) {
        SelectAnalyzedStatement statement = executor.analyze(stmt);
        return statement.relation();
    }

    @Test
    public void testOrderByPushDown() throws Exception {
        QueriedRelation relation = normalize(
            "select * from (select * from t1 limit 10 offset 5) as tt order by x");
        assertThat(relation, instanceOf(QueriedDocTable.class));
        assertThat(relation.querySpec(),
            isSQL("SELECT doc.t1.a, doc.t1.x, doc.t1.i ORDER BY doc.t1.x LIMIT 10 OFFSET 5"));
    }

    @Test
    public void testOrderByMerge() throws Exception {
        QueriedRelation relation = normalize(
            "select x from (select * from (select concat(a, a) as aa, x from t1) as t order by aa) as tt order by x");
        assertThat(relation, instanceOf(QueriedDocTable.class));
        assertThat(relation.querySpec(),
            isSQL("SELECT doc.t1.x ORDER BY doc.t1.x"));
    }

    @Test
    public void testLimitOffsetMerge() throws Exception {
        QueriedRelation relation = normalize(
            "select * from (select * from t1 limit 10 offset 5) as tt limit 5 offset 2");
        assertThat(relation, instanceOf(QueriedDocTable.class));
        assertThat(relation.querySpec(),
            isSQL("SELECT doc.t1.a, doc.t1.x, doc.t1.i LIMIT least(10, 5) OFFSET add(5, 2)"));
    }

    @Test
    public void testOrderByLimitsOnInnerNotMerged() throws Exception {
        QueriedRelation relation = normalize("select * from (" +
                                                "select * from (" +
                                                    "select * from t1 order by a limit 10" +
                                                ") as tt" +
                                             ") as ttt order by x");
        assertThat(relation, instanceOf(QueriedSelectRelation.class));
        QueriedSelectRelation outerRelation = (QueriedSelectRelation) relation;
        assertThat(outerRelation.querySpec(),
            isSQL("SELECT io.crate.analyze.QueriedSelectRelation.a, " +
                  "io.crate.analyze.QueriedSelectRelation.x, " +
                  "io.crate.analyze.QueriedSelectRelation.i ORDER BY io.crate.analyze.QueriedSelectRelation.x"));
        assertThat(outerRelation.subRelation(), instanceOf(QueriedDocTable.class));
        assertThat(outerRelation.subRelation().querySpec(),
            isSQL("SELECT doc.t1.a, doc.t1.x, doc.t1.i ORDER BY doc.t1.a LIMIT 10"));
    }

    @Test
    public void testOrderByMultiLimitsSameOrderByMerge() throws Exception {
        QueriedRelation relation = normalize("select * from (" +
                                                "select * from (" +
                                                    "select * from t1 order by a offset 5" +
                                                ") as tt" +
                                             ") as ttt order by a limit 5");
        assertThat(relation, instanceOf(QueriedDocTable.class));
        assertThat(relation.querySpec(),
            isSQL("SELECT doc.t1.a, doc.t1.x, doc.t1.i ORDER BY doc.t1.a LIMIT 5 OFFSET 5"));
    }

    @Test
    public void testOrderByLimitsNotMerged() throws Exception {
        QueriedRelation relation = normalize("select * from (" +
                                                "select * from (" +
                                                    "select * from t1 order by a limit 10 offset 5" +
                                                ") as tt" +
                                             ") as ttt order by a desc limit 5");
        assertThat(relation, instanceOf(QueriedSelectRelation.class));
        QueriedSelectRelation outerRelation = (QueriedSelectRelation) relation;
        assertThat(outerRelation.querySpec(),
            isSQL("SELECT io.crate.analyze.QueriedSelectRelation.a, " +
                  "io.crate.analyze.QueriedSelectRelation.x, " +
                  "io.crate.analyze.QueriedSelectRelation.i " +
                  "ORDER BY io.crate.analyze.QueriedSelectRelation.a DESC LIMIT 5"));
        assertThat(outerRelation.subRelation(), instanceOf(QueriedDocTable.class));
        assertThat(outerRelation.subRelation().querySpec(),
            isSQL("SELECT doc.t1.a, doc.t1.x, doc.t1.i ORDER BY doc.t1.a LIMIT 10 OFFSET 5"));
    }

    @Test
    public void testOutputsMerge() throws Exception {
        QueriedRelation relation = normalize(
            "select aa, (xxi + 1)" +
            " from (select (xx + i) as xxi, concat(a, a) as aa" +
            "  from (select a, i, (x + x) as xx from t1) as t) as tt");
        assertThat(relation, instanceOf(QueriedDocTable.class));
        assertThat(relation.querySpec(),
            isSQL("SELECT concat(doc.t1.a, doc.t1.a), add(add(add(doc.t1.x, doc.t1.x), doc.t1.i), 1)"));
    }

    @Test
    public void testWhereMerge() throws Exception {
        QueriedRelation relation = normalize(
            "select x from (select x, (i + i) as ii from t1 where a = 'a') as tt where ii > 10");
        assertThat(relation, instanceOf(QueriedDocTable.class));
        assertThat(relation.querySpec(),
            isSQL("SELECT doc.t1.x WHERE ((add(doc.t1.i, doc.t1.i) > 10) AND (doc.t1.a = 'a'))"));
    }

    @Test
    public void testGroupByPushUp() throws Exception {
        QueriedRelation relation = normalize(
            "select * from (select sum(i) as ii from t1 group by x) as tt");
        assertThat(relation, instanceOf(QueriedDocTable.class));
        assertThat(relation.querySpec(), isSQL("SELECT sum(doc.t1.i) GROUP BY doc.t1.x"));
    }

    @Test
    public void testGroupByPushDown() throws Exception {
        QueriedRelation relation = normalize(
            "select sum(i) from (select i, x from t1) as tt group by x");
        assertThat(relation, instanceOf(QueriedDocTable.class));
        assertThat(relation.querySpec(), isSQL("SELECT sum(doc.t1.i) GROUP BY doc.t1.x"));
    }

    @Test
    public void testHavingPushUp() throws Exception {
        QueriedRelation relation = normalize(
            "select * from (select i from t1 group by i having i > 10) as tt");
        assertThat(relation, instanceOf(QueriedDocTable.class));
        assertThat(relation.querySpec(), isSQL("SELECT doc.t1.i GROUP BY doc.t1.i HAVING (doc.t1.i > 10)"));
    }

    @Test
    public void testHavingPushDown() throws Exception {
        QueriedRelation relation = normalize(
            "select i from (select * from t1) as tt group by i having i > 10");
        assertThat(relation, instanceOf(QueriedDocTable.class));
        assertThat(relation.querySpec(), isSQL("SELECT doc.t1.i GROUP BY doc.t1.i HAVING (doc.t1.i > 10)"));
    }

    @Test
    public void testRewritableNestedAggregation() throws Exception {
        QueriedRelation relation = normalize(
            "select count(*) from (select sum(i) as ii from t1 group by x) as tt");
        assertThat(relation, instanceOf(QueriedSelectRelation.class));
    }

    @Test
    public void testNestedGroupByAggregation() throws Exception {
        QueriedRelation relation = normalize(
            "select i from (select i, x from t1 group by i, x) as tt group by i");
        assertThat(relation, instanceOf(QueriedSelectRelation.class));
    }

    @Test
    public void testFilterOnAggregatedField() throws Exception {
        QueriedRelation relation = normalize(
            "select ii, xx from ( " +
            "  select i + i as ii, xx from (" +
            "    select i, sum(x) as xx from t1 group by i) as t) as tt " +
            "where (ii * 2) > 4 and (xx * 2) > 120");
        assertThat(relation, instanceOf(QueriedSelectRelation.class));
    }

    @Test
    public void testFilterOnNotAggregatedField() throws Exception {
        QueriedRelation relation = normalize(
            "select * from (select sum(i) as ii, x from t1 group by x) as tt where x = 10");
        assertThat(relation, instanceOf(QueriedDocTable.class));
        assertThat(relation.querySpec(),
            isSQL("SELECT sum(doc.t1.i), doc.t1.x WHERE (doc.t1.x = 10) GROUP BY doc.t1.x"));
    }

    @Test
    public void testOrderByOnAggregation() throws Exception {
        QueriedRelation relation = normalize(
            "select * from (select sum(i) as ii from t1 group by x) as tt order by ii");
        assertThat(relation, instanceOf(QueriedDocTable.class));
        assertThat(relation.querySpec(),
            isSQL("SELECT sum(doc.t1.i) GROUP BY doc.t1.x ORDER BY sum(doc.t1.i)"));
    }

    @Test
    public void testOrderByGroupedField() throws Exception {
        QueriedRelation relation = normalize(
            "select x from (" +
                  "    select * from t1 order by i) as tt " +
                  "group by x " +
                  "order by x");
        assertThat(relation, instanceOf(QueriedDocTable.class));
        assertThat(relation.querySpec(),
            isSQL("SELECT doc.t1.x GROUP BY doc.t1.x ORDER BY doc.t1.x"));
    }

    @Test
    public void testOrderByNonGroupedField() throws Exception {
        QueriedRelation relation = normalize(
            "select count(*), avg(x) as avgX from ( select * from (" +
            "  select * from t1 order by i) as tt) as ttt");
        assertThat(relation, instanceOf(QueriedDocTable.class));
        assertThat(relation.querySpec(),
            isSQL("SELECT count(), avg(doc.t1.x)"));
    }

    @Test
    public void testGlobalAggOnSubQueryWithLimit() throws Exception {
        // need to apply limit before aggregation is done
        QueriedRelation relation = normalize("select sum(x) from (select x from t1 limit 1) t");
        assertThat(relation, instanceOf(QueriedSelectRelation.class));
    }

    @Test
    public void testSubSelectOnJoins() throws Exception {
        QueriedRelation relation = normalize(
            "select ab " +
            "from (select (ii + y) as iiy, concat(a, b) as ab " +
            "  from (select t1.a, t2.b, t2.y, (t1.i + t2.i) as ii " +
            "    from t1, t2 where t1.a='a' or t2.b='aa') as t) as tt " +
            "order by iiy");
        assertThat(relation, instanceOf(MultiSourceSelect.class));
        assertThat(relation.querySpec(), isSQL(
            "SELECT concat(doc.t1.a, doc.t2.b) WHERE ((doc.t1.a = 'a') OR (doc.t2.b = 'aa')) " +
            "ORDER BY add(add(doc.t1.i, doc.t2.i), doc.t2.y)"));
    }

    @Test
    public void testSubSelectOnOuterJoins() throws Exception {
        QueriedRelation relation = normalize(
            "select ab " +
            "from (select (ii + y) as iiy, concat(a, b) as ab " +
            "  from (select t1.a, t2.b, t2.y, (t1.i + t2.i) as ii " +
            "    from t1 left join t2 on t1.a = t2.b) as t) as tt " +
            "order by iiy");
        assertThat(relation, instanceOf(MultiSourceSelect.class));
        assertThat(relation.querySpec(), isSQL(
            "SELECT concat(doc.t1.a, doc.t2.b) ORDER BY add(add(doc.t1.i, doc.t2.i), doc.t2.y)"));
        assertThat(((MultiSourceSelect) relation).joinPairs().get(0).condition(), isSQL("(doc.t1.a = doc.t2.b)"));

        relation = normalize(
            "select ab " +
            "from (select (ii + y) as iiy, concat(a, b) as ab " +
            "  from (select t1.a, t2.b, t2.y, (t1.i + t2.i) as ii " +
            "    from t1 right join t2 on t1.a = t2.b) as t) as tt " +
            "order by iiy");
        assertThat(relation, instanceOf(MultiSourceSelect.class));
        assertThat(relation.querySpec(), isSQL(
            "SELECT concat(doc.t1.a, doc.t2.b) ORDER BY add(add(doc.t1.i, doc.t2.i), doc.t2.y)"));
        assertThat(((MultiSourceSelect) relation).joinPairs().get(0).condition(), isSQL("(doc.t1.a = doc.t2.b)"));

        relation = normalize(
            "select ab " +
            "from (select (ii + y) as iiy, concat(a, b) as ab " +
            "  from (select t1.a, t2.b, t2.y, (t1.i + t2.i) as ii " +
            "    from t1 full join t2 on t1.a = t2.b) as t) as tt " +
            "order by iiy");
        assertThat(relation, instanceOf(MultiSourceSelect.class));
        assertThat(relation.querySpec(), isSQL(
            "SELECT concat(doc.t1.a, doc.t2.b) ORDER BY add(add(doc.t1.i, doc.t2.i), doc.t2.y)"));
        assertThat(((MultiSourceSelect) relation).joinPairs().get(0).condition(), isSQL("(doc.t1.a = doc.t2.b)"));
    }

    @Test
    public void testSubSelectOnJoinsWithFilter() throws Exception {
        QueriedRelation relation = normalize(
            "select col1, col2 from ( " +
            "  select t1.a as col1, t2.i as col2, t2.y as col3 " +
            "  from t1, t2 where t2.y > 60) as t " +
            "where col1 = 'a' order by col3");
        assertThat(relation, instanceOf(MultiSourceSelect.class));
        assertThat(relation.querySpec(), isSQL(
            "SELECT doc.t1.a, doc.t2.i ORDER BY doc.t2.y"));

        // make sure that where clause was pushed down and didn't disappear somehow
        RelationSource t1 = ((MultiSourceSelect) relation).sources().get(T3.T1);
        assertThat(t1.querySpec().where().query(), isSQL("(doc.t1.a = 'a')"));
        RelationSource t2 = ((MultiSourceSelect) relation).sources().get(T3.T2);
        assertThat(t2.querySpec().where().query(), isSQL("(true AND (doc.t2.y > 60))"));
    }

    @Test
    public void testSubSelectOnLeftJoinWithFilterRewrittenToInner() throws Exception {
        QueriedRelation relation = normalize(
            "select col1, col2 from ( " +
            "  select t1.a as col1, t2.i as col2, t2.y as col3 " +
            "  from t1 left join t2 on t1.a = t2.b where t2.y > 60) as t " +
            "where col1 = 'a' order by col3");
        assertThat(relation, instanceOf(MultiSourceSelect.class));
        MultiSourceSelect mss = (MultiSourceSelect) relation;
        assertThat(mss.querySpec(), isSQL(
            "SELECT doc.t1.a, doc.t2.i ORDER BY doc.t2.y"));
        assertThat(mss.joinPairs().get(0).joinType(), is(JoinType.INNER));
        assertThat(mss.joinPairs().get(0).condition(), isSQL("(doc.t1.a = doc.t2.b)"));

        // make sure that where clause was pushed down and didn't disappear somehow
        RelationSource t1 = mss.sources().get(T3.T1);
        assertThat(t1.querySpec().where().query(), isSQL("(doc.t1.a = 'a')"));
        RelationSource t2 = mss.sources().get(T3.T2);
        assertThat(t2.querySpec().where().query(), isSQL("(doc.t2.y > 60)"));
    }

    @Test
    public void testSubSelectOnRightJoinWithFilterRewrittenToInner() throws Exception {
        QueriedRelation relation = normalize(
            "select col1, col2 from ( " +
            "  select t1.a as col1, t2.i as col2, t2.y as col3 " +
            "  from t1 right join t2 on t1.a = t2.b where t1.x > 60) as t " +
            "where col2 = 10 order by col3");
        assertThat(relation, instanceOf(MultiSourceSelect.class));
        MultiSourceSelect mss = (MultiSourceSelect) relation;
        assertThat(mss.querySpec(), isSQL(
            "SELECT doc.t1.a, doc.t2.i ORDER BY doc.t2.y"));
        assertThat(mss.joinPairs().get(0).joinType(), is(JoinType.INNER));
        assertThat(mss.joinPairs().get(0).condition(), isSQL("(doc.t1.a = doc.t2.b)"));

        // make sure that where clause was pushed down and didn't disappear somehow
        RelationSource t1 = mss.sources().get(T3.T1);
        assertThat(t1.querySpec().where().query(), isSQL("(doc.t1.x > 60)"));
        RelationSource t2 = mss.sources().get(T3.T2);
        assertThat(t2.querySpec().where().query(), isSQL("(doc.t2.i = 10)"));
    }

    @Test
    public void testSubSelectOnJoinsWithLimitAndOffset() throws Exception {
        QueriedRelation relation = normalize(
            "select col1, col2 from ( " +
            "  select t1.a as col1, t2.i as col2, t2.y as col3 " +
            "  from t1, t2 limit 5 offset 5) as t " +
            "order by col3 limit 10 offset 2");
        assertThat(relation, instanceOf(MultiSourceSelect.class));
        assertThat(relation.querySpec(), isSQL(
            "SELECT doc.t1.a, doc.t2.i ORDER BY doc.t2.y LIMIT least(5, 10) OFFSET add(5, 2)"));
    }

    @Test
    public void testFullJoinWithFiltersRewrittenToInner() throws Exception {
        QueriedRelation relation = normalize(
            "select t1.a, t2.i " +
            "from t1 full join t2 on t1.a = t2.b " +
            "where t2.y is not null and t1.a = 'a'");
        assertThat(relation, instanceOf(MultiSourceSelect.class));
        MultiSourceSelect mss = (MultiSourceSelect) relation;
        assertThat(mss.querySpec(), isSQL("SELECT doc.t1.a, doc.t2.i"));

        assertThat(mss.joinPairs().get(0).joinType(), is(JoinType.INNER));
        assertThat(mss.joinPairs().get(0).condition(), isSQL("(doc.t1.a = doc.t2.b)"));

        // make sure that the conditions of where clause were pushed down to the relations
        RelationSource t1 = mss.sources().get(T3.T1);
        assertThat(t1.querySpec().where().query(), isSQL("(doc.t1.a = 'a')"));
        RelationSource t2 = mss.sources().get(T3.T2);
        assertThat(t2.querySpec().where().query(), isSQL("(NOT (ISNULL doc.t2.y))"));
    }

    @Test
    public void testFullJoinWithFiltersRewrittenToLeft() throws Exception {
        QueriedRelation relation = normalize(
            "select t1.a, t2.i " +
            "from t1 full join t2 on t1.a = t2.b " +
            "where t2.y is null and t1.a = 'a'");
        assertThat(relation, instanceOf(MultiSourceSelect.class));
        MultiSourceSelect mss = (MultiSourceSelect) relation;
        assertThat(mss.querySpec(),
            isSQL("SELECT doc.t1.a, doc.t2.i WHERE (ISNULL doc.t2.y)"));

        assertThat(mss.joinPairs().get(0).joinType(), is(JoinType.LEFT));
        assertThat(mss.joinPairs().get(0).left().toString(), is("doc.t1"));
        assertThat(mss.joinPairs().get(0).right().toString(), is("doc.t2"));
        assertThat(mss.joinPairs().get(0).condition(), isSQL("(doc.t1.a = doc.t2.b)"));

        // make sure that where clause for t1 wasn't pushed down since but be applied after the FULL join
        RelationSource t1 = mss.sources().get(T3.T1);
        assertThat(t1.querySpec().where().query(), isSQL("(doc.t1.a = 'a')"));
        RelationSource t2 = mss.sources().get(T3.T2);
        assertThat(t2.querySpec().where().query(), is(nullValue()));
    }

    @Test
    public void testFullJoinWithFiltersRewrittenToRight() throws Exception {
        QueriedRelation relation = normalize(
            "select t1.a as col1, t2.i as col2 " +
            "from t1 full join t2 on t1.a = t2.b " +
            "where t1.x is null and t2.b = 'b'");
        assertThat(relation, instanceOf(MultiSourceSelect.class));
        MultiSourceSelect mss = (MultiSourceSelect) relation;
        assertThat(mss.querySpec(),
            isSQL("SELECT doc.t1.a, doc.t2.i WHERE (ISNULL doc.t1.x)"));

        assertThat(mss.joinPairs().get(0).joinType(), is(JoinType.RIGHT));
        assertThat(mss.joinPairs().get(0).left().toString(), is("doc.t1"));
        assertThat(mss.joinPairs().get(0).right().toString(), is("doc.t2"));
        assertThat(mss.joinPairs().get(0).condition(), isSQL("(doc.t1.a = doc.t2.b)"));

        // make sure that where clause for t2 wasn't pushed down since but be applied after the FULL join
        RelationSource t1 = mss.sources().get(T3.T1);
        assertThat(t1.querySpec().where().query(), is(nullValue()));
        RelationSource t2 = mss.sources().get(T3.T2);
        assertThat(t2.querySpec().where().query(), isSQL("(doc.t2.b = 'b')"));
    }

    @Test
    public void testFullJoinWithFiltersNotRewritten() throws Exception {
        QueriedRelation relation = normalize(
            "select t1.a as col1, t2.i as col2, t2.y as col3 " +
            "from t1 full join t2 on t1.a = t2.b " +
            "where t1.x is null and t2.b = null order by col3");
        assertThat(relation, instanceOf(MultiSourceSelect.class));
        MultiSourceSelect mss = (MultiSourceSelect) relation;
        assertThat(mss.querySpec(),
            isSQL("SELECT doc.t1.a, doc.t2.i, doc.t2.y WHERE ((ISNULL doc.t1.x) AND NULL) ORDER BY doc.t2.y"));

        assertThat(mss.joinPairs().get(0).joinType(), is(JoinType.FULL));
        assertThat(mss.joinPairs().get(0).left().toString(), is("doc.t1"));
        assertThat(mss.joinPairs().get(0).right().toString(), is("doc.t2"));
        assertThat(mss.joinPairs().get(0).condition(), isSQL("(doc.t1.a = doc.t2.b)"));

        // make sure that where clause wasn't pushed down since but be applied after the FULL join
        RelationSource t1 = mss.sources().get(T3.T1);
        assertThat(t1.querySpec().where().query(), is(nullValue()));
        RelationSource t2 = mss.sources().get(T3.T2);
        assertThat(t2.querySpec().where().query(), is(nullValue()));
    }
}
