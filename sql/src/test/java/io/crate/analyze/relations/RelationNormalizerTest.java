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

import io.crate.analyze.BaseAnalyzerTest;
import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.testing.MockedClusterServiceModule;
import io.crate.testing.T3;
import org.elasticsearch.common.inject.Module;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.crate.testing.T3.META_DATA_MODULE;
import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.instanceOf;

public class RelationNormalizerTest extends BaseAnalyzerTest {

    @Override
    protected List<Module> getModules() {
        List<Module> modules = super.getModules();
        modules.addAll(Arrays.<Module>asList(
            new MockedClusterServiceModule(),
            META_DATA_MODULE,
            new ScalarFunctionModule(),
            new AggregationImplModule(),
            new OperatorModule()
        ));
        return modules;
    }

    private QueriedRelation normalize(String stmt) {
        SelectAnalyzedStatement statement = analyze(stmt);
        QueriedRelation relation = statement.relation();
        return relation;
    }

    @Test
    public void testOrderByPushDown() throws Exception {
        QueriedRelation relation = normalize(
            "select * from (select * from t1 limit 10 offset 5) as tt order by x");
        assertThat(relation, instanceOf(QueriedDocTable.class));
        assertThat(relation.querySpec(), isSQL("SELECT doc.t1.a, doc.t1.x, doc.t1.i ORDER BY doc.t1.x LIMIT 10 OFFSET 5"));
    }

    @Test
    public void testOrderByMerge() throws Exception {
        QueriedRelation relation = normalize(
            "select x from (select * from (select concat(a, a) as aa, x from t1) as t order by aa) as tt order by x");
        assertThat(relation, instanceOf(QueriedDocTable.class));
        assertThat(relation.querySpec(), isSQL("SELECT doc.t1.x ORDER BY concat(doc.t1.a, doc.t1.a), doc.t1.x"));
    }

    @Test
    public void testLimitOffsetMerge() throws Exception {
        QueriedRelation relation = normalize(
            "select * from (select * from t1 limit 10 offset 5) as tt limit 5 offset 2");
        assertThat(relation, instanceOf(QueriedDocTable.class));
        assertThat(relation.querySpec(), isSQL("SELECT doc.t1.a, doc.t1.x, doc.t1.i LIMIT 5 OFFSET 7"));
    }

    @Test
    public void testOutputsMerge() throws Exception {
        QueriedRelation relation = normalize(
            "select aa, (xxi + 1)" +
            " from (select (xx + i) as xxi, concat(a, a) as aa" +
            "  from (select a, i, (x + x) as xx from t1) as t) as tt");
        assertThat(relation, instanceOf(QueriedDocTable.class));
        assertThat(relation.querySpec(), isSQL("SELECT concat(doc.t1.a, doc.t1.a), add(add(add(doc.t1.x, doc.t1.x), doc.t1.i), 1)"));
    }

    @Test
    public void testWhereMerge() throws Exception {
        QueriedRelation relation = normalize(
            "select x from (select x, (i + i) as ii from t1 where a = 'a') as tt where ii > 10");
        assertThat(relation, instanceOf(QueriedDocTable.class));
        assertThat(relation.querySpec(), isSQL("SELECT doc.t1.x WHERE ((doc.t1.a = 'a') AND (add(doc.t1.i, doc.t1.i) > 10))"));
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
        assertThat(relation.querySpec(), isSQL("SELECT sum(doc.t1.i), doc.t1.x WHERE (doc.t1.x = 10) GROUP BY doc.t1.x"));
    }

    @Test
    public void testOrderByOnAggregation() throws Exception {
        QueriedRelation relation = normalize(
            "select * from (select sum(i) as ii from t1 group by x) as tt order by ii");
        assertThat(relation, instanceOf(QueriedDocTable.class));
        assertThat(relation.querySpec(), isSQL("SELECT sum(doc.t1.i) GROUP BY doc.t1.x ORDER BY sum(doc.t1.i)"));
    }

    @Test
    public void testOrderByNonGroupedField() throws Exception {
        QueriedRelation relation = normalize(
            "select count(*), avg(x) as avgX from ( " +
            "  select * from t1 order by i) as tt");
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
            "SELECT concat(doc.t1.a, doc.t2.b) WHERE ((doc.t1.a = 'a') OR (doc.t2.b = 'aa')) ORDER BY add(add(doc.t1.i, doc.t2.i), doc.t2.y)"));
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
        assertThat(((MultiSourceSelect)relation).joinPairs().get(0).condition(), isSQL("(doc.t1.a = doc.t2.b)"));

        relation = normalize(
            "select ab " +
            "from (select (ii + y) as iiy, concat(a, b) as ab " +
            "  from (select t1.a, t2.b, t2.y, (t1.i + t2.i) as ii " +
            "    from t1 right join t2 on t1.a = t2.b) as t) as tt " +
            "order by iiy");
        assertThat(relation, instanceOf(MultiSourceSelect.class));
        assertThat(relation.querySpec(), isSQL(
            "SELECT concat(doc.t1.a, doc.t2.b) ORDER BY add(add(doc.t1.i, doc.t2.i), doc.t2.y)"));
        assertThat(((MultiSourceSelect)relation).joinPairs().get(0).condition(), isSQL("(doc.t1.a = doc.t2.b)"));

        relation = normalize(
            "select ab " +
            "from (select (ii + y) as iiy, concat(a, b) as ab " +
            "  from (select t1.a, t2.b, t2.y, (t1.i + t2.i) as ii " +
            "    from t1 full join t2 on t1.a = t2.b) as t) as tt " +
            "order by iiy");
        assertThat(relation, instanceOf(MultiSourceSelect.class));
        assertThat(relation.querySpec(), isSQL(
            "SELECT concat(doc.t1.a, doc.t2.b) ORDER BY add(add(doc.t1.i, doc.t2.i), doc.t2.y)"));
        assertThat(((MultiSourceSelect)relation).joinPairs().get(0).condition(), isSQL("(doc.t1.a = doc.t2.b)"));
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
        MultiSourceSelect.Source t1 = ((MultiSourceSelect) relation).sources().get(T3.T1);
        assertThat(t1.querySpec().where().query(), isSQL("(true AND (doc.t1.a = 'a'))"));
        MultiSourceSelect.Source t2 = ((MultiSourceSelect) relation).sources().get(T3.T2);
        assertThat(t2.querySpec().where().query(), isSQL("(doc.t2.y > 60)"));
    }

    @Test
    public void testSubSelectOnLeftJoinWithFilter() throws Exception {
        QueriedRelation relation = normalize(
            "select col1, col2 from ( " +
            "  select t1.a as col1, t2.i as col2, t2.y as col3 " +
            "  from t1 left join t2 on t1.a = t2.b where t2.y > 60) as t " +
            "where col1 = 'a' order by col3");
        assertThat(relation, instanceOf(MultiSourceSelect.class));
        assertThat(relation.querySpec(), isSQL(
            "SELECT doc.t1.a, doc.t2.i WHERE ((doc.t2.y > 60) AND true) ORDER BY doc.t2.y"));
        assertThat(((MultiSourceSelect) relation).joinPairs().get(0).condition(), isSQL("(doc.t1.a = doc.t2.b)"));

        // make sure that where clause was pushed down and didn't disappear somehow
        MultiSourceSelect.Source t1 = ((MultiSourceSelect) relation).sources().get(T3.T1);
        assertThat(t1.querySpec().where().query(), isSQL("(doc.t1.a = 'a')"));
        MultiSourceSelect.Source t2 = ((MultiSourceSelect) relation).sources().get(T3.T2);
        assertThat(t2.querySpec().where().query(), isSQL("null"));
    }

    @Test
    public void testSubSelectOnRightJoinWithFilter() throws Exception {
        QueriedRelation relation = normalize(
            "select col1, col2 from ( " +
            "  select t1.a as col1, t2.i as col2, t2.y as col3 " +
            "  from t1 right join t2 on t1.a = t2.b where t1.x > 60) as t " +
            "where col2 = 10 order by col3");
        assertThat(relation, instanceOf(MultiSourceSelect.class));
        assertThat(relation.querySpec(), isSQL(
            "SELECT doc.t1.a, doc.t2.i WHERE ((doc.t1.x > 60) AND true) ORDER BY doc.t2.y"));
        assertThat(((MultiSourceSelect)relation).joinPairs().get(0).condition(), isSQL("(doc.t1.a = doc.t2.b)"));

        // make sure that where clause was pushed down and didn't disappear somehow
        MultiSourceSelect.Source t1 = ((MultiSourceSelect) relation).sources().get(T3.T1);
        assertThat(t1.querySpec().where().query(), isSQL("null"));
        MultiSourceSelect.Source t2 = ((MultiSourceSelect) relation).sources().get(T3.T2);
        assertThat(t2.querySpec().where().query(), isSQL("(doc.t2.i = 10)"));
    }

    @Test
    public void testSubSelectOnFullJoinWithFilter() throws Exception {
        QueriedRelation relation = normalize(
            "select col1, col2 from ( " +
            "  select t1.a as col1, t2.i as col2, t2.y as col3 " +
            "  from t1 full join t2 on t1.a = t2.b where t2.y > 60) as t " +
            "where col1 = 'a' order by col3");
        assertThat(relation, instanceOf(MultiSourceSelect.class));
        assertThat(relation.querySpec(), isSQL(
            "SELECT doc.t1.a, doc.t2.i WHERE ((doc.t2.y > 60) AND (doc.t1.a = 'a')) ORDER BY doc.t2.y"));

        // make sure that where clause wasn't pushed down since but be applied after the FULL join
        MultiSourceSelect.Source t1 = ((MultiSourceSelect) relation).sources().get(T3.T1);
        assertThat(t1.querySpec().where().query(), isSQL("null"));
        MultiSourceSelect.Source t2 = ((MultiSourceSelect) relation).sources().get(T3.T2);
        assertThat(t2.querySpec().where().query(), isSQL("null"));
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
            "SELECT doc.t1.a, doc.t2.i ORDER BY doc.t2.y LIMIT 5 OFFSET 7"));
    }
}
