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

package io.crate.integrationtests;

import com.carrotsearch.randomizedtesting.annotations.Seed;
import io.crate.testing.DataTypeTesting;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.List;
import java.util.function.Supplier;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static com.carrotsearch.randomizedtesting.RandomizedTest.$$;
import static io.crate.testing.DataTypeTesting.randomType;
import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
@Seed("54904E791E52DEFD")
public class LuceneQueryBuilderIntegrationTest extends SQLTransportIntegrationTest {

    private static final int NUMBER_OF_BOOLEAN_CLAUSES = 10_000;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                       .put(super.nodeSettings(nodeOrdinal))
                       .put(SearchModule.INDICES_MAX_CLAUSE_COUNT_SETTING.getKey(), NUMBER_OF_BOOLEAN_CLAUSES)
                       .build();
    }

    @Test
    public void testWhereFunctionWithAnalyzedColumnArgument() throws Exception {
        execute("create table t (text string index using fulltext) " +
                "clustered into 1 shards with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t (text) values ('hello world')");
        refresh();

        execute("select text from t where substr(text, 1, 1) = 'h'");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testEqualsQueryOnArrayType() throws Exception {
        execute("create table t (a array(integer)) with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t (a) values (?)", new Object[][]{
            new Object[]{new Object[]{10, 10, 20}},
            new Object[]{new Object[]{40, 50, 60}},
            new Object[]{new Object[]{null, null}}
        });
        execute("refresh table t");

        execute("select * from t where a = [10, 10, 20]");
        assertThat(response.rowCount(), is(1L));

        execute("select * from t where a = [10, 20]");
        assertThat(response.rowCount(), is(0L));

        execute("select * from t where a = [null, null]");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testWhereFunctionWithIndexOffColumn() throws Exception {
        execute("create table t (text string index off) " +
                "clustered into 1 shards with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t (text) values ('hello world')");
        refresh();

        execute("select text from t where substr(text, 1, 1) = 'h'");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testWhereFunctionWithIndexReference() throws Exception {
        execute("create table t (text string, index text_ft using fulltext (text)) " +
                "clustered into 2 shards with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t (text) values ('hello world')");
        execute("insert into t (text) values ('harr')");
        execute("insert into t (text) values ('hh')");
        refresh();

        execute("select text from t where substr(text_ft, 1, 1) = 'h'");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testAnyFunctionWithSubscript() throws Exception {
        execute("create table t (a array(object as (b array(object as (n integer))))) " +
                "clustered into 1 shards with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t (a) values ([{b=[{n=1}, {n=2}, {n=3}]}])");
        execute("insert into t (a) values ([{b=[{n=3}, {n=4}, {n=5}]}])");
        refresh();

        execute("select * from t where 3 = any(a[1]['b']['n'])");
        assertThat(response.rowCount(), is(2L));
        execute("select a[1]['b']['n'] from t where 1 = any(a[1]['b']['n'])");
        assertThat(response.rowCount(), is(1L));
        assertThat(((List) response.rows()[0][0]).get(0), is(1));
        assertThat(((List) response.rows()[0][0]).get(1), is(2));
        assertThat(((List) response.rows()[0][0]).get(2), is(3));
    }

    @Test
    public void testWhereSubstringWithSysColumn() throws Exception {
        execute("create table t (dummy string) clustered into 2 shards with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t (dummy) values ('yalla')");
        refresh();

        execute("select dummy from t where substr(_uid, 1, 1) != '{'");
        assertThat(response.rowCount(), is(1L));
        assertThat(((String) response.rows()[0][0]), is("yalla"));
    }

    @Test
    public void testInWithArgs() throws Exception {
        execute("create table t (i int) clustered into 1 shards with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t values (1), (2)");
        execute("refresh table t");

        StringBuilder sb = new StringBuilder("select i from t where i in (");

        int i = 0;
        for (; i < 1500; i++) {
            sb.append(i);
            sb.append(',');
        }
        sb.append(i);
        sb.append(')');

        execute(sb.toString());
        assertThat(response.rowCount(), is(2L));
    }

    @Test
    public void testWithinGenericFunction() throws Exception {
        execute("create table shaped (id int, point geo_point, shape geo_shape) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into shaped (id, point, shape) VALUES (?, ?, ?)", $$(
            $(1, "POINT (15 15)", "polygon (( 10 10, 10 20, 20 20, 20 15, 10 10))"),
            $(1, "POINT (-10 -10)", "polygon (( 10 10, 10 20, 20 20, 20 15, 10 10))")
        ));
        execute("refresh table shaped");

        execute("select * from shaped where within(point, shape) order by id");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testWithinQueryMatches() throws Exception {
        // test a regression where wrong lucene query was used and therefore did not return any results
        execute("CREATE TABLE locations (id INT, point GEO_POINT) WITH (number_of_replicas=0)");
        ensureYellow();
        execute("INSERT INTO locations (id, point) VALUES (?, ?)", $$(
            $(1, "POINT(-71.06244564056396 42.35373619523924)")
        ));
        execute("REFRESH TABLE locations");
        execute("SELECT * FROM locations WHERE within(point, 'POLYGON((" +
                "-71.06042861938477 42.35473836290108," +
                "-71.05982780456543 42.35251834962908," +
                "-71.06463432312012 42.35213776805158," +
                "-71.06403350830078 42.35359665158396," +
                "-71.06042861938477 42.35473836290108))')");
        assertThat(response.rowCount(), is(1L));

    }

    @Test
    public void testObjectEq() throws Exception {
        execute("create table t (o object as (x int, y long))");
        ensureYellow();

        execute("insert into t (o) values ({x=10, y=20})");
        execute("refresh table t");

        assertThat(execute("select * from t where o = {x=10, y=20}").rowCount(), is(1L));
    }

    @Test
    public void testFunctionWhereIn() throws Exception {
        execute("create table t (x string) with (number_of_replicas = 0)");
        ensureYellow();

        execute("insert into t (x) values ('x'), ('y')");
        execute("refresh table t");

        execute("select * from t where concat(x, '') in ('x', 'y')");
        assertThat(response.rowCount(), is(2L));
    }

    @Test
    public void testWhereINWithNullArguments() throws Exception {
        execute("create table t (x int) with (number_of_replicas = 0)");
        ensureYellow();

        execute("insert into t (x) values (1), (2)");
        execute("refresh table t");

        execute("select * from t where x in (1, null)");
        assertThat(printedTable(response.rows()), is("1\n"));

        execute("select * from t where x in (3, null)");
        assertThat(response.rowCount(), is(0L));

        execute("select * from t where coalesce(x in (3, null), true)");
        assertThat(response.rowCount(), is(2L));
    }

    @Test
    public void testQueriesOnColumnThatDoesNotExistInAllPartitions() throws Exception {
        // LuceneQueryBuilder uses a MappedFieldType to generate queries
        // this MappedFieldType is not available on partitions that are missing fields
        // this test verifies that this case works correctly

        execute("create table t (p int) " +
                "clustered into 1 shards " +
                "partitioned by (p) " +
                "with (number_of_replicas = 0, column_policy = 'dynamic') ");
        execute("insert into t (p) values (1)");
        execute("insert into t (p, x, numbers, obj, objects, s, b) " +
                "values (2, 10, [10, 20, 30], {x=10}, [{x=10}, {x=20}], 'foo', true)");
        ensureYellow();
        execute("refresh table t");

        // match on partition with the columns

        assertThat(printedTable(execute("select p from t where x = 10").rows()), is("2\n"));
        // range queries all hit the same code path, so only > is tested
        assertThat(printedTable(execute("select p from t where x > 9").rows()), is("2\n"));
        assertThat(printedTable(execute("select p from t where x is not null").rows()), is("2\n"));
        assertThat(printedTable(execute("select p from t where x::string like 10").rows()), is("2\n"));
        assertThat(printedTable(execute("select p from t where s like 'f%'").rows()), is("2\n"));
        assertThat(printedTable(execute("select p from t where s ilike 'F%'").rows()), is("2\n"));
        assertThat(printedTable(execute("select p from t where obj = {x=10}").rows()), is("2\n"));
        assertThat(printedTable(execute("select p from t where b").rows()), is("2\n"));

        assertThat(printedTable(execute("select p from t where 10 = any(numbers)").rows()), is("2\n"));
        assertThat(printedTable(execute("select p from t where 10 != any(numbers)").rows()), is("2\n"));
        assertThat(printedTable(execute("select p from t where 15 > any(numbers)").rows()), is("2\n"));

        assertThat(printedTable(execute("select p from t where x = any([10, 20])").rows()), is("2\n"));
        assertThat(printedTable(execute("select p from t where x != any([20, 30])").rows()), is("2\n"));
        assertThat(printedTable(execute("select p from t where x > any([1, 2])").rows()), is("2\n"));


        // match on partitions where the column does not exist
        assertThat(printedTable(execute("select p from t where x is null").rows()), is("1\n"));
        assertThat(printedTable(execute("select p from t where obj is null").rows()), is("1\n"));
    }

    @Test
    public void testWhereNotIdInFunction() throws Exception {
        execute("create table t (dummy string) clustered into 2 shards with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t (dummy) values ('yalla')");
        refresh();

        execute("select dummy from t where substr(_id, 1, 1) != '{'");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][0], is("yalla"));
    }

    @Test
    public void testWhereNotEqualAnyWithLargeArray() throws Exception {
        // Test overriding of default value 8192 for indices.query.bool.max_clause_count
        execute("create table t1 (id integer) clustered into 2 shards with (number_of_replicas = 0)");
        execute("create table t2 (id integer) clustered into 2 shards with (number_of_replicas = 0)");
        ensureYellow();

        int bulkSize = NUMBER_OF_BOOLEAN_CLAUSES;
        Object[][] bulkArgs = new Object[bulkSize][];
        for (int i = 0; i < bulkSize; i++) {
            bulkArgs[i] = new Object[]{i};
        }
        execute("insert into t1 (id) values (?)", bulkArgs);
        execute("insert into t2 (id) values (1)");
        execute("refresh table t1, t2");

        execute("select count(*) from t2 where id != any(select id from t1)");
        assertThat(response.rows()[0][0], is(1L));
    }

    @Test
    public void testNullOperators() throws Exception {
        DataType<?> type = randomType();
        execute("create table t1 (c " + type.getName() + ") with (number_of_replicas = 0)");
        Supplier dataGenerator = DataTypeTesting.getDataGenerator(type);

        Object[][] bulkArgs = $$($(dataGenerator.get()), $(dataGenerator.get()), new Object[]{null});
        execute("insert into t1 (c) values (?)", bulkArgs);
        execute("refresh table t1");

        execute("select count(*) from t1 where c is null");
        assertThat(printedTable(response.rows()), is("1\n"));

        execute("select count(*) from t1 where c is not null");
        assertThat(printedTable(response.rows()), is("2\n"));
    }

    @Test
    public void testIsNotNullFilterMatchesNotNullRecordOnArrayObjectColumn() {
        execute("create table bag (id short primary key, ob array (object))");
        execute("insert into bag (id) values (1)");
        execute("insert into bag (id, ob) values (2, [{bbb = 2}])");
        execute("refresh table bag");

        execute("SELECT id, ob FROM bag WHERE ob IS NOT NULL");
        assertThat(printedTable(response.rows()), is("2| [{bbb=2}]\n"));
    }

    @Test
    public void testNotEqualAnyWithAndWithoutThreeValuedLogic() {
        execute("create table t1 (a array(integer)) clustered into 2 shards with (number_of_replicas = 0)");
        ensureYellow();

        execute("insert into t1(a) values ([1, 2, 3])");
        execute("insert into t1(a) values ([1, 2, 3, null])");
        execute("insert into t1(a) values ([4, 5])");
        execute("insert into t1(a) values ([4, 5, null])");
        execute("refresh table t1");

        execute("select * from t1 where not 5 = any(a)");
        assertThat(printedTable(response.rows()), is("[1, 2, 3]\n"));
        execute("select * from t1 where not ignore3vl(5 = any(a))");
        assertThat(printedTable(response.rows()), anyOf(is("[1, 2, 3, null]\n" +
                                                           "[1, 2, 3]\n"),
                                                        is("[1, 2, 3]\n" +
                                                           "[1, 2, 3, null]\n")));
    }

    @Test
    public void testArrayElementComparisons() {
        execute("create table t1 (a array(long)) clustered into 1 shards with (number_of_replicas = 0)");
        execute("insert into t1(a) values ([1, 2, 3])");
        execute("insert into t1(a) values ([3, 4, 5, 1])");
        execute("insert into t1(a) values ([6, 7, 8])");
        execute("refresh table t1");

        execute("select * from t1 where a[1] = 1");
        assertThat(printedTable(response.rows()), is("[1, 2, 3]\n"));

        execute("select * from t1 where a[1] != 1");
        assertThat(printedTable(response.rows()), is("[3, 4, 5, 1]\n" +
                                                     "[6, 7, 8]\n"));

        execute("select * from t1 where a[1] > 1");
        assertThat(printedTable(response.rows()), is("[3, 4, 5, 1]\n" +
                                                     "[6, 7, 8]\n"));

        execute("select * from t1 where a[3] >= 3");
        assertThat(printedTable(response.rows()), is("[1, 2, 3]\n" +
                                                     "[3, 4, 5, 1]\n" +
                                                     "[6, 7, 8]\n"));

        execute("select * from t1 where a[1] < 3");
        assertThat(printedTable(response.rows()), is("[1, 2, 3]\n"));

        execute("select * from t1 where a[2] <= 4");
        assertThat(printedTable(response.rows()), is("[1, 2, 3]\n" +
                                                     "[3, 4, 5, 1]\n"));
    }

    @Test
    public void testAnyOnNestedArray() {
        execute("create table t (obj array(object as (xs array(integer))))");
        execute("insert into t (obj) values ([{xs = [1, 2, 3]}, {xs = [3, 4]}])");
        execute("refresh table t");

        assertThat(
            "query matches",
            printedTable(execute("select * from t where [1, 2, 3] = any(obj['xs'])").rows()),
            is("[{xs=[1, 2, 3]}, {xs=[3, 4]}]\n")
        );

        assertThat(
            "query doesn't match",
            printedTable(execute("select * from t where [1, 2] = any(obj['xs'])").rows()),
            is("")
        );
    }
}
