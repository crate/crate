/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.integrationtests;

import io.crate.Constants;
import io.crate.action.sql.SQLActionException;
import io.crate.action.sql.SQLRequest;
import io.crate.action.sql.SQLResponse;
import io.crate.core.collections.ArrayBucket;
import io.crate.testing.TestingHelpers;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.core.Is;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.isIn;

@ElasticsearchIntegrationTest.ClusterScope(numDataNodes = 2, numClientNodes = 0)
public class GroupByAggregateTest extends SQLTransportIntegrationTest {

    private Setup setup = new Setup(sqlExecutor);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void initTestData() {
        setup.setUpEmployees();
    }

    @Test
    public void selectGroupByAggregateMinInteger() throws Exception {
        this.setup.groupBySetup("integer");

        execute("select min(age) as minage, gender from characters group by gender order by gender");
        assertArrayEquals(new String[]{"minage", "gender"}, response.cols());
        assertEquals(2L, response.rowCount());
        assertEquals("female", response.rows()[0][1]);
        assertEquals(32, response.rows()[0][0]);

        assertEquals("male", response.rows()[1][1]);
        assertEquals(34, response.rows()[1][0]);
    }

    @Test
    public void testNonDistributedGroupByWithManyKeysNoOrderByAndLimit() throws Exception {
        execute("create table t (name string, x int) clustered by (name) with (number_of_replicas = 0)");
        ensureYellow();

        execute("insert into t (name, x) values ('Marvin', 1), ('Trillian', 1), ('Ford', 1), ('Arthur', 1)");
        execute("refresh table t");

        execute("select count(*), name from t group by name, x limit 2");
        assertThat(response.rowCount(), is(2L));
    }

    @Test
    public void testGroupByOnClusteredByColumnPartOfPrimaryKey() throws Exception {
        execute("CREATE TABLE tickets ( " +
        " pk1 long, " +
        " pk2 integer, " +
        " pk3 timestamp," +
        " value string," +
        " primary key(pk1, pk2, pk3)) " +
        "CLUSTERED BY (pk2) INTO 3 SHARDS " +
        "PARTITIONED BY (pk3) " +
        "WITH (column_policy = 'strict', number_of_replicas=0)");
        ensureYellow();
        execute("insert into tickets (pk1, pk2, pk3, value) values (?, ?, ?, ?)",
        new Object[][]{
                         {1L, 42, 1425168000000L, "foo"},
                                {2L, 43, 0L, "bar"},
                                {3L, 44, 1425168000000L, "baz"},
                                {4L, 45, 499651200000L, null},
                                {5L, 42, 0L, "foo"}
                        });
        ensureYellow();
        refresh();
        execute("select pk2, count(pk2) from tickets group by pk2 order by pk2 limit 100");
        assertThat(TestingHelpers.printedTable(response.rows()), is(
                "42| 2\n" + // assert that different partitions have been merged
                "43| 1\n" +
                "44| 1\n" +
                "45| 1\n"));

        execute("create table tickets_export (c2 int, c long) with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into tickets_export (c2, c) (select pk2, count(pk2) from tickets group by pk2)");
        execute("refresh table tickets_export");

        execute("select c2, c from tickets_export order by 1");
        assertThat(TestingHelpers.printedTable(response.rows()), is(
                "42| 2\n" + // assert that different partitions have been merged
                        "43| 1\n" +
                        "44| 1\n" +
                        "45| 1\n"));
    }

    @Test
    public void selectGroupByAggregateMinFloat() throws Exception {
        this.setup.groupBySetup("float");

        execute("select min(age), gender from characters group by gender order by gender");

        String expected = "32.0| female\n34.0| male\n";

        assertEquals("min(age)", response.cols()[0]);
        assertEquals(expected, TestingHelpers.printedTable(response.rows()));
    }

    @Test
    public void selectGroupByAggregateMinDouble() throws Exception {
        this.setup.groupBySetup("double");

        execute("select min(age) as minAge, gender from characters group by gender order by gender");
        assertEquals(2L, response.rowCount());
        assertEquals(32.0d, response.rows()[0][0]);
        assertEquals(34.0d, response.rows()[1][0]);
    }

    @Test
    public void testCountDistinctGlobal() throws Exception {
        execute("select count(distinct department), count(*) from employees");

        assertEquals(1L, response.rowCount());
        assertEquals(4L, response.rows()[0][0]);
        assertEquals(6L, response.rows()[0][1]);
    }

    @Test
    public void testCountDistinctGroupBy() throws Exception {
        this.setup.groupBySetup("integer");
        execute("select count(distinct gender), count(*), race from characters group by race order by count(*) desc");

        assertEquals(3L, response.rowCount());
        assertEquals(2L, response.rows()[0][0]);
        assertEquals(4L, response.rows()[0][1]);
        assertEquals("Human", response.rows()[0][2]);

        assertEquals(1L, response.rows()[1][0]);
        assertEquals(2L, response.rows()[1][1]);
        assertEquals("Vogon", response.rows()[1][2]);

        assertEquals(1L, response.rows()[2][0]);
        assertEquals(1L, response.rows()[2][1]);
        assertEquals("Android", response.rows()[2][2]);
    }

    @Test
    public void testCountDistinctGroupByOrderByCountDistinct() throws Exception {
        this.setup.groupBySetup("integer");
        execute("select count(distinct gender), count(*), race from characters group by race order by count(distinct gender) desc");
        assertEquals(3L, response.rowCount());
        assertEquals(2L, response.rows()[0][0]);
    }

    @Test
    public void testCountDistinctManySame() throws Exception {
        this.setup.groupBySetup("integer");
        execute("select race, count(distinct gender), count(*), count(distinct gender) " +
                "from characters group by race order by count(distinct gender) desc, " +
                "count(*) desc");

        assertEquals(3L, response.rowCount());

        assertEquals("Human", response.rows()[0][0]);
        assertEquals(2L, response.rows()[0][1]);
        assertEquals(4L, response.rows()[0][2]);
        assertEquals(2L, response.rows()[0][3]);

        assertEquals("Vogon", response.rows()[1][0]);
        assertEquals(1L, response.rows()[1][1]);
        assertEquals(2L, response.rows()[1][2]);
        assertEquals(1L, response.rows()[1][3]);

        assertEquals("Android", response.rows()[2][0]);
        assertEquals(1L, response.rows()[2][1]);
        assertEquals(1L, response.rows()[2][2]);
        assertEquals(1L, response.rows()[2][3]);
    }

    @Test
    public void testCountDistinctManyDifferent() throws Exception {
        this.setup.groupBySetup("integer");
        execute("select race, count(distinct gender), count(distinct age) " +
                "from characters group by race order by count(distinct gender) desc, race desc");
        assertEquals(3L, response.rowCount());

        assertEquals("Human", response.rows()[0][0]);
        assertEquals(2L, response.rows()[0][1]);
        assertEquals(4L, response.rows()[0][2]);


        assertEquals("Vogon", response.rows()[1][0]);
        assertEquals(1L, response.rows()[1][1]);
        assertEquals(0L, response.rows()[1][2]);

        assertEquals("Android", response.rows()[2][0]);
        assertEquals(1L, response.rows()[2][1]);
        assertEquals(0L, response.rows()[2][2]);
    }

    @Test
    public void selectGroupByAggregateMinOrderByMin() throws Exception {
        this.setup.groupBySetup("double");

        execute("select min(age) as minAge, gender from characters group by gender order by minAge desc");
        assertEquals(2L, response.rowCount());

        assertEquals("male", response.rows()[0][1]);
        assertEquals(34.0d, response.rows()[0][0]);

        assertEquals("female", response.rows()[1][1]);
        assertEquals(32.0d, response.rows()[1][0]);

        execute("select min(age), gender from characters group by gender order by min(age) asc");
        assertEquals(2L, response.rowCount());

        assertEquals("female", response.rows()[0][1]);
        assertEquals(32.0d, response.rows()[0][0]);

        assertEquals("male", response.rows()[1][1]);
        assertEquals(34.0d, response.rows()[1][0]);
    }

    @Test
    public void selectGroupByAggregateMinLong() throws Exception {
        this.setup.groupBySetup("long");

        execute("select min(age) as minAge, gender from characters group by gender order by gender");
        assertEquals(2L, response.rowCount());
        assertEquals(32L, response.rows()[0][0]);
        assertEquals(34L, response.rows()[1][0]);
    }

    @Test
    public void selectGroupByAggregateMinString() throws Exception {
        this.setup.groupBySetup();

        execute("select min(name) as minName, gender from characters group by gender order by gender");
        assertEquals(2L, response.rowCount());
        assertEquals("Anjie", response.rows()[0][0]);
        assertEquals("Arthur Dent", response.rows()[1][0]);
    }

    @Test
    public void selectGroupByAggregateMinDate() throws Exception {
        this.setup.groupBySetup();

        execute("select min(birthdate) as minBirthdate, gender from characters group by gender " +
                "order by gender");
        assertEquals(2L, response.rowCount());
        assertEquals("female", response.rows()[0][1]);
        assertEquals(0L, response.rows()[0][0]);

        assertEquals("male", response.rows()[1][1]);
        assertEquals(181353600000L, response.rows()[1][0]);
    }

    // MAX

    @Test
    public void selectGroupByAggregateMaxString() throws Exception {
        this.setup.groupBySetup();

        execute("select max(name), gender from characters group by gender order by gender");
        assertEquals(2L, response.rowCount());
        assertEquals("Trillian", response.rows()[0][0]);
        assertEquals("Marving", response.rows()[1][0]);
    }

    @Test
    public void selectGroupByMixedCaseAggregateMaxString() throws Exception {
        this.setup.groupBySetup();

        execute("select max(NAME), GENDER from characters group by gender order by gender");
        assertEquals(2L, response.rowCount());
        assertEquals("Trillian", response.rows()[0][0]);
        assertEquals("Marving", response.rows()[1][0]);
    }

    @Test
    public void selectGroupByAggregateMaxLong() throws Exception {
        this.setup.groupBySetup("long");

        execute("select max(age), gender from characters group by gender order by gender");
        assertEquals(2L, response.rowCount());
        assertEquals(43L, response.rows()[0][0]);
        assertEquals(112L, response.rows()[1][0]);
    }

    @Test
    public void selectGroupByAggregateMaxDate() throws Exception {

        execute("select max(hired), department from employees group by department " +
                "order by department asc");
        assertEquals(4L, response.rowCount());

        assertEquals("HR", response.rows()[0][1]);
        assertEquals(631152000000L, response.rows()[0][0]);

        assertEquals("engineering", response.rows()[1][1]);
        assertEquals(946684800000L, response.rows()[1][0]);

        assertEquals("internship", response.rows()[2][1]);
        assertEquals(null, response.rows()[2][0]);

        assertEquals("management", response.rows()[3][1]);
        assertEquals(1286668800000L, response.rows()[3][0]);
    }

    @Test
    public void selectGroupByAggregateMaxInteger() throws Exception {
        this.setup.groupBySetup("integer");

        execute("select max(age), gender from characters group by gender order by gender");
        assertArrayEquals(new String[]{"max(age)", "gender"}, response.cols());
        assertEquals(2L, response.rowCount());
        assertEquals("female", response.rows()[0][1]);
        assertEquals(43, response.rows()[0][0]);

        assertEquals("male", response.rows()[1][1]);
        assertEquals(112, response.rows()[1][0]);
    }

    @Test
    public void selectGroupByAggregateMaxFloat() throws Exception {
        this.setup.groupBySetup("float");

        execute("select max(age), gender from characters group by gender order by gender");
        assertEquals(2L, response.rowCount());
        assertEquals("max(age)", response.cols()[0]);
        assertEquals(43.0f, response.rows()[0][0]);
        assertEquals(112.0f, response.rows()[1][0]);
    }

    @Test
    public void selectGroupByAggregateMaxDouble() throws Exception {

        execute("select max(income), department from employees group by department order by department");
        assertEquals(4L, response.rowCount());
        assertEquals(999999999.99d, response.rows()[0][0]);
        assertEquals("HR", response.rows()[0][1]);

        assertEquals(6000.0d, response.rows()[1][0]);
        assertEquals("engineering", response.rows()[1][1]);

        assertEquals(null, response.rows()[2][0]);
        assertEquals("internship", response.rows()[2][1]);

        assertEquals(Double.MAX_VALUE, response.rows()[3][0]);
        assertEquals("management", response.rows()[3][1]);
    }

    @Test
    public void selectGroupByAggregateMaxOrderByMax() throws Exception {
        this.setup.groupBySetup("double");

        execute("select max(age) as maxage, gender from characters group by gender order by maxage desc");
        assertEquals(2L, response.rowCount());

        assertEquals("male", response.rows()[0][1]);
        assertEquals(112.0d, response.rows()[0][0]);

        assertEquals("female", response.rows()[1][1]);
        assertEquals(43.0d, response.rows()[1][0]);

        execute("select max(age), gender from characters group by gender order by max(age) asc");
        assertEquals(2L, response.rowCount());

        assertEquals("female", response.rows()[0][1]);
        assertEquals(43.0d, response.rows()[0][0]);

        assertEquals("male", response.rows()[1][1]);
        assertEquals(112.0d, response.rows()[1][0]);
    }

    @Test
    public void testGroupByAggSumDouble() throws Exception {

        execute("select sum(income), department from employees group by department order by sum(income) asc");
        assertEquals(4, response.rowCount());

        assertEquals("engineering", response.rows()[0][1]);
        assertEquals(10000.0, response.rows()[0][0]);

        assertEquals("HR", response.rows()[1][1]);
        assertEquals(1000000000.49, response.rows()[1][0]);

        assertEquals("management", response.rows()[2][1]);
        assertEquals(Double.MAX_VALUE, response.rows()[2][0]);

        assertEquals("internship", response.rows()[3][1]);
        assertNull(response.rows()[3][0]);
    }

    @Test
    public void testGroupByAggSumShort() throws Exception {

        execute("select sum(age), department from employees group by department order by department asc");
        assertEquals(4, response.rowCount());

        assertEquals("HR", response.rows()[0][1]);
        assertEquals(12.0d, response.rows()[0][0]);

        assertEquals("engineering", response.rows()[1][1]);
        assertEquals(101.0d, response.rows()[1][0]);

        assertEquals("internship", response.rows()[2][1]);
        assertEquals(28.0d, response.rows()[2][0]);

        assertEquals("management", response.rows()[3][1]);
        assertEquals(45.0d, response.rows()[3][0]);

    }

    @Test
    public void testGroupByAvgDouble() throws Exception {
        execute("select avg(income), mean(income), department from employees group by department order by department asc");
        assertEquals(4, response.rowCount());

        assertEquals(500000000.245d, response.rows()[0][0]);
        assertEquals(500000000.245d, response.rows()[0][1]);
        assertEquals("HR", response.rows()[0][2]);

        assertEquals(5000.0d, response.rows()[1][0]);
        assertEquals(5000.0d, response.rows()[1][1]);
        assertEquals("engineering", response.rows()[1][2]);


        assertEquals(null, response.rows()[2][0]);
        assertEquals(null, response.rows()[2][1]);
        assertEquals("internship", response.rows()[2][2]);

        assertEquals(Double.MAX_VALUE, response.rows()[3][0]);
        assertEquals(Double.MAX_VALUE, response.rows()[3][1]);
        assertEquals("management", response.rows()[3][2]);
    }

    @Test
    public void testGroupByAvgMany() throws Exception {
        execute("select avg(income), avg(age), department from employees group by department order by department asc");
        assertEquals(4, response.rowCount());

        assertEquals("HR", response.rows()[0][2]);
        assertEquals(500000000.245d, response.rows()[0][0]);
        assertEquals(12.0d, response.rows()[0][1]);

        assertEquals("engineering", response.rows()[1][2]);
        assertEquals(5000.0d, response.rows()[1][0]);
        assertEquals(50.5d, response.rows()[1][1]);

        assertEquals("internship", response.rows()[2][2]);
        assertEquals(null, response.rows()[2][0]);
        assertEquals(28.0d, response.rows()[2][1]);

        assertEquals("management", response.rows()[3][2]);
        assertEquals(Double.MAX_VALUE, response.rows()[3][0]);
        assertEquals(45.0d, response.rows()[3][1]);
    }

    @Test
    public void testGroupByArbitrary() throws Exception {
        this.setup.groupBySetup();

        execute("select arbitrary(name), race from characters group by race order by race asc");
        SQLResponse arbitrary_response = response;
        assertEquals(3, arbitrary_response.rowCount());

        assertEquals("Android", arbitrary_response.rows()[0][1]);
        assertEquals(1,
                execute("select name from characters where race=? AND name=? ",
                        new Object[]{"Android", arbitrary_response.rows()[0][0]})
                        .rowCount()
        );
        assertEquals("Human", arbitrary_response.rows()[1][1]);
        assertEquals(1,
                execute("select name from characters where race=? AND name=? ",
                        new Object[]{"Human", arbitrary_response.rows()[1][0]})
                        .rowCount()
        );
        assertEquals("Vogon", arbitrary_response.rows()[2][1]);
        assertEquals(1,
                execute("select name from characters where race=? AND name=? ",
                        new Object[]{"Vogon", arbitrary_response.rows()[2][0]})
                        .rowCount()
        );

    }

    @Test
    public void testGlobalAggregateArbitrary() throws Exception {
        this.setup.groupBySetup();

        execute("select arbitrary(age) from characters where age is not null");
        assertEquals(1, response.rowCount());
        assertEquals(1,
                execute("select count(*) from characters where age=?",
                        new Object[]{response.rows()[0][0]})
                        .rowCount()
        );
    }

    @Test
    public void testAggregateArbitraryOnBoolean() throws Exception {
        execute("select arbitrary(good) from employees");
        assertEquals(1, response.rowCount());
        assertThat(response.rows()[0][0], isIn(new Object[]{true, false, null}));

        execute("select arbitrary(good) from employees where name='dilbert'");
        assertEquals(1, response.rowCount());
        assertEquals(true, response.rows()[0][0]);

        execute("select arbitrary(good), department from employees group by department order by department asc");
        assertEquals(4, response.rowCount());
        assertEquals("HR", response.rows()[0][1]);
        assertThat(response.rows()[0][0], isIn(new Object[]{false, null}));

        assertEquals("engineering", response.rows()[1][1]);
        assertEquals(true, response.rows()[1][0]);  // by accident only single values exist in group

        assertEquals("internship", response.rows()[2][1]);
        assertNull(response.rows()[2][0]);

        assertEquals("management", response.rows()[3][1]);
        assertEquals(false, response.rows()[3][0]);
    }

    public void testGroupByCountOnColumn() throws Exception {
        execute("select department, count(income), count(*) " +
                "from employees group by department order by department asc");
        assertEquals(4, response.rowCount());

        assertEquals("HR", response.rows()[0][0]);
        assertEquals(2L, response.rows()[0][1]);
        assertEquals(2L, response.rows()[0][2]);

        assertEquals("engineering", response.rows()[1][0]);
        assertEquals(2L, response.rows()[1][1]);
        assertEquals(2L, response.rows()[1][2]);

        assertEquals("internship", response.rows()[2][0]);
        assertEquals(0L, response.rows()[2][1]);
        assertEquals(1L, response.rows()[2][2]);

        assertEquals("management", response.rows()[3][0]);
        assertEquals(1L, response.rows()[3][1]);
        assertEquals(1L, response.rows()[3][2]);
    }

    @Test
    public void testGlobalCountOnColumn() throws Exception {

        execute("select count(*), count(good), count(distinct good) from employees");
        assertEquals(1, response.rowCount());
        assertEquals(6L, response.rows()[0][0]);
        assertEquals(4L, response.rows()[0][1]);
        assertEquals(2L, response.rows()[0][2]);
    }

    @Test
    public void testGlobalCountDistinct() throws Exception {

        execute("select count(distinct good) from employees");
        assertEquals(1, response.rowCount());
        assertEquals(2L, response.rows()[0][0]);
    }

    @Test
    public void testGlobalCountDistinctColumnReuse() throws Exception {

        execute("select count(distinct good), count(distinct department), count(distinct good) from employees");
        assertEquals(1, response.rowCount());
        assertEquals(2L, response.rows()[0][0]);
        assertEquals(4L, response.rows()[0][1]);
        assertEquals(2L, response.rows()[0][2]);
    }

    @Test
    public void testGlobalAggregateOnNestedColumn() throws Exception {
        this.setup.groupBySetup();
        waitNoPendingTasksOnAll();
        execute("select count(details['job']), min(details['job']), max(details['job']), count(distinct details['job']) from characters");
        assertEquals(1, response.rowCount());
        assertArrayEquals(new String[]{"count(details['job'])", "min(details['job'])", "max(details['job'])", "count(DISTINCT details['job'])"}, response.cols());
        assertEquals(2L, response.rows()[0][0]);
        assertEquals("Mathematician", response.rows()[0][1]);
        assertEquals("Sandwitch Maker", response.rows()[0][2]);
        assertEquals(2L, response.rows()[0][3]);
    }

    @Test
    public void testGroupByAggregateOnNestedColumn() throws Exception {
        this.setup.groupBySetup();
        waitNoPendingTasksOnAll();
        execute("select race, count(details['job']) from characters group by race order by race");
        assertEquals(3, response.rowCount());
        assertEquals("Android", response.rows()[0][0]);
        assertEquals(0L, response.rows()[0][1]);

        assertEquals("Human", response.rows()[1][0]);
        assertEquals(2L, response.rows()[1][1]);

        assertEquals("Vogon", response.rows()[2][0]);
        assertEquals(0L, response.rows()[2][1]);
    }

    @Test
    public void testGroupByAggregateOnNestedColumnOrderBy() throws Exception {
        this.setup.groupBySetup();
        waitNoPendingTasksOnAll();
        execute("select race, count(details['job']) from characters group by race order by count(details['job']) desc limit 1");
        assertEquals(1, response.rowCount());
        assertArrayEquals(new String[]{"race", "count(details['job'])"}, response.cols());
        assertEquals("Human", response.rows()[0][0]);
        assertEquals(2L, response.rows()[0][1]);
    }

    @Test
    public void testGroupByAggregateOnNestedColumnOrderByAlias() throws Exception {
        this.setup.groupBySetup();
        waitNoPendingTasksOnAll();
        execute("select race, count(details['job']) as job_count from characters group by race order by job_count desc limit 1");
        assertEquals(1, response.rowCount());
        assertArrayEquals(new String[]{"race", "job_count"}, response.cols());

        assertEquals("Human", response.rows()[0][0]);
        assertEquals(2L, response.rows()[0][1]);
    }

    @Test
    public void testGroupByUnknownResultColumn() throws Exception {
        this.setup.groupBySetup();
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("column 'details_ignored['lol']' must appear in the GROUP BY clause or be used in an aggregation function");
        execute("select details_ignored['lol'] from characters group by race");
    }

    @Test
    public void testGroupByUnknownGroupByColumn() throws Exception {
        this.setup.groupBySetup();
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Cannot GROUP BY 'details_ignored['lol']': invalid data type 'null'");
        execute("select max(birthdate) from characters group by details_ignored['lol']");
    }

    @Test
    public void testGroupByUnknownWhere() throws Exception {
        this.setup.groupBySetup();
        execute("select max(birthdate), race from characters where details_ignored['lol']='funky' group by race");
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testNonDistributedGroupByNoMatch() throws Exception {
        execute("create table characters (" +
                " details_ignored object(ignored)," +
                " race string" +
                ") clustered into 1 shards");
        ensureYellow();
        SQLRequest request = new SQLRequest("select race from characters where details_ignored['lol']='funky' group by race");
        SQLResponse response = sqlExecutor.execute(request).actionGet(1000);
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testGlobalAggregateUnknownWhere() throws Exception {
        this.setup.groupBySetup();
        execute("select max(birthdate) from characters where details_ignored['lol']='funky'");
        assertEquals(1, response.rowCount());
        assertNull(response.rows()[0][0]);
    }

    @Test
    public void testAggregateNonExistingColumn() throws Exception {
        this.setup.groupBySetup();
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("unknown function: max(null)"); // TODO: better exception
        execute("select max(details_ignored['lol']), race from characters group by race");
    }

    @Test
    public void testHavingGlobalAggregation() throws Exception {
        this.setup.groupBySetup("integer");
        execute("select min(birthdate), min(age) from characters having min(age) < 33 and max(age) > 100");
        assertEquals(1L, response.rowCount());
        assertEquals(2, response.rows()[0].length);
        assertEquals(0L, response.rows()[0][0]);
        assertEquals(32, response.rows()[0][1]);
    }

    @Test
    public void testHavingGroupBy() throws Exception {
        this.setup.groupBySetup("integer");
        execute("select age from characters group by age having age > 40 order by age");
        assertEquals(2L, response.rowCount());
        assertEquals(43, response.rows()[0][0]);
    }

    @Test
    public void testHavingOnSameAggregate() throws Exception {
        this.setup.groupBySetup("integer");
        execute("select avg(birthdate) from characters group by gender\n" +
                "having avg(birthdate) = 181353600000.0");
        assertThat(response.rowCount(), is(1L));
        assertThat(TestingHelpers.printedTable(response.rows()), is("1.813536E11\n"));
    }

    @Test
    public void testHavingOnOtherAggregate() throws Exception {
        this.setup.groupBySetup("integer");
        execute("select avg(birthdate) from characters group by gender\n" +
                "having min(birthdate) > '1970-01-01'");
        assertThat(response.rowCount(), is(1L));
        assertThat(TestingHelpers.printedTable(response.rows()), is("1.813536E11\n"));
    }


    @Test
    public void testHavingGroupByWithAggSelected() throws Exception {
        this.setup.groupBySetup("integer");
        execute("select age, count(*) from characters group by age having age > 40 order by age");
        assertEquals(2L, response.rowCount());
        assertEquals(43, response.rows()[0][0]);
    }

    @Test
    public void testHavingGroupByNonDistributed() throws Exception {
        execute("create table foo (id int, name string, country string) clustered by (country) with (number_of_replicas = 0)");
        ensureYellow();

        execute("insert into foo (id, name, country) values (?, ?, ?)", new Object[][]{
                new Object[] { 1, "Arthur", "Austria" },
                new Object[] { 2, "Trillian", "Austria" },
                new Object[] { 3, "Marvin", "Austria" },
                new Object[] { 4, "Jeltz", "German" },
                new Object[] { 5, "Ford", "German" },
                new Object[] { 6, "Slartibardfast", "Italy" },
        });
        refresh();


        execute("select country from foo group by country having country = 'Austria'");
        assertThat(response.rowCount(), is(1L));
        assertThat((String) response.rows()[0][0], is("Austria"));

        execute("select count(*), country from foo group by country having country = 'Austria'");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long)response.rows()[0][0], is(3L));
        assertThat((String) response.rows()[0][1], is("Austria"));

        execute("select country, min(id) from foo group by country having min(id) < 5 ");
        assertThat(response.rowCount(), is(2L));
    }

    @Test
    public void testGroupByHavingInsertInto() throws Exception {
        execute("create table foo (id int, name string, country string) with (number_of_replicas = 0)");
        execute("create table bar (country string) clustered by (country) with (number_of_replicas = 0)");
        ensureYellow();

        execute("insert into foo (id, name, country) values (?, ?, ?)", new Object[][]{
                new Object[]{1, "Arthur", "Austria"},
                new Object[]{2, "Trillian", "Austria"},
                new Object[]{3, "Marvin", "Austria"},
                new Object[]{4, "Jeltz", "German"},
                new Object[]{5, "Ford", "German"},
                new Object[]{6, "Slartibardfast", "Italy"},
        });
        refresh();

        execute("insert into bar(country)(select country from foo group by country having country = 'Austria')");
        refresh();
        execute("select country from bar");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testGroupByHavingWithAggregate() throws Exception {
        this.setup.groupBySetup("integer");
        execute("select gender from characters group by gender having min(age) < 33");
        assertEquals(1L, response.rowCount());
    }

    @Test
    public void testGroupByOnClusteredByColumn() throws Exception {
        execute("create table foo (id int, name string, country string) clustered by (country) with (number_of_replicas = 0)");
        ensureYellow();

        execute("insert into foo (id, name, country) values (?, ?, ?)", new Object[][]{
                new Object[]{1, "Arthur", "Austria"},
                new Object[]{2, "Trillian", "Austria"},
                new Object[]{3, "Marvin", "Austria"},
                new Object[]{4, "Jeltz", "Germany"},
                new Object[]{5, "Ford", "Germany"},
                new Object[]{6, "Slartibardfast", "Italy"},
        });
        refresh();

        execute("select count(*), country from foo group by country order by count(*) desc");
        assertThat(response.rowCount(), Is.is(3L));
        assertThat((String) response.rows()[0][1], Is.is("Austria"));
        assertThat((String) response.rows()[1][1], Is.is("Germany"));
        assertThat((String) response.rows()[2][1], Is.is("Italy"));
    }

    @Test
    public void testGroupByOnAllPrimaryKeys() throws Exception {
        execute("create table foo (id int primary key, name string primary key) with (number_of_replicas = 0)");
        ensureYellow();

        execute("insert into foo (id, name) values (?, ?)", new Object[][] {
                new Object[] { 1, "Arthur" },
                new Object[] { 2, "Trillian" },
                new Object[] { 3, "Slartibardfast" },
                new Object[] { 4, "Marvin" },
        });
        refresh();

        execute("select count(*), name from foo group by id, name order by name desc");
        assertThat(TestingHelpers.printedTable(response.rows()), is(
                "1| Trillian\n" +
                "1| Slartibardfast\n" +
                "1| Marvin\n" +
                "1| Arthur\n"));
    }

    @Test
    public void testGroupByEmpty() throws Exception {
        execute("create table test (col1 string)");
        ensureYellow();

        execute("select count(*), col1 from test group by col1");
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testGroupByMultiValueField() throws Exception {
        this.setup.groupBySetup();
        // inserting multiple values not supported anymore
        client().prepareIndex("characters", Constants.DEFAULT_MAPPING_TYPE).setSource(new HashMap<String, Object>() {{
            put("race", new String[]{"Android"});
            put("gender", new String[]{"male", "robot"});
            put("name", "Marvin2");
        }}).execute().actionGet();
        client().prepareIndex("characters", Constants.DEFAULT_MAPPING_TYPE).setSource(new HashMap<String, Object>() {{
            put("race", new String[]{"Android"});
            put("gender", new String[]{"male", "robot"});
            put("name", "Marvin3");
        }}).execute().actionGet();
        execute("refresh table characters");

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Column \"gender\" has a value that is an array. Group by doesn't work on Arrays");

        execute("select gender from characters group by gender");
    }

    @Test
    public void testGroupByOnSysNodes() throws Exception {
        execute("select count(*), name from sys.nodes group by name");
        assertThat(response.rowCount(), Is.is(2L));

        execute("select count(*), hostname from sys.nodes group by hostname");
        assertThat(response.rowCount(), Is.is(1L));
    }

    @Test
    public void testGroupByCountStringGroupByPrimaryKey() throws Exception {
        execute("create table rankings (" +
                " \"pageURL\" string primary key," +
                " \"pageRank\" int," +
                " \"avgDuration\" int" +
                ") with (number_of_replicas=0)");
        ensureYellow();
        for (int i = 0; i<100; i++) {
            execute("insert into rankings (\"pageURL\", \"pageRank\", \"avgDuration\") values (?, ?, ?)",
                    new Object[]{String.valueOf(i), randomIntBetween(i, i*i),  randomInt(i) });
            assertThat(response.rowCount(), is(1L));
        }
        execute("refresh table rankings");

        execute("select count(*), \"pageURL\" from rankings group by \"pageURL\" order by 1 desc limit 100");
        assertThat(response.rowCount(), is(100L));
        assertThat(new ArrayBucket(response.rows()), TestingHelpers.isSorted(0, true, null));
    }

    @Test
    public void testGroupByCountString() throws Exception {
        execute("create table rankings (" +
                " \"pageURL\" string," +
                " \"pageRank\" int," +
                " \"avgDuration\" int" +
                ") with (number_of_replicas=0)");
        ensureYellow();
        for (int i = 0; i<100; i++) {
            execute("insert into rankings (\"pageURL\", \"pageRank\", \"avgDuration\") values (?, ?, ?)",
                    new Object[]{randomAsciiOfLength(10 + (i%3)), randomIntBetween(i, i*i),  randomInt(i) });
        }
        execute("refresh table rankings");
        execute("select count(*), \"pageURL\" from rankings group by \"pageURL\" order by 1 desc limit 100");
        assertThat(response.rowCount(), is(100L));
    }

    @Test
    public void testAggregateTwiceOnRoutingColumn() throws Exception {
        execute("create table twice (" +
                "name string, " +
                "url string, " +
                "score double" +
                ") clustered by (url) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into twice (\"name\", \"url\", \"score\") values (?, ?, ?)",
                new Object[]{
                        "A",
                        "https://Ä.com",
                        99.6d});
        refresh();
        execute("select avg(score), url, avg(score) from twice group by url limit 10");
        assertThat(TestingHelpers.printedTable(response.rows()), is("99.6| https://Ä.com| 99.6\n"));
    }

    @Test
    public void testGroupByWithHavingAndLimit() throws Exception {
        execute("create table likes (" +
                "   event_id string," +
                "   item_id string" +
                ") clustered into 2 shards with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into likes (event_id, item_id) values (?, ?)", new Object[][] {
                new Object[] { "event1", "item1" },
                new Object[] { "event1", "item1" },
                new Object[] { "event1", "item2" },
                new Object[] { "event2", "item1" },
                new Object[] { "event2", "item2" },
        });
        execute("refresh table likes");

        try {
            execute("select count(*), item_id from likes where event_id = 'event1' group by 2 having count(*) > 1");
            assertThat(response.rowCount(), is(1L));
        } catch (SQLActionException e) {
            fail(e.getMessage());
        }

        try {
            execute("select count(*), item_id from likes where event_id = 'event1' group by 2 having count(*) > 1 limit 100");
            assertThat(response.rowCount(), is(1L));
        } catch (SQLActionException e) {
            fail(e.getMessage());
        }

        try {
            execute("select item_id, count(*) from likes where event_id = 'event1' group by 1 having count(*) > 1");
            assertThat(response.rowCount(), is(1L));
        } catch (SQLActionException e) {
            fail(e.getMessage());
        }

        try {
            execute("select item_id, count(*) from likes where event_id = 'event1' group by 1 having count(*) > 1 limit 100");
            assertThat(response.rowCount(), is(1L));
        } catch (SQLActionException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testNonDistributedGroupByWithHavingAndLimit() throws Exception {
        execute("create table likes (" +
                "   event_id string," +
                "   item_id string" +
                ") clustered into 1 shards with (number_of_replicas = 0)");
        // only 1 shard to force non-distribution
        ensureYellow();
        execute("insert into likes (event_id, item_id) values (?, ?)", new Object[][]{
                new Object[]{"event1", "item1"},
                new Object[]{"event1", "item1"},
                new Object[]{"event1", "item2"},
                new Object[]{"event2", "item1"},
                new Object[]{"event2", "item2"},
        });
        execute("refresh table likes");

        try {
            execute("select count(*), item_id from likes where event_id = 'event1' group by 2 having count(*) > 1");
            assertThat(response.rowCount(), is(1L));
        } catch (SQLActionException e) {
            fail(e.getMessage());
        }

        try {
            execute("select count(*), item_id from likes where event_id = 'event1' group by 2 having count(*) > 1 limit 100");
            assertThat(response.rowCount(), is(1L));
        } catch (SQLActionException e) {
            fail(e.getMessage());
        }

        try {
            execute("select item_id, count(*) from likes where event_id = 'event1' group by 1 having count(*) > 1");
            assertThat(response.rowCount(), is(1L));
        } catch (SQLActionException e) {
            fail(e.getMessage());
        }

        try {
            execute("select item_id, count(*) from likes where event_id = 'event1' group by 1 having count(*) > 1 limit 100");
            assertThat(response.rowCount(), is(1L));
        } catch (SQLActionException e) {
            fail(e.getMessage());
        }

        try {
            execute("select count(*), item_id from likes group by item_id having min(event_id) = 'event1'");
            assertThat(response.rowCount(), is(2L));
        } catch (SQLActionException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void groupByAggregateStdDevByte() throws Exception {
        this.setup.groupBySetup("byte");

        execute("select stddev(age), gender from characters group by gender order by gender");
        assertEquals(2L, response.rowCount());
        assertEquals(5.5d, response.rows()[0][0]);
        assertEquals(39.0d, response.rows()[1][0]);
    }

    @Test
    public void groupByAggregateVarianceByte() throws Exception {
        this.setup.groupBySetup("byte");

        execute("select variance(age), gender from characters group by gender order by gender");
        assertEquals(2L, response.rowCount());
        assertEquals(30.25d, response.rows()[0][0]);
        assertEquals(1521.0d, response.rows()[1][0]);
    }

    @Test
    public void groupByAggregateStdDevDouble() throws Exception {
        this.setup.groupBySetup("double");

        execute("select stddev(age), gender from characters group by gender order by gender");
        assertEquals(2L, response.rowCount());
        assertEquals(5.5d, response.rows()[0][0]);
        assertEquals(39.0d, response.rows()[1][0]);
    }

    @Test
    public void groupByStatsAggregatesGlobal() throws Exception {
        this.setup.groupBySetup("short");
        execute("select min(age), mean(age), geometric_mean(age), max(age), variance(age), stddev(age) from characters");
        assertThat((Short) response.rows()[0][0], is((short) 32));
        assertThat((Double) response.rows()[0][1], is(55.25d));

        assertThat((Double) response.rows()[0][2], closeTo(47.84415001097868d, 0.0000001));
        assertThat((Short) response.rows()[0][3], is((short) 112));
        assertThat((Double) response.rows()[0][4], is(1090.6875d));
        assertThat((Double) response.rows()[0][5], is(33.025558284456d));
    }

    @Test
    public void testGroupByOnClusteredByColumnPartitioned() throws Exception {
        execute("CREATE TABLE tickets ( " +
                "  ticket_id long, " +
                "  tenant_id integer, " +
                "  created_month string )" +
                "PARTITIONED BY (created_month) " +
                "CLUSTERED BY (tenant_id) " +
                "WITH (number_of_replicas=0)");
        ensureYellow();
        execute("insert into tickets (ticket_id, tenant_id, created_month) values (?, ?, ?)",
                new Object[][]{
                        {1L, 42, 1425168000000L},
                        {2L, 43, 0},
                        {3L, 44, 1425168000000L},
                        {4L, 45, 499651200000L},
                        {5L, 42, 0L},
                });
        ensureYellow();
        refresh();

        execute("select count(*), tenant_id from tickets group by 2 order by tenant_id limit 100");
        assertThat(TestingHelpers.printedTable(response.rows()), is(
                "2| 42\n" + // assert that different partitions have been merged
                "1| 43\n" +
                "1| 44\n" +
                "1| 45\n"));
    }

    @Test
    public void testFilterByScore() throws Exception {
        execute("create table locations (" +
                " altitude int," +
                " name string," +
                " description string index using fulltext" +
                ")");
        ensureYellow();

        execute("insert into locations (altitude, name, description) values (420, 'Crate Dornbirn', 'A nice place in a nice country')");
        execute("insert into locations (altitude, name, description) values (230, 'Crate Berlin', 'Also very nice place mostly nice in summer')");
        execute("insert into locations (altitude, name, description) values (70, 'Crate SF', 'A nice place with lot of sunshine')");
        execute("refresh table locations");

        execute("select min(altitude) as altitude, name from locations where match(description, 'nice') " +
                "and _score >= 0.99 group by name order by name");
        assertEquals(2L, response.rowCount());
    }

}
