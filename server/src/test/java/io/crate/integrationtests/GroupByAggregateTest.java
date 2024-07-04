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

package io.crate.integrationtests;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomAsciiLettersOfLength;
import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.TestingHelpers.printedTable;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.assertj.core.api.Assertions.fail;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.assertj.core.data.Percentage;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Before;
import org.junit.Test;

import io.crate.data.Paging;
import io.crate.testing.Asserts;
import io.crate.testing.SQLResponse;
import io.crate.testing.UseJdbc;
import io.crate.testing.UseRandomizedOptimizerRules;

@IntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
public class GroupByAggregateTest extends IntegTestCase {

    private Setup setup = new Setup(sqlExecutor);

    @Before
    public void initTestData() {
        setup.setUpEmployees();
    }

    @Test
    public void selectGroupByAggregateMinInteger() throws Exception {
        this.setup.groupBySetup("integer");

        execute("select min(age) as minage, gender from characters group by gender order by gender");
        assertThat(response.cols()).isEqualTo(new String[]{"minage", "gender"});
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response.rows()[0][1]).isEqualTo("female");
        assertThat(response.rows()[0][0]).isEqualTo(32);

        assertThat(response.rows()[1][1]).isEqualTo("male");
        assertThat(response.rows()[1][0]).isEqualTo(34);
    }

    @Test
    public void testSelectDistinctWithPaging() throws Exception {
        Paging.PAGE_SIZE = 2;
        execute("create table t (name string) with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into t (name) values ('Marvin'), ('Trillian'), ('Ford'), ('Arthur')");
        execute("refresh table t");
        execute("select distinct name from t");
    }

    @Test
    public void testNonDistributedGroupByWithManyKeysNoOrderByAndLimit() throws Exception {
        execute("create table t (name string, x int) clustered by (name) with (number_of_replicas = 0)");
        ensureYellow();

        execute("insert into t (name, x) values ('Marvin', 1), ('Trillian', 1), ('Ford', 1), ('Arthur', 1)");
        execute("refresh table t");

        execute("select count(*), name from t group by name, x limit 2");
        assertThat(response).hasRowCount(2L);
    }

    @Test
    public void testGroupByOnClusteredByColumnPartOfPrimaryKey() {
        execute("CREATE TABLE tickets ( " +
                " pk1 long, " +
                " pk2 integer, " +
                " pk3 timestamp with time zone," +
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
        assertThat(response).hasRows(
            "42| 2", // assert that different partitions have been merged
            "43| 1",
            "44| 1",
            "45| 1");

        execute("create table tickets_export (c2 int, c long) with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into tickets_export (c2, c) (select pk2, count(pk2) from tickets group by pk2)");
        execute("refresh table tickets_export");

        execute("select c2, c from tickets_export order by 1");
        assertThat(response).hasRows(
            "42| 2", // assert that different partitions have been merged
            "43| 1",
            "44| 1",
            "45| 1");
    }

    @Test
    public void selectGroupByAggregateMinFloat() throws Exception {
        this.setup.groupBySetup("float");

        execute("select min(age), gender from characters group by gender order by gender");

        String expected = "32.0| female\n34.0| male\n";

        assertThat(response.cols()[0]).isEqualTo("min(age)");
        assertThat(printedTable(response.rows())).isEqualTo(expected);
    }

    @Test
    public void selectGroupByAggregateMinDouble() throws Exception {
        this.setup.groupBySetup("double");

        execute("select min(age) as minAge, gender from characters group by gender order by gender");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response.rows()[0][0]).isEqualTo(32.0d);
        assertThat(response.rows()[1][0]).isEqualTo(34.0d);
    }

    @Test
    public void testCountDistinctGlobal() throws Exception {
        execute("select count(distinct department), count(*) from employees");

        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.rows()[0][0]).isEqualTo(4L);
        assertThat(response.rows()[0][1]).isEqualTo(6L);
    }

    @Test
    public void testCountDistinctGroupBy() throws Exception {
        this.setup.groupBySetup("integer");
        execute("select count(distinct gender), count(*), race from characters group by race order by count(*) desc");

        assertThat(response.rowCount()).isEqualTo(3L);
        assertThat(response.rows()[0][0]).isEqualTo(2L);
        assertThat(response.rows()[0][1]).isEqualTo(4L);
        assertThat(response.rows()[0][2]).isEqualTo("Human");

        assertThat(response.rows()[1][0]).isEqualTo(1L);
        assertThat(response.rows()[1][1]).isEqualTo(2L);
        assertThat(response.rows()[1][2]).isEqualTo("Vogon");

        assertThat(response.rows()[2][0]).isEqualTo(1L);
        assertThat(response.rows()[2][1]).isEqualTo(1L);
        assertThat(response.rows()[2][2]).isEqualTo("Android");
    }

    @Test
    public void testCountDistinctGroupByOrderByCountDistinct() throws Exception {
        this.setup.groupBySetup("integer");
        execute("select count(distinct gender), count(*), race from characters group by race order by count(distinct gender) desc");
        assertThat(response.rowCount()).isEqualTo(3L);
        assertThat(response.rows()[0][0]).isEqualTo(2L);
    }

    @Test
    public void testCountDistinctManySame() throws Exception {
        this.setup.groupBySetup("integer");
        execute("select race, count(distinct gender), count(*), count(distinct gender) " +
                "from characters group by race order by count(distinct gender) desc, " +
                "count(*) desc");

        assertThat(response.rowCount()).isEqualTo(3L);

        assertThat(response.rows()[0][0]).isEqualTo("Human");
        assertThat(response.rows()[0][1]).isEqualTo(2L);
        assertThat(response.rows()[0][2]).isEqualTo(4L);
        assertThat(response.rows()[0][3]).isEqualTo(2L);

        assertThat(response.rows()[1][0]).isEqualTo("Vogon");
        assertThat(response.rows()[1][1]).isEqualTo(1L);
        assertThat(response.rows()[1][2]).isEqualTo(2L);
        assertThat(response.rows()[1][3]).isEqualTo(1L);

        assertThat(response.rows()[2][0]).isEqualTo("Android");
        assertThat(response.rows()[2][1]).isEqualTo(1L);
        assertThat(response.rows()[2][2]).isEqualTo(1L);
        assertThat(response.rows()[2][3]).isEqualTo(1L);
    }

    @Test
    public void testCountDistinctManyDifferent() throws Exception {
        this.setup.groupBySetup("integer");
        execute("select race, count(distinct gender), count(distinct age) " +
                "from characters group by race order by count(distinct gender) desc, race desc");
        assertThat(response.rowCount()).isEqualTo(3L);

        assertThat(response.rows()[0][0]).isEqualTo("Human");
        assertThat(response.rows()[0][1]).isEqualTo(2L);
        assertThat(response.rows()[0][2]).isEqualTo(4L);


        assertThat(response.rows()[1][0]).isEqualTo("Vogon");
        assertThat(response.rows()[1][1]).isEqualTo(1L);
        assertThat(response.rows()[1][2]).isEqualTo(0L);

        assertThat(response.rows()[2][0]).isEqualTo("Android");
        assertThat(response.rows()[2][1]).isEqualTo(1L);
        assertThat(response.rows()[2][2]).isEqualTo(0L);
    }

    @Test
    public void selectGroupByAggregateMinOrderByMin() throws Exception {
        this.setup.groupBySetup("double");

        execute("select min(age) as minAge, gender from characters group by gender order by minAge desc");
        assertThat(response.rowCount()).isEqualTo(2L);

        assertThat(response.rows()[0][1]).isEqualTo("male");
        assertThat(response.rows()[0][0]).isEqualTo(34.0d);

        assertThat(response.rows()[1][1]).isEqualTo("female");
        assertThat(response.rows()[1][0]).isEqualTo(32.0d);

        execute("select min(age), gender from characters group by gender order by min(age) asc");
        assertThat(response.rowCount()).isEqualTo(2L);

        assertThat(response.rows()[0][1]).isEqualTo("female");
        assertThat(response.rows()[0][0]).isEqualTo(32.0d);

        assertThat(response.rows()[1][1]).isEqualTo("male");
        assertThat(response.rows()[1][0]).isEqualTo(34.0d);
    }

    @Test
    public void selectGroupByAggregateMinLong() throws Exception {
        this.setup.groupBySetup("long");

        execute("select min(age) as minAge, gender from characters group by gender order by gender");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response.rows()[0][0]).isEqualTo(32L);
        assertThat(response.rows()[1][0]).isEqualTo(34L);
    }

    @Test
    public void selectGroupByAggregateMinString() throws Exception {
        this.setup.groupBySetup();

        execute("select min(name) as minName, gender from characters group by gender order by gender");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response.rows()[0][0]).isEqualTo("Anjie");
        assertThat(response.rows()[1][0]).isEqualTo("Arthur Dent");
    }

    @Test
    public void selectGroupByAggregateMinDate() throws Exception {
        this.setup.groupBySetup();

        execute("select min(birthdate) as minBirthdate, gender from characters group by gender " +
                "order by gender");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response.rows()[0][1]).isEqualTo("female");
        assertThat(response.rows()[0][0]).isEqualTo(0L);

        assertThat(response.rows()[1][1]).isEqualTo("male");
        assertThat(response.rows()[1][0]).isEqualTo(181353600000L);
    }

    // MAX

    @Test
    public void selectGroupByAggregateMaxString() throws Exception {
        this.setup.groupBySetup();

        execute("select max(name), gender from characters group by gender order by gender");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response.rows()[0][0]).isEqualTo("Trillian");
        assertThat(response.rows()[1][0]).isEqualTo("Marving");
    }

    @Test
    public void selectGroupByMixedCaseAggregateMaxString() throws Exception {
        this.setup.groupBySetup();

        execute("select max(NAME), GENDER from characters group by gender order by gender");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response.rows()[0][0]).isEqualTo("Trillian");
        assertThat(response.rows()[1][0]).isEqualTo("Marving");
    }

    @Test
    public void selectGroupByAggregateMaxLong() throws Exception {
        this.setup.groupBySetup("long");

        execute("select max(age), gender from characters group by gender order by gender");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response.rows()[0][0]).isEqualTo(43L);
        assertThat(response.rows()[1][0]).isEqualTo(112L);
    }

    @Test
    public void selectGroupByAggregateMaxDate() throws Exception {

        execute("select max(hired), department from employees group by department " +
                "order by department asc");
        assertThat(response.rowCount()).isEqualTo(4L);

        assertThat(response.rows()[0][1]).isEqualTo("HR");
        assertThat(response.rows()[0][0]).isEqualTo(631152000000L);

        assertThat(response.rows()[1][1]).isEqualTo("engineering");
        assertThat(response.rows()[1][0]).isEqualTo(946684800000L);

        assertThat(response.rows()[2][1]).isEqualTo("internship");
        assertThat(response.rows()[2][0]).isEqualTo(null);

        assertThat(response.rows()[3][1]).isEqualTo("management");
        assertThat(response.rows()[3][0]).isEqualTo(1286668800000L);
    }

    @Test
    public void selectGroupByAggregateMaxInteger() throws Exception {
        this.setup.groupBySetup("integer");

        execute("select max(age), gender from characters group by gender order by gender");
        assertThat(response.cols()).isEqualTo(new String[]{"max(age)", "gender"});
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response.rows()[0][1]).isEqualTo("female");
        assertThat(response.rows()[0][0]).isEqualTo(43);

        assertThat(response.rows()[1][1]).isEqualTo("male");
        assertThat(response.rows()[1][0]).isEqualTo(112);
    }

    @Test
    public void selectGroupByAggregateMaxFloat() throws Exception {
        this.setup.groupBySetup("float");

        execute("select max(age), gender from characters group by gender order by gender");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response.cols()[0]).isEqualTo("max(age)");
        assertThat(response.rows()[0][0]).isEqualTo(43.0f);
        assertThat(response.rows()[1][0]).isEqualTo(112.0f);
    }

    @Test
    public void selectGroupByAggregateMaxDouble() throws Exception {

        execute("select max(income), department from employees group by department order by department");
        assertThat(response.rowCount()).isEqualTo(4L);
        assertThat(response.rows()[0][0]).isEqualTo(999999999.99d);
        assertThat(response.rows()[0][1]).isEqualTo("HR");

        assertThat(response.rows()[1][0]).isEqualTo(6000.0d);
        assertThat(response.rows()[1][1]).isEqualTo("engineering");

        assertThat(response.rows()[2][0]).isEqualTo(null);
        assertThat(response.rows()[2][1]).isEqualTo("internship");

        assertThat(response.rows()[3][0]).isEqualTo(Double.MAX_VALUE);
        assertThat(response.rows()[3][1]).isEqualTo("management");
    }

    @Test
    public void selectGroupByAggregateMaxOrderByMax() throws Exception {
        this.setup.groupBySetup("double");

        execute("select max(age) as maxage, gender from characters group by gender order by maxage desc");
        assertThat(response.rowCount()).isEqualTo(2L);

        assertThat(response.rows()[0][1]).isEqualTo("male");
        assertThat(response.rows()[0][0]).isEqualTo(112.0d);

        assertThat(response.rows()[1][1]).isEqualTo("female");
        assertThat(response.rows()[1][0]).isEqualTo(43.0d);

        execute("select max(age), gender from characters group by gender order by max(age) asc");
        assertThat(response.rowCount()).isEqualTo(2L);

        assertThat(response.rows()[0][1]).isEqualTo("female");
        assertThat(response.rows()[0][0]).isEqualTo(43.0d);

        assertThat(response.rows()[1][1]).isEqualTo("male");
        assertThat(response.rows()[1][0]).isEqualTo(112.0d);
    }

    @Test
    public void testGroupByAggSumDouble() throws Exception {

        execute("select sum(income), department from employees group by department order by sum(income) asc");
        assertThat(response.rowCount()).isEqualTo(4);

        assertThat(response.rows()[0][1]).isEqualTo("engineering");
        assertThat(response.rows()[0][0]).isEqualTo(10000.0);

        assertThat(response.rows()[1][1]).isEqualTo("HR");
        assertThat(response.rows()[1][0]).isEqualTo(1000000000.49);

        assertThat(response.rows()[2][1]).isEqualTo("management");
        assertThat(response.rows()[2][0]).isEqualTo(Double.MAX_VALUE);

        assertThat(response.rows()[3][1]).isEqualTo("internship");
        assertThat(response.rows()[3][0]).isNull();
    }

    @Test
    public void testGroupByAggSumShort() throws Exception {

        execute("select sum(age), department from employees group by department order by department asc");
        assertThat(response.rowCount()).isEqualTo(4);

        assertThat(response.rows()[0][1]).isEqualTo("HR");
        assertThat(response.rows()[0][0]).isEqualTo(12L);

        assertThat(response.rows()[1][1]).isEqualTo("engineering");
        assertThat(response.rows()[1][0]).isEqualTo(101L);

        assertThat(response.rows()[2][1]).isEqualTo("internship");
        assertThat(response.rows()[2][0]).isEqualTo(28L);

        assertThat(response.rows()[3][1]).isEqualTo("management");
        assertThat(response.rows()[3][0]).isEqualTo(45L);

    }

    @Test
    public void testGroupByAvgDouble() throws Exception {
        execute("select avg(income), mean(income), department from employees group by department order by department asc");
        assertThat(response.rowCount()).isEqualTo(4);

        assertThat(response.rows()[0][0]).isEqualTo(500000000.245d);
        assertThat(response.rows()[0][1]).isEqualTo(500000000.245d);
        assertThat(response.rows()[0][2]).isEqualTo("HR");

        assertThat(response.rows()[1][0]).isEqualTo(5000.0d);
        assertThat(response.rows()[1][1]).isEqualTo(5000.0d);
        assertThat(response.rows()[1][2]).isEqualTo("engineering");


        assertThat(response.rows()[2][0]).isEqualTo(null);
        assertThat(response.rows()[2][1]).isEqualTo(null);
        assertThat(response.rows()[2][2]).isEqualTo("internship");

        assertThat(response.rows()[3][0]).isEqualTo(Double.MAX_VALUE);
        assertThat(response.rows()[3][1]).isEqualTo(Double.MAX_VALUE);
        assertThat(response.rows()[3][2]).isEqualTo("management");
    }

    @Test
    public void testGroupByAvgMany() throws Exception {
        execute("select avg(income), avg(age), department from employees group by department order by department asc");
        assertThat(response.rowCount()).isEqualTo(4);

        assertThat(response.rows()[0][2]).isEqualTo("HR");
        assertThat(response.rows()[0][0]).isEqualTo(500000000.245d);
        assertThat(response.rows()[0][1]).isEqualTo(12.0d);

        assertThat(response.rows()[1][2]).isEqualTo("engineering");
        assertThat(response.rows()[1][0]).isEqualTo(5000.0d);
        assertThat(response.rows()[1][1]).isEqualTo(50.5d);

        assertThat(response.rows()[2][2]).isEqualTo("internship");
        assertThat(response.rows()[2][0]).isEqualTo(null);
        assertThat(response.rows()[2][1]).isEqualTo(28.0d);

        assertThat(response.rows()[3][2]).isEqualTo("management");
        assertThat(response.rows()[3][0]).isEqualTo(Double.MAX_VALUE);
        assertThat(response.rows()[3][1]).isEqualTo(45.0d);
    }

    @Test
    public void testGroupByArbitrary() throws Exception {
        this.setup.groupBySetup();

        execute("select arbitrary(name), race from characters group by race order by race asc");
        SQLResponse arbitrary_response = response;
        assertThat(arbitrary_response.rowCount()).isEqualTo(3);

        assertThat(arbitrary_response.rows()[0][1]).isEqualTo("Android");
        assertThat(execute("select name from characters where race=? AND name=? ",
                new Object[]{"Android", arbitrary_response.rows()[0][0]})
                .rowCount()
        ).isEqualTo(1);
        assertThat(arbitrary_response.rows()[1][1]).isEqualTo("Human");
        assertThat(execute("select name from characters where race=? AND name=? ",
                new Object[]{"Human", arbitrary_response.rows()[1][0]})
                .rowCount()
        ).isEqualTo(1);
        assertThat(arbitrary_response.rows()[2][1]).isEqualTo("Vogon");
        assertThat(execute("select name from characters where race=? AND name=? ",
                new Object[]{"Vogon", arbitrary_response.rows()[2][0]})
                .rowCount()
        ).isEqualTo(1);

    }

    @Test
    public void testGlobalAggregateArbitrary() throws Exception {
        this.setup.groupBySetup();

        execute("select arbitrary(age) from characters where age is not null");
        assertThat(response.rowCount()).isEqualTo(1);
        assertThat(execute("select count(*) from characters where age=?",
                new Object[]{response.rows()[0][0]})
                .rowCount()
        ).isEqualTo(1);
    }

    @Test
    public void testAggregateArbitraryOnBoolean() throws Exception {
        execute("select arbitrary(good) from employees");
        assertThat(response.rowCount()).isEqualTo(1);
        assertThat(response.rows()[0][0]).isIn(true, false, null);

        execute("select arbitrary(good) from employees where name='dilbert'");
        assertThat(response.rowCount()).isEqualTo(1);
        assertThat(response.rows()[0][0]).isEqualTo(true);

        execute("select arbitrary(good), department from employees group by department order by department asc");
        assertThat(response.rowCount()).isEqualTo(4);
        assertThat(response.rows()[0][1]).isEqualTo("HR");
        assertThat(response.rows()[0][0]).isIn(false, null);

        assertThat(response.rows()[1][1]).isEqualTo("engineering");
        assertThat(response.rows()[1][0]).isEqualTo(true);  // by accident only single values exist in group

        assertThat(response.rows()[2][1]).isEqualTo("internship");
        assertThat(response.rows()[2][0]).isNull();

        assertThat(response.rows()[3][1]).isEqualTo("management");
        assertThat(response.rows()[3][0]).isEqualTo(false);
    }

    public void testGroupByCountOnColumn() throws Exception {
        execute("select department, count(income), count(*) " +
                "from employees group by department order by department asc");
        assertThat(response.rowCount()).isEqualTo(4);

        assertThat(response.rows()[0][0]).isEqualTo("HR");
        assertThat(response.rows()[0][1]).isEqualTo(2L);
        assertThat(response.rows()[0][2]).isEqualTo(2L);

        assertThat(response.rows()[1][0]).isEqualTo("engineering");
        assertThat(response.rows()[1][1]).isEqualTo(2L);
        assertThat(response.rows()[1][2]).isEqualTo(2L);

        assertThat(response.rows()[2][0]).isEqualTo("internship");
        assertThat(response.rows()[2][1]).isEqualTo(0L);
        assertThat(response.rows()[2][2]).isEqualTo(1L);

        assertThat(response.rows()[3][0]).isEqualTo("management");
        assertThat(response.rows()[3][1]).isEqualTo(1L);
        assertThat(response.rows()[3][2]).isEqualTo(1L);
    }

    @Test
    public void testCompareCountOnObjectHavingNotNullSubcolumnAndCountDirectlyOnNotNullSubcolumn() {
        execute("""
                   create table tbl (
                        device int not null,
                        payload object as (
                            col int,
                            nested_payload object as (
                                col int not null)
                        ),
                        nullable_payload object as (
                            col int
                        )
                   )
                   """);
        execute("insert into tbl values(1, {col=11, nested_payload={col=111}}, {col=null})");
        execute("insert into tbl values(1, {col=null, nested_payload={col=111}}, {col=11})");
        execute("insert into tbl values(1, {col=11, nested_payload={col=111}}, null)");
        execute("refresh table tbl");

        /* count(payload) is implicitly optimized to use DocValueAggregator of payload['nested_payload']['col'],
           so below 3 executions are expected to have the same results. */
        execute("select device, count(payload) from tbl group by 1");
        assertThat(response).hasRows("1| 3");
        execute("select device, count(payload['nested_payload']) from tbl group by 1");
        assertThat(response).hasRows("1| 3");
        execute("select device, count(payload['nested_payload']['col']) from tbl group by 1");
        assertThat(response).hasRows("1| 3");
        // below are just there to show that no other available columns are used instead of payload['nested_payload']['col']
        execute("select device, count(nullable_payload) from tbl group by 1");
        assertThat(response).hasRows("1| 2");
        execute("select device, count(payload['col']) from tbl group by 1");
        assertThat(response).hasRows("1| 2");
    }

    @Test
    public void testGlobalCountOnColumn() throws Exception {

        execute("select count(*), count(good), count(distinct good) from employees");
        assertThat(response.rowCount()).isEqualTo(1);
        assertThat(response.rows()[0][0]).isEqualTo(6L);
        assertThat(response.rows()[0][1]).isEqualTo(4L);
        assertThat(response.rows()[0][2]).isEqualTo(2L);
    }

    @Test
    public void testGlobalCountDistinct() throws Exception {

        execute("select count(distinct good) from employees");
        assertThat(response.rowCount()).isEqualTo(1);
        assertThat(response.rows()[0][0]).isEqualTo(2L);
    }

    @Test
    public void testGlobalCountDistinctColumnReuse() throws Exception {

        execute("select count(distinct good), count(distinct department), count(distinct good) from employees");
        assertThat(response.rowCount()).isEqualTo(1);
        assertThat(response.rows()[0][0]).isEqualTo(2L);
        assertThat(response.rows()[0][1]).isEqualTo(4L);
        assertThat(response.rows()[0][2]).isEqualTo(2L);
    }

    @Test
    public void testGlobalAggregateOnNestedColumn() throws Exception {
        this.setup.groupBySetup();
        waitNoPendingTasksOnAll();
        execute("select count(details['job']), min(details['job']), max(details['job']), count(distinct details['job']) from characters");
        assertThat(response.rowCount()).isEqualTo(1);
        assertThat(response.rows()[0][0]).isEqualTo(2L);
        assertThat(response.rows()[0][1]).isEqualTo("Mathematician");
        assertThat(response.rows()[0][2]).isEqualTo("Sandwitch Maker");
        assertThat(response.rows()[0][3]).isEqualTo(2L);
    }

    @Test
    public void testGroupByAggregateOnNestedColumn() throws Exception {
        this.setup.groupBySetup();
        waitNoPendingTasksOnAll();
        execute("select race, count(details['job']) from characters group by race order by race");
        assertThat(response.rowCount()).isEqualTo(3);
        assertThat(response.rows()[0][0]).isEqualTo("Android");
        assertThat(response.rows()[0][1]).isEqualTo(0L);

        assertThat(response.rows()[1][0]).isEqualTo("Human");
        assertThat(response.rows()[1][1]).isEqualTo(2L);

        assertThat(response.rows()[2][0]).isEqualTo("Vogon");
        assertThat(response.rows()[2][1]).isEqualTo(0L);
    }

    @Test
    public void testGroupByAggregateOnNestedColumnOrderBy() throws Exception {
        this.setup.groupBySetup();
        waitNoPendingTasksOnAll();
        execute("select race, count(details['job']) from characters group by race order by count(details['job']) desc limit 1");
        assertThat(response.rowCount()).isEqualTo(1);
        assertThat(response.cols()).isEqualTo(new String[]{"race", "count(details['job'])"});
        assertThat(response.rows()[0][0]).isEqualTo("Human");
        assertThat(response.rows()[0][1]).isEqualTo(2L);
    }

    @Test
    public void testGroupByAggregateOnNestedColumnOrderByAlias() throws Exception {
        this.setup.groupBySetup();
        waitNoPendingTasksOnAll();
        execute("select race, count(details['job']) as job_count from characters group by race order by job_count desc limit 1");
        assertThat(response.rowCount()).isEqualTo(1);
        assertThat(response.cols()).isEqualTo(new String[]{"race", "job_count"});

        assertThat(response.rows()[0][0]).isEqualTo("Human");
        assertThat(response.rows()[0][1]).isEqualTo(2L);
    }

    @Test
    public void testGroupByUnknownResultColumn() throws Exception {
        this.setup.groupBySetup();
        Asserts.assertSQLError(() -> execute("select details_ignored['lol'] from characters group by race"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("'details_ignored['lol']' must appear in the GROUP BY clause");
    }

    @Test
    public void test_group_by_undefined_column_casted() throws Exception {
        this.setup.groupBySetup();
        execute("select max(age) from characters group by details_ignored['lol']::String");
        assertThat(response.rowCount()).isEqualTo(1);
        assertThat(response.rows()[0][0]).isEqualTo(112);
    }

    @Test
    public void testGroupByUnknownWhere() throws Exception {
        this.setup.groupBySetup();
        execute("select max(birthdate), race from characters where details_ignored['lol']='funky' group by race");
        assertThat(response.rowCount()).isEqualTo(0);
    }

    @Test
    public void testNonDistributedGroupByNoMatch() throws Exception {
        execute("create table characters (" +
                " details_ignored object(ignored)," +
                " race string" +
                ") clustered into 1 shards");
        ensureYellow();
        execute("select race from characters where details_ignored['lol']='funky' group by race");
        assertThat(response.rowCount()).isEqualTo(0);
    }

    @Test
    public void testGlobalAggregateUnknownWhere() throws Exception {
        this.setup.groupBySetup();
        execute("select max(birthdate) from characters where details_ignored['lol']='funky'");
        assertThat(response.rowCount()).isEqualTo(1);
        assertThat(response.rows()[0][0]).isNull();
    }

    @Test
    public void testAggregateNonExistingColumn() throws Exception {
        this.setup.groupBySetup();
        execute("select max(details_ignored['lol']), race from characters group by race order by race");
        assertThat(response).hasRows(
            "NULL| Android",
            "NULL| Human",
            "NULL| Vogon"
        );
    }

    @Test
    public void testHavingGlobalAggregation() throws Exception {
        this.setup.groupBySetup("integer");
        execute("select min(birthdate), min(age) from characters having min(age) < 33 and max(age) > 100");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.rows()[0].length).isEqualTo(2);
        assertThat(response.rows()[0][0]).isEqualTo(0L);
        assertThat(response.rows()[0][1]).isEqualTo(32);
    }

    @Test
    public void testHavingGroupBy() throws Exception {
        this.setup.groupBySetup("integer");
        execute("select age from characters group by age having age > 40 order by age");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response.rows()[0][0]).isEqualTo(43);
    }

    @Test
    public void testHavingGroupByOnScalar() throws Exception {
        this.setup.groupBySetup("integer");
        execute("select date_trunc('week', birthdate) from characters group by 1" +
                " having date_trunc('week', birthdate) > 0" +
                " order by date_trunc('week', birthdate)");
        assertThat(response.rowCount()).isEqualTo(2L);
    }

    @Test
    public void testHavingOnSameAggregate() throws Exception {
        this.setup.groupBySetup("integer");
        execute("select avg(birthdate) from characters group by gender\n" +
                "having avg(birthdate) = 181353600000.0");
        assertThat(response).hasRowCount(1L);
        assertThat(response).hasRows("1.813536E11");
    }

    @Test
    public void testHavingOnOtherAggregate() throws Exception {
        this.setup.groupBySetup("integer");
        execute("select avg(birthdate) from characters group by gender\n" +
                "having min(birthdate) > '1970-01-01'");
        assertThat(response).hasRowCount(1L);
        assertThat(response).hasRows("1.813536E11");
    }


    @Test
    public void testHavingGroupByWithAggSelected() throws Exception {
        this.setup.groupBySetup("integer");
        execute("select age, count(*) from characters group by age having age > 40 order by age");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response.rows()[0][0]).isEqualTo(43);
    }

    @Test
    public void testHavingGroupByNonDistributed() throws Exception {
        execute("create table foo (id int, name string, country string) clustered by (country) with (number_of_replicas = 0)");
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


        execute("select country from foo group by country having country = 'Austria'");
        assertThat(response).hasRowCount(1L);
        assertThat(response).hasRows("Austria");

        execute("select count(*), country from foo group by country having country = 'Austria'");
        assertThat(response).hasRowCount(1L);
        assertThat(response).hasRows("3| Austria");

        execute("select country, min(id) from foo group by country having min(id) < 5 ");
        assertThat(response).hasRowCount(2L);
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
        assertThat(response).hasRowCount(1L);
    }

    @Test
    public void testGroupByHavingWithAggregate() throws Exception {
        this.setup.groupBySetup("integer");
        execute("select gender from characters group by gender having min(age) < 33");
        assertThat(response.rowCount()).isEqualTo(1L);
    }

    @Test
    public void testGroupByOnClusteredByColumn() throws Exception {
        execute("create table foo (id int, name string, country string) clustered by (country) with (number_of_replicas = 0)");

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
        assertThat(response).hasRows(
            "3| Austria",
            "2| Germany",
            "1| Italy"
        );
    }

    @Test
    public void testGroupByOnAllPrimaryKeys() throws Exception {
        execute("create table foo (id int primary key, name string primary key) with (number_of_replicas = 0)");
        ensureYellow();

        execute("insert into foo (id, name) values (?, ?)", new Object[][]{
            new Object[]{1, "Arthur"},
            new Object[]{2, "Trillian"},
            new Object[]{3, "Slartibardfast"},
            new Object[]{4, "Marvin"},
        });
        refresh();

        execute("select count(*), name from foo group by id, name order by name desc");
        assertThat(response).hasRows(
            "1| Trillian",
            "1| Slartibardfast",
            "1| Marvin",
            "1| Arthur");
    }

    @Test
    public void testGroupByEmpty() throws Exception {
        execute("create table test (col1 string)");
        ensureYellow();

        execute("select count(*), col1 from test group by col1");
        assertThat(response.rowCount()).isEqualTo(0);
    }

    @Test
    public void testGroupByOnSysNodes() throws Exception {
        execute("select count(*), name from sys.nodes group by name");
        assertThat(response).hasRowCount(2L);

        execute("select count(*), hostname from sys.nodes group by hostname");
        assertThat(response).hasRowCount(1L);
    }

    @Test
    public void testGroupByCountStringGroupByPrimaryKey() throws Exception {
        execute("create table rankings (" +
                " \"pageURL\" string primary key," +
                " \"pageRank\" int," +
                " \"avgDuration\" int" +
                ") with (number_of_replicas=0)");
        ensureYellow();
        for (int i = 0; i < 100; i++) {
            execute("insert into rankings (\"pageURL\", \"pageRank\", \"avgDuration\") values (?, ?, ?)",
                new Object[]{String.valueOf(i), randomIntBetween(i, i * i), randomInt(i)});
            assertThat(response).hasRowCount(1L);
        }
        execute("refresh table rankings");

        execute("select count(*), \"pageURL\" from rankings group by \"pageURL\" order by 1 desc limit 100");
        assertThat(response).hasRowCount(100L);
        List<Object[]> sortedRows = Stream.of(response.rows())
            .sorted((o1, o2) -> Long.compare((long) o1[0], (long) o2[0]))
            .toList();
        assertThat(Arrays.asList(response.rows())).containsExactlyElementsOf(sortedRows);
    }

    @Test
    public void testGroupByCountString() throws Exception {
        execute("create table rankings (" +
                " \"pageURL\" string," +
                " \"pageRank\" int," +
                " \"avgDuration\" int" +
                ") with (number_of_replicas=0)");
        ensureYellow();
        for (int i = 0; i < 100; i++) {
            execute("insert into rankings (\"pageURL\", \"pageRank\", \"avgDuration\") values (?, ?, ?)",
                new Object[]{randomAsciiLettersOfLength(10 + (i % 3)), randomIntBetween(i, i * i), randomInt(i)});
        }
        execute("refresh table rankings");
        execute("select count(*), \"pageURL\" from rankings group by \"pageURL\" order by 1 desc limit 100");
        assertThat(response).hasRowCount(100L);
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
        assertThat(response).hasRows("99.6| https://Ä.com| 99.6");
    }

    @Test
    public void testGroupByWithHavingAndLimit() throws Exception {
        execute("create table likes (" +
                "   event_id string," +
                "   item_id string" +
                ") clustered into 2 shards with (number_of_replicas = 0)");
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
            assertThat(response).hasRowCount(1L);
        } catch (Exception e) {
            fail(e.getMessage());
        }

        try {
            execute("select count(*), item_id from likes where event_id = 'event1' group by 2 having count(*) > 1 limit 100");
            assertThat(response).hasRowCount(1L);
        } catch (Exception e) {
            fail(e.getMessage());
        }

        try {
            execute("select item_id, count(*) from likes where event_id = 'event1' group by 1 having count(*) > 1");
            assertThat(response).hasRowCount(1L);
        } catch (Exception e) {
            fail(e.getMessage());
        }

        try {
            execute("select item_id, count(*) from likes where event_id = 'event1' group by 1 having count(*) > 1 limit 100");
            assertThat(response).hasRowCount(1L);
        } catch (Exception e) {
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
            assertThat(response).hasRowCount(1L);
        } catch (Exception e) {
            fail(e.getMessage());
        }

        try {
            execute("select count(*), item_id from likes where event_id = 'event1' group by 2 having count(*) > 1 limit 100");
            assertThat(response).hasRowCount(1L);
        } catch (Exception e) {
            fail(e.getMessage());
        }

        try {
            execute("select item_id, count(*) from likes where event_id = 'event1' group by 1 having count(*) > 1");
            assertThat(response).hasRowCount(1L);
        } catch (Exception e) {
            fail(e.getMessage());
        }

        try {
            execute("select item_id, count(*) from likes where event_id = 'event1' group by 1 having count(*) > 1 limit 100");
            assertThat(response).hasRowCount(1L);
        } catch (Exception e) {
            fail(e.getMessage());
        }

        try {
            execute("select count(*), item_id from likes group by item_id having min(event_id) = 'event1'");
            assertThat(response).hasRowCount(2L);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void groupByAggregateStdDevByte() throws Exception {
        this.setup.groupBySetup("byte");

        execute("select stddev(age), gender from characters group by gender order by gender");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response.rows()[0][0]).isEqualTo(5.5d);
        assertThat(response.rows()[1][0]).isEqualTo(39.0d);
    }

    @Test
    public void groupByAggregateVarianceByte() throws Exception {
        this.setup.groupBySetup("byte");

        execute("select variance(age), gender from characters group by gender order by gender");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response.rows()[0][0]).isEqualTo(30.25d);
        assertThat(response.rows()[1][0]).isEqualTo(1521.0d);
    }

    @Test
    public void groupByAggregateStdDevDouble() throws Exception {
        this.setup.groupBySetup("double");

        execute("select stddev(age), gender from characters group by gender order by gender");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response.rows()[0][0]).isEqualTo(5.5d);
        assertThat(response.rows()[1][0]).isEqualTo(39.0d);
    }

    @Test
    public void groupByStatsAggregatesGlobal() throws Exception {
        this.setup.groupBySetup("short");
        execute("select min(age), mean(age), geometric_mean(age), max(age), variance(age), stddev(age) from characters");
        Object[] row = response.rows()[0];
        assertThat(row[0]).isEqualTo((short) 32);
        assertThat(row[1]).isEqualTo(55.25d);

        assertThat((double) row[2]).isCloseTo(47.84415001097868d, Percentage.withPercentage(0.01));
        assertThat(row[3]).isEqualTo((short) 112);
        assertThat(row[4]).isEqualTo(1090.6875d);
        assertThat(row[5]).isEqualTo(33.025558284456d);
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
        assertThat(response).hasRows(
            "2| 42", // assert that different partitions have been merged
            "1| 43",
            "1| 44",
            "1| 45");
    }

    @Test
    public void testFilterByScore() throws Exception {
        execute("create table locations (" +
                " altitude int," +
                " name string," +
                " description string index using fulltext" +
                ") clustered into 1 shards with (number_of_replicas = 0)"); // 1 shard because scoring is relative within a shard
        ensureYellow();

        execute("insert into locations (altitude, name, description) values (420, 'Crate Dornbirn', 'A nice place in a nice country')");
        execute("insert into locations (altitude, name, description) values (230, 'Crate Berlin', 'Also very nice place mostly nice in summer')");
        execute("insert into locations (altitude, name, description) values (70, 'Crate SF', 'A nice place with lot of sunshine')");
        execute("refresh table locations");

        execute("select min(altitude) as altitude, name, _score from locations where match(description, 'nice') " +
                "and _score >= 1.08 group by name, _score order by name");
        assertThat(response.rowCount()).isEqualTo(2L);
    }

    @Test
    public void testDistinctWithGroupBy() throws Exception {
        execute("select DISTINCT max(col1), min(col1) from unnest([1,1,1,2,2,2,2,3,3],[1,2,3,1,2,3,4,1,2]) " +
                "group by col2 order by 2, 1");
        assertThat(response).hasRows(
            "2| 1",
            "3| 1",
            "2| 2");
    }

    @Test
    public void testDistinctWithGroupByLimitAndOffset() throws Exception {
        execute("select DISTINCT max(col2), min(col2) from " +
                "unnest([1,1,2,2,3,3,4,4,5,5,6,6],[1,2,2,1,2,1,3,4,4,3,5,6]) " +
                "group by col1 order by 1 desc, 2 limit 2 offset 1");
        assertThat(response).hasRows(
            "4| 3",
            "2| 1"
        );
    }

    @Test
    public void testDistinctOnJoinWithGroupBy() throws Exception {
        execute("select DISTINCT max(t1.col1), min(t2.col2) from " +
                "unnest([1,1,1,2,2,2,2,3,3],[1,2,3,1,2,3,4,1,2]) as t1, " +
                "unnest([1,1,1,2,2,2,2,3,3],[1,2,3,1,2,3,4,1,2]) as t2 " +
                "where t1.col1=t2.col2 " +
                "group by t1.col2 order by 2, 1");
        assertThat(response).hasRows(
            "2| 1",
            "3| 1",
            "2| 2"
        );
    }

    @Test
    public void testDistinctOnSubselectWithGroupBy() throws Exception {
        execute("select * from (" +
                " select distinct max(col1), min(col1) from unnest([1,1,1,2,2,2,2,3,3],[1,2,3,1,2,3,4,1,2]) " +
                " group by col2 order by 2, 1 limit 2" +
                ") t order by 1 desc limit 1");
        assertThat(response).hasRows("3| 1");
    }

    @Test
    public void testGroupByOnScalarOnArray() throws Exception {
        execute("select string_to_array(unnest, ' ')[2], count(*) " +
                "from unnest([' select foo', 'insert into ', 'select 1']) " +
                "group by 1 order by 2 desc");
        assertThat(response).hasRows(
            "into| 1",
            "1| 1",
            "select| 1"
        );
    }

    @Test
    public void testGroupByOnComplexLiterals() throws Exception {
        execute("select '{coordinates=[[0.0, 0.0], [1.0, 1.0]], type=LineString}', count(*) " +
                "from employees group by 1");
        assertThat(response).hasRows("{coordinates=[[0.0, 0.0], [1.0, 1.0]], type=LineString}| 6");
        execute("select {id1=1, id2=36}, count(*) " +
                "from employees group by 1");
        assertThat(response).hasRows("{id1=1, id2=36}| 6");
    }

    @Test
    public void testGroupByOnIndexOff() throws Exception {
        execute("create table t1 (i int index off, s string index off)");
        execute("insert into t1 (i, s) values (?,?)",
                new Object[][] {
                    {1, "foo"},
                    {2, "bar"},
                    {1, "foo"}
                });
        refresh();
        execute("select count(*), i, s from t1 group by i, s order by 1");
        assertThat(response).hasRows(
            "1| 2| bar",
            "2| 1| foo"
        );
    }

    @Test
    public void testAggregateOnIndexOff() throws Exception {
        execute("create table t1 (i int index off, s string index off)");
        execute("insert into t1 (i, s) values (?,?)",
                new Object[][] {
                    {1, "foo"},
                    {2, "foobar"},
                    {1, "foo"}
                });
        refresh();
        execute("select sum(i), max(s) from t1");
        assertThat(response).hasRows("4| foobar");
    }

    @Test
    public void testGroupBySingleNumberWithNullValues() {
        execute("create table t (along long, aint int, ashort short) clustered into 2 shards with (number_of_replicas = 0)");
        execute("insert into t (along,aint,ashort) values (1,1,1), (1,1,1), (1,1,1), (2,2,2), (2,2,2), (null,null,null), " +
                "(null,null,null), (null,null,null), (null,null,null)");
        execute("refresh table t");

        assertThat(execute("select along, count(*) from t group by along order by 2 desc")).hasRows(
            "NULL| 4",
            "1| 3",
            "2| 2"
        );

        assertThat(execute("select aint, count(*) from t group by aint order by 2 desc")).hasRows(
            "NULL| 4",
            "1| 3",
            "2| 2"
        );

        assertThat(execute("select ashort, count(*) from t group by ashort order by 2 desc")).hasRows(
            "NULL| 4",
            "1| 3",
            "2| 2"
        );
    }

    @Test
    public void testSelectAndGroupBySingleStringKeyForLowCardinalityField() {
        execute("create table t (name string) clustered into 1 shards");
        // has low cardinality ration: CARDINALITY_RATIO_THRESHOLD (0.5) > 2 terms / 4 docs
        execute("insert into t (name) values ('a'), ('b'), ('a'), ('b')");
        execute("refresh table t");
        execute("select name, count(name) from t group by 1 order by 1");
        assertThat(response).hasRows(
            "a| 2",
            "b| 2"
        );
    }

    @Test
    public void testSelectCollectSetAndGroupBy() {
        execute("SELECT\n" +
                "col1,\n" +
                "collect_set(col1)\n" +
                "FROM unnest(ARRAY[1, 2, 2, 3, 4, 5, null]) col1 " +
                "GROUP BY col1 " +
                "ORDER BY col1 NULLS LAST");
        assertThat(response).hasRows(
            "1| [1]",
            "2| [2]",
            "3| [3]",
            "4| [4]",
            "5| [5]",
            "NULL| []"
        );
    }

    @UseRandomizedOptimizerRules(0)
    @Test
    public void test_optimized_limit_distinct_returns_2_unique_items() throws Throwable {
        execute("create table m.tbl (id int primary key)");
        Object[][] ids = new Object[100][];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = new Object[] { i };
        }
        execute("insert into m.tbl (id) values (?)", ids);
        execute("refresh table m.tbl");
        execute("analyze");
        execute("explain (costs false) select distinct id from m.tbl limit 2");
        assertThat(response).hasLines(
            "LimitDistinct[2::bigint;0 | [id]]",
            "  └ Collect[m.tbl | [id] | true]"
        );
        execute("select distinct id from m.tbl limit 2");
        assertThat(response).hasRowCount(2L);
        Object firstId = response.rows()[0][0];
        Object secondId = response.rows()[1][0];
        assertThat(firstId).isNotEqualTo(secondId);
    }

    @Test
    public void test_select_distinct_with_limit_and_offset_applies_limit_and_offset_on_distinct_resultset() throws Exception {
        execute("create table tbl (x int)");
        execute("insert into tbl (x) values (1), (1), (2), (3)");
        execute("refresh table tbl");

        execute("select distinct x from tbl limit 1 offset 3");
        assertThat(response).hasRowCount(0L);
    }

    @Test
    public void test_group_by_on_subscript_on_object_of_sub_relation() {
        execute("create table tbl (obj object as (x int))");
        execute("insert into tbl (obj) values ({x=10})");
        execute("refresh table tbl");
        execute("select obj['x'] from (select obj from tbl) as t group by obj['x']");
        assertThat(response).hasRows("10");
    }

    @Test
    public void test_select_same_aggregate_multiple_times() throws Exception {
        execute("create table doc.tbl (name text, x int)");
        execute("insert into doc.tbl (name, x) values ('Apple', 1), ('Apple', 2), ('Apple', 3)");
        execute("refresh table doc.tbl");


        execute("explain (costs false) select name, count(x), count(x) from doc.tbl group by name");
        assertThat(response).hasLines(
            "Eval[name, count(x), count(x)]",
            "  └ GroupHashAggregate[name | count(x)]",
            "    └ Collect[doc.tbl | [x, name] | true]"
        );
        execute("select name, count(x), count(x) from doc.tbl group by name");
        assertThat(response).hasRows("Apple| 3| 3");
    }

    @Test
    @UseJdbc(1)
    public void test_group_by_on_duplicate_keys() throws Exception {
        execute("create table tbl (city text, name text, id text, location geo_point)");
        execute(
            "insert into tbl (id, name, location, city) values " +
            " ('122021430M', 'Rue Penavayre', [2.573609,44.35007], 'Rodez'), " +
            " ('013440476U', 'Avenue de Trévoux ', [5.201722,46.20096], 'Saint-Denis-lès-Bourg'), " +
            " ('021140050C', 'Rue Paul Doumer ', [3.426928,49.04915], 'Brasles') ");
        execute("refresh table tbl");
        execute("SELECT DISTINCT id, name, id, location, city FROM tbl");

        // Ensure deterministic order for assertion → sort by id
        Arrays.sort(response.rows(), (a, b) -> ((String) a[0]).compareTo((String) b[0]));
        assertThat(response).hasRows(
            "013440476U| Avenue de Trévoux | 013440476U| Pt(x=5.20172193646431,y=46.200959966517985)| Saint-Denis-lès-Bourg",
            "021140050C| Rue Paul Doumer | 021140050C| Pt(x=3.426927952095866,y=49.04914998449385)| Brasles",
            "122021430M| Rue Penavayre| 122021430M| Pt(x=2.573608970269561,y=44.35006999410689)| Rodez"
        );
    }

    @Test
    public void test_group_on_null_literal() {
        execute("select null, count(*) from unnest([1, 2]) group by 1");
        assertThat(response).hasRows("NULL| 2");

        execute("SELECT nn, sum(x) from (SELECT NULL as nn, x from unnest([1]) tbl (x)) t GROUP BY nn");
        assertThat(response).hasRows(
            "NULL| 1"
        );
    }

    @Test
    public void test_group_by_on_ignored_column() {
        execute("create table tbl (o object (ignored))");
        execute("insert into tbl (o) values ({x='foo'}), ({x=10})");
        assertThat(response).hasRowCount(2);
        execute("refresh table tbl");
        execute("select o['x'], count(*) from tbl group by o['x']");
        assertThat(printedTable(response.rows())).satisfiesAnyOf(
            rows -> assertThat(rows).isEqualTo("foo| 1\n10| 1\n"),
            rows -> assertThat(rows).isEqualTo("10| 1\nfoo| 1\n")
        );
    }

    @Test
    public void test_group_by_array_type() {
        execute("create table tbl (arr array(int))");
        execute("insert into tbl(arr) values ([1, 2, null]), ([2, 1]), ([1, 2]), ([1, 2])");
        execute("insert into tbl(arr) values ([]::int[]), ([]::int[]), ([]::int[])");
        execute("insert into tbl(arr) values ([null]), ([null]), ([null]), ([null])");
        execute("insert into tbl(arr) values (null), (null), (null), (null), (null)");
        refresh();
        execute("select arr, count(*) cnt from tbl group by arr order by cnt, arr[1]");
        assertThat(response).hasRows(
            "[1, 2, null]| 1",
            "[2, 1]| 1",
            "[1, 2]| 2",
            "[]| 3",
            "[null]| 4",
            "NULL| 5"
        );
    }
}
