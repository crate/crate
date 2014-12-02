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

import io.crate.Build;
import io.crate.Version;
import io.crate.action.sql.SQLActionException;
import io.crate.action.sql.SQLResponse;
import io.crate.executor.TaskResult;
import io.crate.metadata.settings.CrateSettings;
import io.crate.test.integration.ClassLifecycleIntegrationTest;
import io.crate.testing.SQLTransportExecutor;
import io.crate.testing.TestingHelpers;
import org.hamcrest.Matchers;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;

public class TransportSQLActionClassLifecycleTest extends ClassLifecycleIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static boolean dataInitialized = false;
    private static SQLTransportExecutor executor;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void initTestData() throws Exception {
        synchronized (SysShardsTest.class) {
            if (dataInitialized) {
                return;
            }
            executor = SQLTransportExecutor.create(ClassLifecycleIntegrationTest.GLOBAL_CLUSTER);
            Setup setup = new Setup(executor);
            setup.partitionTableSetup();
            setup.groupBySetup();
            executor.ensureGreen();
            dataInitialized = true;
        }
    }

    @After
    public void resetSettings() throws Exception {
        // reset stats settings in case of some tests changed it and failed without resetting.
        executor.exec("reset global stats.enabled, stats.jobs_log_size, stats.operations_log_size");
    }

    @AfterClass
    public synchronized static void after() throws Exception {
        if (executor != null) {
            executor = null;
        }
    }


    @Test
    public void testRefreshSystemTable() throws Exception {
        SQLResponse response = executor.exec("refresh table sys.shards");
        assertFalse(response.hasRowCount());
        assertThat(response.rows(), is(TaskResult.EMPTY_RESULT.rows()));
    }

    @Test
    public void testSelectNonExistentGlobalExpression() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Schema 'suess' unknown");
        executor.exec("select count(race), suess.cluster.name from characters");
    }

    @Test
    public void testSelectOrderByNullSortingASC() throws Exception {
        SQLResponse response = executor.exec("select age from characters order by age");
        assertEquals(32, response.rows()[0][0]);
        assertEquals(34, response.rows()[1][0]);
        assertEquals(43, response.rows()[2][0]);
        assertEquals(112, response.rows()[3][0]);
        assertEquals(null, response.rows()[4][0]);
        assertEquals(null, response.rows()[5][0]);
        assertEquals(null, response.rows()[6][0]);
    }

    @Test
    public void testSelectDoc() throws Exception {
        SQLResponse response = executor.exec("select _doc from characters order by name desc limit 1");
        assertArrayEquals(new String[]{"_doc"}, response.cols());
        assertEquals(
                "{details={job=Mathematician}, name=Trillian, age=32, " +
                        "birthdate=276912000000, gender=female, race=Human}\n",
                TestingHelpers.printedTable(response.rows()));
    }

    @Test
    public void testSelectRaw() throws Exception {
        SQLResponse response = executor.exec("select _raw from characters order by name desc limit 1");
        assertEquals(
                "{\"race\":\"Human\",\"gender\":\"female\",\"age\":32,\"birthdate\":276912000000," +
                        "\"name\":\"Trillian\",\"details\":{\"job\":\"Mathematician\"}}\n",
                TestingHelpers.printedTable(response.rows()));
    }

    @Test
    public void testSelectRawWithGrouping() throws Exception {
        SQLResponse response = executor.exec("select name, _raw from characters " +
                "group by _raw, name order by name desc limit 1");
        assertEquals(
                "Trillian| {\"race\":\"Human\",\"gender\":\"female\",\"age\":32,\"birthdate\":276912000000," +
                        "\"name\":\"Trillian\",\"details\":{\"job\":\"Mathematician\"}}\n",
                TestingHelpers.printedTable(response.rows()));
    }

    @Test
    public void testSelectOrderByGlobalExpression() throws Exception {
        SQLResponse response = executor.exec(
            "select count(*), sys.cluster.name from characters group by sys.cluster.name order by sys.cluster.name");
        assertEquals(1, response.rowCount());
        assertEquals(7L, response.rows()[0][0]);

        assertEquals(GLOBAL_CLUSTER.clusterName(), response.rows()[0][1]);
    }

    @Test
    public void testSelectOrderByNullSortingDESC() throws Exception {
        SQLResponse response = executor.exec("select age from characters order by age desc");
        assertEquals(null, response.rows()[0][0]);
        assertEquals(null, response.rows()[1][0]);
        assertEquals(null, response.rows()[2][0]);
        assertEquals(112, response.rows()[3][0]);
        assertEquals(43, response.rows()[4][0]);
        assertEquals(34, response.rows()[5][0]);
        assertEquals(32, response.rows()[6][0]);
    }


    @Test
    public void testSelectGlobalExpressionGroupBy() throws Exception {
        SQLResponse response = executor.exec(
            "select count(distinct race), sys.cluster.name from characters group by sys.cluster.name");
        assertEquals(1, response.rowCount());
        for (int i=0; i<response.rowCount();i++) {
            assertEquals(3L, response.rows()[i][0]);
            assertEquals(GLOBAL_CLUSTER.clusterName(), response.rows()[i][1]);
        }
    }

    @Test
    public void testSelectGlobalExpressionGroupByWith2GroupByKeys() throws Exception {
        SQLResponse response = executor.exec(
            "select count(name), sys.cluster.name from characters " +
                "group by race, sys.cluster.name order by count(name) desc");
        assertEquals(3, response.rowCount());
        assertEquals(4L, response.rows()[0][0]);
        assertEquals(GLOBAL_CLUSTER.clusterName(), response.rows()[0][1]);
        assertEquals(response.rows()[0][1], response.rows()[1][1]);
        assertEquals(response.rows()[0][1], response.rows()[2][1]);
    }

    @Test
    public void testSelectGlobalExpressionGlobalAggregate() throws Exception {
        SQLResponse response = executor.exec("select count(distinct race), sys.cluster.name " +
                "from characters group by sys.cluster.name");
        assertEquals(1, response.rowCount());
        assertArrayEquals(new String[]{"count(DISTINCT race)", "sys.cluster.name"}, response.cols());
        assertEquals(3L, response.rows()[0][0]);
        assertEquals(GLOBAL_CLUSTER.clusterName(), response.rows()[0][1]);
    }

    @Test
    public void testSelectGroupByOrderByNullSortingASC() throws Exception {
        SQLResponse response = executor.exec("select age from characters group by age order by age");
        assertEquals(32, response.rows()[0][0]);
        assertEquals(34, response.rows()[1][0]);
        assertEquals(43, response.rows()[2][0]);
        assertEquals(112, response.rows()[3][0]);
        assertEquals(null, response.rows()[4][0]);
    }

    @Test
    public void testSelectGroupByOrderByNullSortingDESC() throws Exception {
        SQLResponse response = executor.exec("select age from characters group by age order by age desc");
        assertEquals(null, response.rows()[0][0]);
        assertEquals(112, response.rows()[1][0]);
        assertEquals(43, response.rows()[2][0]);
        assertEquals(34, response.rows()[3][0]);
        assertEquals(32, response.rows()[4][0]);
    }

    @Test
    public void testSelectAggregateOnGlobalExpression() throws Exception {
        SQLResponse response = executor.exec("select count(sys.cluster.name) from characters");
        assertEquals(1, response.rowCount());
        assertEquals(7L, response.rows()[0][0]);

        response = executor.exec("select count(distinct sys.cluster.name) from characters");
        assertEquals(1, response.rowCount());
        assertEquals(1L, response.rows()[0][0]);
    }

    @Test
    public void testSelectGlobalExpressionWithAlias() throws Exception {
        SQLResponse response = executor.exec("select sys.cluster.name as cluster_name, race from characters " +
            "group by sys.cluster.name, race " +
            "order by cluster_name, race");
        assertEquals(3L, response.rowCount());
        assertEquals(GLOBAL_CLUSTER.clusterName(), response.rows()[0][0]);
        assertEquals(GLOBAL_CLUSTER.clusterName(), response.rows()[1][0]);
        assertEquals(GLOBAL_CLUSTER.clusterName(), response.rows()[2][0]);

        assertEquals("Android", response.rows()[0][1]);
        assertEquals("Human", response.rows()[1][1]);
        assertEquals("Vogon", response.rows()[2][1]);
    }


    @Test
    public void testGlobalAggregateSimple() throws Exception {
        SQLResponse response = executor.exec("select max(age) from characters");

        assertEquals(1, response.rowCount());
        assertEquals("max(age)", response.cols()[0]);
        assertEquals(112, response.rows()[0][0]);

        response = executor.exec("select min(name) from characters");

        assertEquals(1, response.rowCount());
        assertEquals("min(name)", response.cols()[0]);
        assertEquals("Anjie", response.rows()[0][0]);

        response = executor.exec("select avg(age) as median_age from characters");
        assertEquals(1, response.rowCount());
        assertEquals("median_age", response.cols()[0]);
        assertEquals(55.25d, response.rows()[0][0]);

        response = executor.exec("select sum(age) as sum_age from characters");
        assertEquals(1, response.rowCount());
        assertEquals("sum_age", response.cols()[0]);
        assertEquals(221.0d, response.rows()[0][0]);
    }

    @Test
    public void testGlobalAggregateWithoutNulls() throws Exception {
        SQLResponse firstResp = executor.exec("select sum(age) from characters");
        SQLResponse secondResp = executor.exec("select sum(age) from characters where age is not null");

        assertEquals(
            firstResp.rowCount(),
            secondResp.rowCount()
        );
        assertEquals(
            firstResp.rows()[0][0],
            secondResp.rows()[0][0]
        );
    }

    @Test
    public void testGlobalAggregateNullRowWithoutMatchingRows() throws Exception {
        SQLResponse response = executor.exec(
            "select sum(age), avg(age) from characters where characters.age > 112");
        assertEquals(1, response.rowCount());
        assertNull(response.rows()[0][0]);
        assertNull(response.rows()[0][1]);

        response = executor.exec("select sum(age) from characters limit 0");
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testGlobalAggregateMany() throws Exception {
        SQLResponse response = executor.exec("select sum(age), min(age), max(age), avg(age) from characters");
        assertEquals(1, response.rowCount());
        assertEquals(221.0d, response.rows()[0][0]);
        assertEquals(32, response.rows()[0][1]);
        assertEquals(112, response.rows()[0][2]);
        assertEquals(55.25d, response.rows()[0][3]);
    }

    @Test (expected = SQLActionException.class)
    public void selectMultiGetRequestFromNonExistentTable() throws Exception {
        executor.exec("SELECT * FROM \"non_existent\" WHERE \"_id\" in (?,?)", new Object[]{"1", "2"});
    }

    @Test
    public void testGroupByNestedObject() throws Exception {
        SQLResponse response = executor.exec("select count(*), details['job'] from characters " +
            "group by details['job'] order by count(*), details['job']");
        assertEquals(3, response.rowCount());
        assertEquals(1L, response.rows()[0][0]);
        assertEquals("Mathematician", response.rows()[0][1]);
        assertEquals(1L, response.rows()[1][0]);
        assertEquals("Sandwitch Maker", response.rows()[1][1]);
        assertEquals(5L, response.rows()[2][0]);
        assertNull(null, response.rows()[2][1]);
    }

    @Test
    public void testCountWithGroupByOrderOnKeyDescAndLimit() throws Exception {
        SQLResponse response = executor.exec(
            "select count(*), race from characters group by race order by race desc limit 2");

        assertEquals(2L, response.rowCount());
        assertEquals(2L, response.rows()[0][0]);
        assertEquals("Vogon", response.rows()[0][1]);
        assertEquals(4L, response.rows()[1][0]);
        assertEquals("Human", response.rows()[1][1]);
    }

    @Test
    public void testCountWithGroupByOrderOnKeyAscAndLimit() throws Exception {
        SQLResponse response = executor.exec(
            "select count(*), race from characters group by race order by race asc limit 2");

        assertEquals(2, response.rowCount());
        assertEquals(1L, response.rows()[0][0]);
        assertEquals("Android", response.rows()[0][1]);
        assertEquals(4L, response.rows()[1][0]);
        assertEquals("Human", response.rows()[1][1]);
    }

    @Test
    public void testCountWithGroupByNullArgs() throws Exception {
        SQLResponse response = executor.exec("select count(*), race from characters group by race", new Object[] { null });
        assertEquals(3, response.rowCount());
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
    }

    @Test
    public void testGroupByAndOrderByAlias() throws Exception {
        SQLResponse response = executor.exec(
            "select characters.race as test_race from characters group by characters.race order by characters.race");
        assertEquals(3, response.rowCount());

        response = executor.exec(
            "select characters.race as test_race from characters group by characters.race order by test_race");
        assertEquals(3, response.rowCount());
    }

    @Test
    public void testCountWithGroupByWithWhereClause() throws Exception {
        SQLResponse response = executor.exec(
            "select count(*), race from characters where race = 'Human' group by race");
        assertEquals(1, response.rowCount());
    }

    @Test
    public void testCountWithGroupByOrderOnAggAscFuncAndLimit() throws Exception {
        SQLResponse response = executor.exec("select count(*), race from characters " +
                "group by race order by count(*) asc limit ?",
            new Object[]{2});

        assertEquals(2, response.rowCount());
        assertEquals(1L, response.rows()[0][0]);
        assertEquals("Android", response.rows()[0][1]);
        assertEquals(2L, response.rows()[1][0]);
        assertEquals("Vogon", response.rows()[1][1]);
    }

    @Test
    public void testCountWithGroupByOrderOnAggAscFuncAndSecondColumnAndLimit() throws Exception {
        SQLResponse response = executor.exec("select count(*), gender, race from characters " +
            "group by race, gender order by count(*) desc, race, gender asc limit 2");

        assertEquals(2L, response.rowCount());
        assertEquals(2L, response.rows()[0][0]);
        assertEquals("female", response.rows()[0][1]);
        assertEquals("Human", response.rows()[0][2]);
        assertEquals(2L, response.rows()[1][0]);
        assertEquals("male", response.rows()[1][1]);
        assertEquals("Human", response.rows()[1][2]);
    }

    @Test
    public void testCountWithGroupByOrderOnAggAscFuncAndSecondColumnAndLimitAndOffset() throws Exception {
        SQLResponse response = executor.exec("select count(*), gender, race from characters " +
            "group by race, gender order by count(*) desc, race asc limit 2 offset 2");

        assertEquals(2, response.rowCount());
        assertEquals(2L, response.rows()[0][0]);
        assertEquals("male", response.rows()[0][1]);
        assertEquals("Vogon", response.rows()[0][2]);
        assertEquals(1L, response.rows()[1][0]);
        assertEquals("male", response.rows()[1][1]);
        assertEquals("Android", response.rows()[1][2]);
    }

    @Test
    public void testCountWithGroupByOrderOnAggAscFuncAndSecondColumnAndLimitAndTooLargeOffset() throws Exception {
        SQLResponse response = executor.exec("select count(*), gender, race from characters " +
                "group by race, gender order by count(*) desc, race asc limit 2 offset 20");

        assertEquals(0, response.rows().length);
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testCountWithGroupByOrderOnAggDescFuncAndLimit() throws Exception {
        SQLResponse response = executor.exec(
                "select count(*), race from characters group by race order by count(*) desc limit 2");

        assertEquals(2, response.rowCount());
        assertEquals(4L, response.rows()[0][0]);
        assertEquals("Human", response.rows()[0][1]);
        assertEquals(2L, response.rows()[1][0]);
        assertEquals("Vogon", response.rows()[1][1]);
    }

    @Test
    public void testDateRange() throws Exception {
        SQLResponse response = executor.exec("select * from characters where birthdate > '1970-01-01'");
        assertThat(response.rowCount(), Matchers.is(2L));
    }

    @Test
    public void testOrderByNullsFirstAndLast() throws Exception {
        SQLResponse response = executor.exec(
                "select details['job'] from characters order by details['job'] nulls first limit 1");
        assertNull(response.rows()[0][0]);

        response = executor.exec(
                "select details['job'] from characters order by details['job'] desc nulls first limit 1");
        assertNull(response.rows()[0][0]);

        response = executor.exec(
                "select details['job'] from characters order by details['job'] nulls last");
        assertNull(response.rows()[((Long) response.rowCount()).intValue() - 1][0]);

        response = executor.exec(
                "select details['job'] from characters order by details['job'] desc nulls last");
        assertNull(response.rows()[((Long) response.rowCount()).intValue() - 1][0]);


        response = executor.exec(
                "select distinct details['job'] from characters order by details['job'] desc nulls last");
        assertNull(response.rows()[((Long) response.rowCount()).intValue() - 1][0]);
    }

    @Test
    public void testCopyToDirectoryOnPartitionedTableWithPartitionClause() throws Exception {
        String uriTemplate = Paths.get(folder.getRoot().toURI()).toAbsolutePath().toString();
        SQLResponse response = executor.exec("copy parted partition (date='2014-01-01') to DIRECTORY ?", uriTemplate);
        assertThat(response.rowCount(), is(2L));

        List<String> lines = new ArrayList<>(2);
        DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(folder.getRoot().toURI()), "*.json");
        for (Path entry: stream) {
            lines.addAll(Files.readAllLines(entry, StandardCharsets.UTF_8));
        }
        assertThat(lines.size(), is(2));
        for (String line : lines) {
            assertTrue(line.contains("2") || line.contains("1"));
            assertFalse(line.contains("1388534400000"));  // date column not included in export
            assertThat(line, startsWith("{"));
            assertThat(line, endsWith("}"));
        }
    }

    @Test
    public void testCopyToDirectoryOnPartitionedTableWithoutPartitionClause() throws Exception {
        String uriTemplate = Paths.get(folder.getRoot().toURI()).toAbsolutePath().toString();
        SQLResponse response = executor.exec("copy parted to DIRECTORY ?", uriTemplate);
        assertThat(response.rowCount(), is(5L));

        List<String> lines = new ArrayList<>(5);
        DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(folder.getRoot().toURI()), "*.json");
        for (Path entry: stream) {
            lines.addAll(Files.readAllLines(entry, StandardCharsets.UTF_8));
        }
        assertThat(lines.size(), is(5));
        for (String line : lines) {
            // date column included in output
            if (!line.contains("1388534400000")) {
                assertTrue(line.contains("1391212800000"));
            }
            assertThat(line, startsWith("{"));
            assertThat(line, endsWith("}"));
        }
    }

    @Test
    public void testArithmeticFunctions() throws Exception {
        SQLResponse response = executor.exec("select ((2 * 4 - 2 + 1) / 2) % 3 from sys.cluster");
        assertThat(response.cols()[0], is("(((((2 * 4) - 2) + 1) / 2) % 3)"));
        assertThat((Long)response.rows()[0][0], is(0L));

        response = executor.exec("select ((2 * 4.0 - 2 + 1) / 2) % 3 from sys.cluster");
        assertThat((Double)response.rows()[0][0], is(0.5));

        response = executor.exec("select ? + 2 from sys.cluster", 1);
        assertThat((Long)response.rows()[0][0], is(3L));

        response = executor.exec("select load['1'] + load['5'], load['1'], load['5'] from sys.nodes limit 1");
        assertEquals(response.rows()[0][0], (Double) response.rows()[0][1] + (Double) response.rows()[0][2]);
    }

    @Test
    public void testJobLog() throws Exception {
        executor.exec("select name from sys.cluster");
        SQLResponse response= executor.exec("select * from sys.jobs_log");
        assertThat(response.rowCount(), is(0L)); // default length is zero

        executor.exec("set global transient stats.enabled = true, stats.jobs_log_size=1");

        executor.exec("select id from sys.cluster");
        executor.exec("select id from sys.cluster");
        executor.exec("select id from sys.cluster");
        response= executor.exec("select stmt from sys.jobs_log order by ended desc");

        // there are 2 nodes so depending on whether both nodes were hit this should be either 1 or 2
        // but never 3 because the queue size is only 1
        assertThat(response.rowCount(), Matchers.lessThanOrEqualTo(2L));
        assertThat((String)response.rows()[0][0], is("select id from sys.cluster"));

        executor.exec("reset global stats.enabled, stats.jobs_log_size");
        waitNoPendingTasksOnAll();
        response= executor.exec("select * from sys.jobs_log");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testQueryNameFromSysOperations() throws Exception {
        executor.exec("set global stats.enabled = true");
        SQLResponse resp = executor.exec("select name, job_id from sys.operations order by name asc");

        // usually this should return collect on 2 nodes, localMerge on 1 node
        // but it could be that the collect is finished before the localMerge task is started in which
        // case it is missing.

        assertThat(resp.rowCount(), Matchers.greaterThanOrEqualTo(2L));
        List<String> names = new ArrayList<>();
        for (Object[] objects : resp.rows()) {
            names.add((String) objects[0]);
        }
        Collections.sort(names);
        assertTrue(names.contains("collect"));
        executor.exec("set global stats.enabled = false");
    }

    @Test
    public void testSetSingleStatement() throws Exception {
        SQLResponse response = executor.exec("select settings['stats']['jobs_log_size'] from sys.cluster");
        assertThat(response.rowCount(), is(1L));
        assertThat((Integer)response.rows()[0][0], is(CrateSettings.STATS_JOBS_LOG_SIZE.defaultValue()));

        response = executor.exec("set global persistent stats.enabled= true, stats.jobs_log_size=7");
        assertThat(response.rowCount(), is(1L));

        response = executor.exec("select settings['stats']['jobs_log_size'] from sys.cluster");
        assertThat(response.rowCount(), is(1L));
        assertThat((Integer)response.rows()[0][0], is(7));

        response = executor.exec("reset global stats.enabled, stats.jobs_log_size");
        assertThat(response.rowCount(), is(1L));
        waitNoPendingTasksOnAll();

        response = executor.exec("select settings['stats']['enabled'], settings['stats']['jobs_log_size'] from sys.cluster");
        assertThat(response.rowCount(), is(1L));
        assertThat((Boolean)response.rows()[0][0], is(CrateSettings.STATS_ENABLED.defaultValue()));
        assertThat((Integer)response.rows()[0][1], is(CrateSettings.STATS_JOBS_LOG_SIZE.defaultValue()));

    }

    @Test
    public void testSetMultipleStatement() throws Exception {
        SQLResponse response = executor.exec(
                "select settings['stats']['operations_log_size'], settings['stats']['enabled'] from sys.cluster");
        assertThat(response.rowCount(), is(1L));
        assertThat((Integer)response.rows()[0][0], is(CrateSettings.STATS_OPERATIONS_LOG_SIZE.defaultValue()));
        assertThat((Boolean)response.rows()[0][1], is(CrateSettings.STATS_ENABLED.defaultValue()));

        response = executor.exec("set global persistent stats.operations_log_size=1024, stats.enabled=false");
        assertThat(response.rowCount(), is(1L));

        response = executor.exec(
                "select settings['stats']['operations_log_size'], settings['stats']['enabled'] from sys.cluster");
        assertThat(response.rowCount(), is(1L));
        assertThat((Integer)response.rows()[0][0], is(1024));
        assertThat((Boolean)response.rows()[0][1], is(false));

        response = executor.exec("reset global stats.operations_log_size, stats.enabled");
        assertThat(response.rowCount(), is(1L));
        waitNoPendingTasksOnAll();

        response = executor.exec(
                "select settings['stats']['operations_log_size'], settings['stats']['enabled'] from sys.cluster");
        assertThat(response.rowCount(), is(1L));
        assertThat((Integer)response.rows()[0][0], is(CrateSettings.STATS_OPERATIONS_LOG_SIZE.defaultValue()));
        assertThat((Boolean)response.rows()[0][1], is(CrateSettings.STATS_ENABLED.defaultValue()));
    }

    @Test
    public void testSetStatementInvalid() throws Exception {
        try {
            executor.exec("set global persistent stats.operations_log_size=-1024");
            fail("expected SQLActionException, none was thrown");
        } catch (SQLActionException e) {
            assertThat(e.getMessage(), is("Invalid value for argument 'stats.operations_log_size'"));

            SQLResponse response = executor.exec("select settings['stats']['operations_log_size'] from sys.cluster");
            assertThat(response.rowCount(), is(1L));
            assertThat((Integer) response.rows()[0][0], is(CrateSettings.STATS_OPERATIONS_LOG_SIZE.defaultValue()));
        }
    }

    @Test
    public void testSysOperationsLog() throws Exception {
        executor.exec(
            "select count(*), race from characters group by race order by count(*) desc limit 2");
        SQLResponse resp = executor.exec("select count(*) from sys.operations_log");
        assertThat((Long)resp.rows()[0][0], is(0L));

        executor.exec("set global transient stats.enabled = true, stats.operations_log_size=10");
        waitNoPendingTasksOnAll();

        executor.exec(
            "select count(*), race from characters group by race order by count(*) desc limit 2");
        resp = executor.exec("select * from sys.operations_log");

        List<String> names = new ArrayList<>();
        for (Object[] objects : resp.rows()) {
            names.add((String)objects[2]);
        }
        assertTrue((names.contains("distributing collect") && names.contains("distributed merge")) || names.contains("collect"));
        assertTrue(names.contains("localMerge"));

        executor.exec("reset global stats.enabled, stats.operations_log_size");
        waitNoPendingTasksOnAll();
        resp = executor.exec("select count(*) from sys.operations_log");
        assertThat((Long) resp.rows()[0][0], is(0L));
    }

    @Test
    public void testEmptyJobsInLog() throws Exception {
        executor.exec("set global transient stats.enabled = true");
        executor.exec("insert into characters (name) values ('sysjobstest')");
        executor.exec("delete from characters where name = 'sysjobstest'");

        SQLResponse response = executor.exec(
                "select * from sys.jobs_log where stmt like 'insert into%' or stmt like 'delete%'");
        assertThat(response.rowCount(), is(2L));
        executor.exec("reset global stats.enabled");
    }

    @Test
    public void testSelectFromJobsLogWithLimit() throws Exception {
        // this is an regression test to verify that the CollectionTerminatedException is handled correctly
        executor.exec("set global transient stats.enabled = true");
        executor.exec("select * from sys.jobs");
        executor.exec("select * from sys.jobs");
        executor.exec("select * from sys.jobs");
        executor.exec("select * from sys.jobs_log limit 1");
        executor.exec("reset global stats.enabled");
    }

    @Test
    public void testDistinctSysOperations() throws Exception {
        // this tests a distributing collect without shards but DOC level granularity
        SQLResponse response = executor.exec("select distinct name  from sys.operations");
        // no data since stats.enabled is disabled
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testAddPrimaryKeyColumnToNonEmptyTable() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Cannot add a primary key column to a table that isn't empty");
        executor.exec("alter table characters add newpkcol string primary key");
    }

    @Test
    public void testIsNullOnObjects() throws Exception {
        SQLResponse resp = executor.exec("select name from characters where details is null order by name");
        assertThat(resp.rowCount(), is(5L));
        List<String> names = new ArrayList<>(5);
        for (Object[] objects : resp.rows()) {
            names.add((String) objects[0]);
        }
        assertThat(names, Matchers.contains("Anjie", "Ford Perfect", "Jeltz" ,"Kwaltz", "Marving"));

        resp = executor.exec("select count(*) from characters where details is not null");
        assertThat((Long)resp.rows()[0][0], is(2L));
    }

    @Test
    public void testDistanceQueryOnSysTable() throws Exception {
        SQLResponse response = executor.exec(
                "select Distance('POINT (10 20)', 'POINT (11 21)') from sys.cluster");
        assertThat((Double) response.rows()[0][0], is(152462.70754934277));
    }

    @Test
    public void testCreateTableWithInvalidAnalyzer() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Analyzer [foobar] not found for field [content]");
        executor.exec("create table t (content string index using fulltext with (analyzer='foobar'))");
    }

    @Test
    public void testSysNodesVersionFromMultipleNodes() throws Exception {
        SQLResponse response = executor.exec("select version, version['number'], " +
                "version['build_hash'], version['build_snapshot'] " +
                "from sys.nodes");
        assertThat(response.rowCount(), is(2L));
        for (int i = 0; i <=1 ; i++) {
            assertThat(response.rows()[i][0], instanceOf(Map.class));
            assertThat((Map<String, Object>) response.rows()[i][0], allOf(hasKey("number"), hasKey("build_hash"), hasKey("build_snapshot")));
            assertThat((String) response.rows()[i][1], Matchers.is(Version.CURRENT.number()));
            assertThat((String)response.rows()[i][2], is(Build.CURRENT.hash()));
            assertThat((Boolean) response.rows()[i][3], is(Version.CURRENT.snapshot()));
        }
    }
}
