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
import io.crate.operation.collect.stats.JobsLogService;
import io.crate.testing.SQLResponse;
import io.crate.testing.SQLTransportExecutor;
import io.crate.testing.TestingHelpers;
import io.crate.testing.UseJdbc;
import org.apache.lucene.util.Constants;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;
import org.joda.time.DateTimeUtils;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(numClientNodes = 0, numDataNodes = 2, supportsDedicatedMasters = false)
@UseJdbc
public class TransportSQLActionClassLifecycleTest extends SQLTransportIntegrationTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @BeforeClass
    public static void setTimeForTesting() throws Exception {
        // A monotonically increasing system clock that prevents from clock adjustments (e.g. via NTP)
        // and avoids synchronization.
        DateTimeUtils.setCurrentMillisProvider(new DateTimeUtils.MillisProvider() {
            private final AtomicLong lastTime = new AtomicLong();

            @Override
            public long getMillis() {
                long now = System.currentTimeMillis();
                long time = lastTime.incrementAndGet();
                if (now >= time) {
                    lastTime.compareAndSet(time, now);
                    return lastTime.getAndIncrement();
                }
                return time;
            }
        });
    }

    @Before
    public void initTestData() throws Exception {
        Setup setup = new Setup(sqlExecutor);
        setup.partitionTableSetup();
        setup.groupBySetup();
    }

    @After
    public void resetSettings() throws Exception {
        // reset stats settings in case of some tests changed it and failed without resetting.
        execute("reset global stats.enabled, stats.jobs_log_size, stats.operations_log_size");
    }

    @Test
    public void testSelectNonExistentGlobalExpression() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Cannot resolve relation 'suess.cluster'");
        execute("select count(race), suess.cluster.name from characters");
    }

    @Test
    public void testSelectDoc() throws Exception {
        SQLResponse response = execute("select _doc from characters order by name desc limit 1");
        assertArrayEquals(new String[]{"_doc"}, response.cols());
        Map<String, Object> _doc = new TreeMap<>((Map) response.rows()[0][0]);
        assertEquals(
            "{age=32, birthdate=276912000000, details={job=Mathematician}, " +
            "gender=female, name=Trillian, race=Human}",
            _doc.toString());
    }

    @Test
    public void testSelectRaw() throws Exception {
        SQLResponse response = execute("select _raw from characters order by name desc limit 1");
        assertEquals(
            "{\"race\":\"Human\",\"gender\":\"female\",\"age\":32,\"birthdate\":276912000000," +
            "\"name\":\"Trillian\",\"details\":{\"job\":\"Mathematician\"}}\n",
            TestingHelpers.printedTable(response.rows()));
    }

    @Test
    public void testSelectRawWithGrouping() throws Exception {
        SQLResponse response = execute("select name, _raw from characters " +
                                       "group by _raw, name order by name desc limit 1");
        assertEquals(
            "Trillian| {\"race\":\"Human\",\"gender\":\"female\",\"age\":32,\"birthdate\":276912000000," +
            "\"name\":\"Trillian\",\"details\":{\"job\":\"Mathematician\"}}\n",
            TestingHelpers.printedTable(response.rows()));
    }

    @Test
    public void testSelectOrderByNullSortingASC() throws Exception {
        execute("select age from characters order by age");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("32\n" +
               "34\n" +
               "43\n" +
               "112\n" +
               "NULL\n" +
               "NULL\n" +
               "NULL\n"));
    }

    @Test
    public void testSelectOrderByNullSortingDESC() throws Exception {
        execute("select age from characters order by age desc");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("NULL\n" +
               "NULL\n" +
               "NULL\n" +
               "112\n" +
               "43\n" +
               "34\n" +
               "32\n"));
    }

    @Test
    public void testSelectOrderByNullSortingASCWithFunction() throws Exception {
        execute("select abs(age) from characters order by 1 asc");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("32\n" +
               "34\n" +
               "43\n" +
               "112\n" +
               "NULL\n" +
               "NULL\n" +
               "NULL\n"));
    }

    @Test
    public void testSelectOrderByNullSortingDESCWithFunction() throws Exception {
        execute("select abs(age) from characters order by 1 desc");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("NULL\n" +
               "NULL\n" +
               "NULL\n" +
               "112\n" +
               "43\n" +
               "34\n" +
               "32\n"));
    }


    @Test
    public void testSelectGroupByOrderByNullSortingASC() throws Exception {
        execute("select age from characters group by age order by age");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("32\n" +
               "34\n" +
               "43\n" +
               "112\n" +
               "NULL\n"));
    }

    @Test
    public void testSelectGroupByOrderByNullSortingDESC() throws Exception {
        execute("select age from characters group by age order by age desc");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("NULL\n" +
               "112\n" +
               "43\n" +
               "34\n" +
               "32\n"));
    }

    @Test
    public void testGlobalAggregateSimple() throws Exception {
        SQLResponse response = execute("select max(age) from characters");

        assertEquals(1, response.rowCount());
        assertEquals("max(age)", response.cols()[0]);
        assertEquals(112, response.rows()[0][0]);

        response = execute("select min(name) from characters");

        assertEquals(1, response.rowCount());
        assertEquals("min(name)", response.cols()[0]);
        assertEquals("Anjie", response.rows()[0][0]);

        response = execute("select avg(age) as median_age from characters");
        assertEquals(1, response.rowCount());
        assertEquals("median_age", response.cols()[0]);
        assertEquals(55.25d, response.rows()[0][0]);

        response = execute("select sum(age) as sum_age from characters");
        assertEquals(1, response.rowCount());
        assertEquals("sum_age", response.cols()[0]);
        assertEquals(221.0d, response.rows()[0][0]);
    }

    @Test
    public void testGlobalAggregateWithoutNulls() throws Exception {
        SQLResponse firstResp = execute("select sum(age) from characters");
        SQLResponse secondResp = execute("select sum(age) from characters where age is not null");

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
        SQLResponse response = execute(
            "select sum(age), avg(age) from characters where characters.age > 112");
        assertEquals(1, response.rowCount());
        assertNull(response.rows()[0][0]);
        assertNull(response.rows()[0][1]);

        response = execute("select sum(age) from characters limit 0");
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testGlobalAggregateMany() throws Exception {
        SQLResponse response = execute("select sum(age), min(age), max(age), avg(age) from characters");
        assertEquals(1, response.rowCount());
        assertEquals(221.0d, response.rows()[0][0]);
        assertEquals(32, response.rows()[0][1]);
        assertEquals(112, response.rows()[0][2]);
        assertEquals(55.25d, response.rows()[0][3]);
    }

    @Test
    public void selectMultiGetRequestFromNonExistentTable() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("TableUnknownException: Table 'doc.non_existent' unknown");
        execute("SELECT * FROM \"non_existent\" WHERE \"_id\" in (?,?)", new Object[]{"1", "2"});
    }

    @Test
    public void testGroupByNestedObject() throws Exception {
        SQLResponse response = execute("select count(*), details['job'] from characters " +
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
        SQLResponse response = execute(
            "select count(*), race from characters group by race order by race desc limit 2");

        assertEquals(2L, response.rowCount());
        assertEquals(2L, response.rows()[0][0]);
        assertEquals("Vogon", response.rows()[0][1]);
        assertEquals(4L, response.rows()[1][0]);
        assertEquals("Human", response.rows()[1][1]);
    }

    @Test
    public void testCountWithGroupByOrderOnKeyAscAndLimit() throws Exception {
        SQLResponse response = execute(
            "select count(*), race from characters group by race order by race asc limit 2");

        assertEquals(2, response.rowCount());
        assertEquals(1L, response.rows()[0][0]);
        assertEquals("Android", response.rows()[0][1]);
        assertEquals(4L, response.rows()[1][0]);
        assertEquals("Human", response.rows()[1][1]);
    }

    @Test
    @UseJdbc(0) // NPE because of unused null parameter
    public void testCountWithGroupByNullArgs() throws Exception {
        SQLResponse response = execute("select count(*), race from characters group by race", new Object[]{null});
        assertEquals(3, response.rowCount());
    }

    @Test
    public void testGroupByAndOrderByAlias() throws Exception {
        SQLResponse response = execute(
            "select characters.race as test_race from characters group by characters.race order by characters.race");
        assertEquals(3, response.rowCount());

        response = execute(
            "select characters.race as test_race from characters group by characters.race order by test_race");
        assertEquals(3, response.rowCount());
    }

    @Test
    public void testCountWithGroupByWithWhereClause() throws Exception {
        SQLResponse response = execute(
            "select count(*), race from characters where race = 'Human' group by race");
        assertEquals(1, response.rowCount());
    }

    @Test
    public void testCountWithGroupByOrderOnAggAscFuncAndLimit() throws Exception {
        SQLResponse response = execute("select count(*), race from characters " +
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
        SQLResponse response = execute("select count(*), gender, race from characters " +
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
        SQLResponse response = execute("select count(*), gender, race from characters " +
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
        SQLResponse response = execute("select count(*), gender, race from characters " +
                                       "group by race, gender order by count(*) desc, race asc limit 2 offset 20");

        assertEquals(0, response.rows().length);
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testCountWithGroupByOrderOnAggDescFuncAndLimit() throws Exception {
        SQLResponse response = execute(
            "select count(*), race from characters group by race order by count(*) desc limit 2");

        assertEquals(2, response.rowCount());
        assertEquals(4L, response.rows()[0][0]);
        assertEquals("Human", response.rows()[0][1]);
        assertEquals(2L, response.rows()[1][0]);
        assertEquals("Vogon", response.rows()[1][1]);
    }

    @Test
    public void testDateRange() throws Exception {
        SQLResponse response = execute("select * from characters where birthdate > '1970-01-01'");
        assertThat(response.rowCount(), Matchers.is(2L));
    }

    @Test
    public void testOrderByNullsFirstAndLast() throws Exception {
        SQLResponse response = execute(
            "select details['job'] from characters order by details['job'] nulls first limit 1");
        assertNull(response.rows()[0][0]);

        response = execute(
            "select details['job'] from characters order by details['job'] desc nulls first limit 1");
        assertNull(response.rows()[0][0]);

        response = execute(
            "select details['job'] from characters order by details['job'] nulls last");
        assertNull(response.rows()[((Long) response.rowCount()).intValue() - 1][0]);

        response = execute(
            "select details['job'] from characters order by details['job'] desc nulls last");
        assertNull(response.rows()[((Long) response.rowCount()).intValue() - 1][0]);


        response = execute(
            "select distinct details['job'] from characters order by details['job'] desc nulls last");
        assertNull(response.rows()[((Long) response.rowCount()).intValue() - 1][0]);
    }

    @Test
    @UseJdbc(0) // copy has no rowcount
    public void testCopyToDirectoryOnPartitionedTableWithPartitionClause() throws Exception {
        String uriTemplate = Paths.get(folder.getRoot().toURI()).toUri().toString();
        SQLResponse response = execute("copy parted partition (date='2014-01-01') to DIRECTORY ?", $(uriTemplate));
        assertThat(response.rowCount(), is(2L));

        List<String> lines = new ArrayList<>(2);
        DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(folder.getRoot().toURI()), "*.json");
        for (Path entry : stream) {
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
    @UseJdbc(0) // COPY has no rowcount
    public void testCopyToDirectoryOnPartitionedTableWithoutPartitionClause() throws Exception {
        String uriTemplate = Paths.get(folder.getRoot().toURI()).toUri().toString();
        SQLResponse response = execute("copy parted to DIRECTORY ?", $(uriTemplate));
        assertThat(response.rowCount(), is(4L));

        List<String> lines = new ArrayList<>(4);
        DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(folder.getRoot().toURI()), "*.json");
        for (Path entry : stream) {
            lines.addAll(Files.readAllLines(entry, StandardCharsets.UTF_8));
        }
        assertThat(lines.size(), is(4));
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
        SQLResponse response = execute("select ((2 * 4 - 2 + 1) / 2) % 3 from sys.cluster");
        assertThat(response.cols()[0], is("(((((2 * 4) - 2) + 1) / 2) % 3)"));
        assertThat((Long) response.rows()[0][0], is(0L));

        response = execute("select ((2 * 4.0 - 2 + 1) / 2) % 3 from sys.cluster");
        assertThat((Double) response.rows()[0][0], is(0.5));

        response = execute("select ? + 2 from sys.cluster", $(1));
        assertThat((Long) response.rows()[0][0], is(3L));

        if (!Constants.WINDOWS) {
            response = execute("select load['1'] + load['5'], load['1'], load['5'] from sys.nodes limit 1");
            assertEquals(response.rows()[0][0], (Double) response.rows()[0][1] + (Double) response.rows()[0][2]);
        }
    }

    @Test
    @UseJdbc(0) // set has no rowcount
    public void testSetMultipleStatement() throws Exception {
        SQLResponse response = execute(
            "select settings['stats']['operations_log_size'], settings['stats']['enabled'] from sys.cluster");
        assertThat(response.rowCount(), is(1L));
        assertThat((Integer) response.rows()[0][0], is(JobsLogService.STATS_OPERATIONS_LOG_SIZE_SETTING.getDefault()));
        assertThat((Boolean) response.rows()[0][1], is(JobsLogService.STATS_ENABLED_SETTING.getDefault()));

        response = execute("set global persistent stats.operations_log_size=1024, stats.enabled=false");
        assertThat(response.rowCount(), is(1L));

        response = execute(
            "select settings['stats']['operations_log_size'], settings['stats']['enabled'] from sys.cluster");
        assertThat(response.rowCount(), is(1L));
        assertThat((Integer) response.rows()[0][0], is(1024));
        assertThat((Boolean) response.rows()[0][1], is(false));

        response = execute("reset global stats.operations_log_size, stats.enabled");
        assertThat(response.rowCount(), is(1L));
        waitNoPendingTasksOnAll();

        response = execute(
            "select settings['stats']['operations_log_size'], settings['stats']['enabled'] from sys.cluster");
        assertThat(response.rowCount(), is(1L));
        assertThat((Integer) response.rows()[0][0], is(JobsLogService.STATS_OPERATIONS_LOG_SIZE_SETTING.getDefault()));
        assertThat((Boolean) response.rows()[0][1], is(JobsLogService.STATS_ENABLED_SETTING.getDefault()));
    }

    @Test
    public void testSetStatementInvalid() throws Exception {
        try {
            execute("set global persistent stats.operations_log_size=-1024");
            fail("expected SQLActionException, none was thrown");
        } catch (SQLActionException e) {
            assertThat(e.getMessage(), containsString("Failed to parse value [-1024] for setting [stats.operations_log_size] must be >= 0"));

            SQLResponse response = execute("select settings['stats']['operations_log_size'] from sys.cluster");
            assertThat(response.rowCount(), is(1L));
            assertThat((Integer) response.rows()[0][0], is(JobsLogService.STATS_OPERATIONS_LOG_SIZE_SETTING.getDefault()));
        }
    }

    @Test
    public void testSysOperationsLog() throws Exception {
        execute(
            "select count(*), race from characters group by race order by count(*) desc limit 2");
        SQLResponse resp = execute("select count(*) from sys.operations_log");
        assertThat((Long) resp.rows()[0][0], is(0L));

        execute("set global transient stats.enabled = true, stats.operations_log_size=10");
        waitNoPendingTasksOnAll();

        execute(
            "select count(*), race from characters group by race order by count(*) desc limit 2");

        assertBusy(new Runnable() {
            @Override
            public void run() {
                SQLResponse resp = execute("select * from sys.operations_log order by ended limit 3");

                List<String> names = new ArrayList<>();
                for (Object[] objects : resp.rows()) {
                    names.add((String) objects[4]);
                }
                assertThat(names, Matchers.anyOf(
                    Matchers.hasItems("distributing collect", "distributing collect"),
                    Matchers.hasItems("collect", "localMerge"),

                    // the select * from sys.operations_log has 2 collect operations (1 per node)
                    Matchers.hasItems("collect", "collect"),
                    Matchers.hasItems("distributed merge", "localMerge")));
            }
        }, 10L, TimeUnit.SECONDS);

        execute("reset global stats.enabled, stats.operations_log_size");
        waitNoPendingTasksOnAll();
        resp = execute("select count(*) from sys.operations_log");
        assertThat((Long) resp.rows()[0][0], is(0L));
    }

    @Test
    public void testSysOperationsLogConcurrentAccess() throws Exception {
        execute("set global transient stats.enabled = true, stats.operations_log_size=10");
        waitNoPendingTasksOnAll();

        Thread selectThread = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 50; i++) {
                    execute("select count(*), race from characters group by race order by count(*) desc limit 2");
                }
            }
        });

        Thread sysOperationThread = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 50; i++) {
                    execute("select * from sys.operations_log order by ended");
                }
            }
        });
        selectThread.start();
        sysOperationThread.start();
        selectThread.join(SQLTransportExecutor.REQUEST_TIMEOUT.millis());
        sysOperationThread.join(SQLTransportExecutor.REQUEST_TIMEOUT.millis());
    }

    @Test
    public void testSelectFromJobsLogWithLimit() throws Exception {
        // this is an regression test to verify that the CollectionTerminatedException is handled correctly
        execute("set global transient stats.enabled = true");
        execute("select * from sys.jobs");
        execute("select * from sys.jobs");
        execute("select * from sys.jobs");
        execute("select * from sys.jobs_log limit 1");
    }

    @Test
    public void testAddPrimaryKeyColumnToNonEmptyTable() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Cannot add a primary key column to a table that isn't empty");
        execute("alter table characters add newpkcol string primary key");
    }

    @Test
    public void testIsNullOnObjects() throws Exception {
        SQLResponse resp = execute("select name from characters where details is null order by name");
        assertThat(resp.rowCount(), is(5L));
        List<String> names = new ArrayList<>(5);
        for (Object[] objects : resp.rows()) {
            names.add((String) objects[0]);
        }
        assertThat(names, Matchers.contains("Anjie", "Ford Perfect", "Jeltz", "Kwaltz", "Marving"));

        resp = execute("select count(*) from characters where details is not null");
        assertThat((Long) resp.rows()[0][0], is(2L));
    }

    @Test
    public void testDistanceQueryOnSysTable() throws Exception {
        SQLResponse response = execute(
            "select Distance('POINT (10 20)', 'POINT (11 21)') from sys.cluster");
        assertThat(response.rows()[0][0], is(152354.3209044634));
    }

    @Test
    public void testCreateTableWithInvalidAnalyzer() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("analyzer [foobar] not found for field [content]");
        execute("create table t (content string index using fulltext with (analyzer='foobar'))");
    }

    @Test
    public void testSysNodesVersionFromMultipleNodes() throws Exception {
        SQLResponse response = execute("select version, version['number'], " +
                                       "version['build_hash'], version['build_snapshot'] " +
                                       "from sys.nodes");
        assertThat(response.rowCount(), is(2L));
        for (int i = 0; i <= 1; i++) {
            assertThat(response.rows()[i][0], instanceOf(Map.class));
            assertThat((Map<String, Object>) response.rows()[i][0], allOf(hasKey("number"), hasKey("build_hash"), hasKey("build_snapshot")));
            assertThat((String) response.rows()[i][1], Matchers.is(Version.CURRENT.number()));
            assertThat((String) response.rows()[i][2], is(Build.CURRENT.hash()));
            assertThat((Boolean) response.rows()[i][3], is(Version.CURRENT.snapshot()));
        }
    }

    @Test
    public void selectCurrentTimestamp() throws Exception {
        long before = DateTimeUtils.currentTimeMillis();
        SQLResponse response = execute("select current_timestamp from sys.cluster");
        long after = DateTimeUtils.currentTimeMillis();
        assertThat(response.cols(), arrayContaining("current_timestamp"));
        assertThat((long) response.rows()[0][0], allOf(greaterThanOrEqualTo(before), lessThanOrEqualTo(after)));
    }

    @Test
    public void selectWhereEqualCurrentTimestamp() throws Exception {
        SQLResponse response = execute("select * from sys.cluster where current_timestamp = current_timestamp");
        assertThat(response.rowCount(), is(1L));

        SQLResponse newResponse = execute("select * from sys.cluster where current_timestamp > current_timestamp");
        assertThat(newResponse.rowCount(), is(0L));
    }
}
