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

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.protocols.postgres.PGErrorStatus.UNDEFINED_TABLE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

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
import java.util.function.UnaryOperator;

import org.apache.lucene.util.Constants;
import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.IntegTestCase;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.crate.execution.engine.collect.stats.JobsLogService;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.testing.Asserts;
import io.crate.testing.SQLResponse;
import io.crate.testing.SQLTransportExecutor;
import io.crate.testing.UseJdbc;

@IntegTestCase.ClusterScope(numClientNodes = 0, numDataNodes = 2, supportsDedicatedMasters = false)
public class TransportSQLActionClassLifecycleTest extends IntegTestCase {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @After
    public void resetSettings() throws Exception {
        // reset stats settings in case of some tests changed it and failed without resetting.
        execute("reset global stats.enabled, stats.jobs_log_size, stats.operations_log_size");
    }

    @Test
    public void testSelectNonExistentGlobalExpression() throws Exception {
        new Setup(sqlExecutor).groupBySetup();
        Asserts.assertSQLError(() -> execute("select count(race), suess.cluster.name from characters"))
            .hasPGError(UNDEFINED_TABLE)
            .hasHTTPError(NOT_FOUND, 4041)
            .hasMessageContaining("Relation 'suess.cluster' unknown");
    }

    @Test
    public void testSelectDoc() throws Exception {
        new Setup(sqlExecutor).groupBySetup();
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
        new Setup(sqlExecutor).groupBySetup();
        SQLResponse response = execute("select _raw from characters order by name desc limit 1");

        var schemas = cluster().getDataNodeInstance(NodeContext.class).schemas();
        DocTableInfo table = schemas.getTableInfo(new RelationName(sqlExecutor.getCurrentSchema(), "characters"));
        UnaryOperator<String> mapName = s -> {
            var ref = table.getReference(ColumnIdent.fromPath(s));
            return ref.storageIdent();
        };

        Object raw = response.rows()[0][0];
        Map<String, Object> rawMap = JsonXContent.JSON_XCONTENT.createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, (String) raw).map();

        assertThat(rawMap.get(mapName.apply("race"))).isEqualTo("Human");
        assertThat(rawMap.get(mapName.apply("gender"))).isEqualTo("female");
        assertThat(rawMap.get(mapName.apply("age"))).isEqualTo(32);
        assertThat(rawMap.get(mapName.apply("name"))).isEqualTo("Trillian");
    }

    @Test
    public void testSelectRawWithGrouping() throws Exception {
        new Setup(sqlExecutor).groupBySetup();
        SQLResponse response = execute("select name, _raw from characters " +
                                       "group by _raw, name order by name desc limit 1");

        var schemas = cluster().getDataNodeInstance(NodeContext.class).schemas();
        DocTableInfo table = schemas.getTableInfo(new RelationName(sqlExecutor.getCurrentSchema(), "characters"));
        UnaryOperator<String> mapName = s -> {
            var ref = table.getReference(ColumnIdent.fromPath(s));
            return ref.storageIdent();
        };

        Object raw = response.rows()[0][1];
        Map<String, Object> rawMap = JsonXContent.JSON_XCONTENT.createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, (String) raw).map();

        assertThat(rawMap.get(mapName.apply("race"))).isEqualTo("Human");
        assertThat(rawMap.get(mapName.apply("gender"))).isEqualTo("female");
        assertThat(rawMap.get(mapName.apply("age"))).isEqualTo(32);
        assertThat(rawMap.get(mapName.apply("name"))).isEqualTo("Trillian");
    }

    @Test
    public void testGlobalAggregateSimple() throws Exception {
        new Setup(sqlExecutor).groupBySetup();
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
        assertEquals(221L, response.rows()[0][0]);
    }

    @Test
    public void testGlobalAggregateWithoutNulls() throws Exception {
        new Setup(sqlExecutor).groupBySetup();
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
        new Setup(sqlExecutor).groupBySetup();
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
        new Setup(sqlExecutor).groupBySetup();
        SQLResponse response = execute("select sum(age), min(age), max(age), avg(age) from characters");
        assertEquals(1, response.rowCount());
        assertEquals(221L, response.rows()[0][0]);
        assertEquals(32, response.rows()[0][1]);
        assertEquals(112, response.rows()[0][2]);
        assertEquals(55.25d, response.rows()[0][3]);
    }

    @Test
    public void selectMultiGetRequestFromNonExistentTable() throws Exception {
        Asserts.assertSQLError(() -> execute("SELECT * FROM \"non_existent\" WHERE \"_id\" in (?,?)", new Object[]{"1", "2"}))
            .hasPGError(UNDEFINED_TABLE)
            .hasHTTPError(NOT_FOUND, 4041)
            .hasMessageContaining("Relation 'non_existent' unknown");
    }

    @Test
    public void testGroupByNestedObject() throws Exception {
        new Setup(sqlExecutor).groupBySetup();
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
        new Setup(sqlExecutor).groupBySetup();
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
        new Setup(sqlExecutor).groupBySetup();
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
        new Setup(sqlExecutor).groupBySetup();
        SQLResponse response = execute("select count(*), race from characters group by race", new Object[]{null});
        assertEquals(3, response.rowCount());
    }

    @Test
    public void testGroupByAndOrderByAlias() throws Exception {
        new Setup(sqlExecutor).groupBySetup();
        SQLResponse response = execute(
            "select characters.race as test_race from characters group by characters.race order by characters.race");
        assertEquals(3, response.rowCount());

        response = execute(
            "select characters.race as test_race from characters group by characters.race order by test_race");
        assertEquals(3, response.rowCount());
    }

    @Test
    public void testCountWithGroupByWithWhereClause() throws Exception {
        new Setup(sqlExecutor).groupBySetup();
        SQLResponse response = execute(
            "select count(*), race from characters where race = 'Human' group by race");
        assertEquals(1, response.rowCount());
    }

    @Test
    public void testCountWithGroupByOrderOnAggAscFuncAndLimit() throws Exception {
        new Setup(sqlExecutor).groupBySetup();
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
        new Setup(sqlExecutor).groupBySetup();
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
        new Setup(sqlExecutor).groupBySetup();
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
        new Setup(sqlExecutor).groupBySetup();
        SQLResponse response = execute("select count(*), gender, race from characters " +
                                       "group by race, gender order by count(*) desc, race asc limit 2 offset 20");

        assertEquals(0, response.rows().length);
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testCountWithGroupByOrderOnAggDescFuncAndLimit() throws Exception {
        new Setup(sqlExecutor).groupBySetup();
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
        new Setup(sqlExecutor).groupBySetup();
        SQLResponse response = execute("select * from characters where birthdate > '1970-01-01'");
        assertThat(response.rowCount(), Matchers.is(2L));
    }

    @Test
    public void testCopyToDirectoryOnPartitionedTableWithPartitionClause() throws Exception {
        new Setup(sqlExecutor).partitionTableSetup();
        String uriTemplate = Paths.get(folder.getRoot().toURI()).toUri().toString();
        SQLResponse response = execute("copy parted partition (date='2014-01-01') to DIRECTORY ?", $(uriTemplate));
        assertThat(response.rowCount()).isEqualTo(2L);

        List<String> lines = new ArrayList<>(2);
        DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(folder.getRoot().toURI()), "*.json");
        for (Path entry : stream) {
            lines.addAll(Files.readAllLines(entry, StandardCharsets.UTF_8));
        }
        assertThat(lines).hasSize(2);
        for (String line : lines) {
            assertThat(line.contains("2") || line.contains("1")).isTrue();
            assertThat(line.contains("1388534400000")).isFalse();  // date column not included in export
            assertThat(line, startsWith("{"));
            assertThat(line, endsWith("}"));
        }
    }

    @Test
    public void testCopyToDirectoryOnPartitionedTableWithoutPartitionClause() throws Exception {
        new Setup(sqlExecutor).partitionTableSetup();
        String uriTemplate = Paths.get(folder.getRoot().toURI()).toUri().toString();
        SQLResponse response = execute("copy parted to DIRECTORY ?", $(uriTemplate));
        assertThat(response.rowCount()).isEqualTo(4L);

        List<String> lines = new ArrayList<>(4);
        DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(folder.getRoot().toURI()), "*.json");
        for (Path entry : stream) {
            lines.addAll(Files.readAllLines(entry, StandardCharsets.UTF_8));
        }
        assertThat(lines).hasSize(4);
        for (String line : lines) {
            // date column included in output
            if (!line.contains("1388534400000")) {
                assertThat(line.contains("1391212800000")).isTrue();
            }
            assertThat(line, startsWith("{"));
            assertThat(line, endsWith("}"));
        }
    }

    @Test
    public void testArithmeticFunctions() throws Exception {
        execute("select ((2 * 4 - 2 + 1) / 2) % 3 from sys.cluster");
        assertThat(response.cols()[0]).isEqualTo("0");
        assertThat(response.rows()[0][0]).isEqualTo(0);

        execute("select ((2 * 4.0 - 2 + 1) / 2) % 3 from sys.cluster");
        assertThat(response.rows()[0][0]).isEqualTo(0.5);

        execute("select ? + 2 from sys.cluster", $(1));
        assertThat(response.rows()[0][0]).isEqualTo(3);

        if (!Constants.WINDOWS) {
            response = execute("select load['1'] + load['5'], load['1'], load['5'] from sys.nodes limit 1");
            assertEquals(response.rows()[0][0], (Double) response.rows()[0][1] + (Double) response.rows()[0][2]);
        }
    }

    @Test
    public void testSetMultipleStatement() throws Exception {
        SQLResponse response = execute(
            "select settings['stats']['operations_log_size'], settings['stats']['enabled'] from sys.cluster");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat((Integer) response.rows()[0][0]).isEqualTo(JobsLogService.STATS_OPERATIONS_LOG_SIZE_SETTING.getDefault(Settings.EMPTY));
        assertThat((Boolean) response.rows()[0][1]).isEqualTo(JobsLogService.STATS_ENABLED_SETTING.getDefault(Settings.EMPTY));

        response = execute("set global persistent stats.operations_log_size=1024, stats.enabled=false");
        assertThat(response.rowCount()).isEqualTo(1L);

        response = execute(
            "select settings['stats']['operations_log_size'], settings['stats']['enabled'] from sys.cluster");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat((Integer) response.rows()[0][0]).isEqualTo(1024);
        assertThat((Boolean) response.rows()[0][1]).isFalse();

        response = execute("reset global stats.operations_log_size, stats.enabled");
        assertThat(response.rowCount()).isEqualTo(1L);
        waitNoPendingTasksOnAll();

        response = execute(
            "select settings['stats']['operations_log_size'], settings['stats']['enabled'] from sys.cluster");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat((Integer) response.rows()[0][0]).isEqualTo(JobsLogService.STATS_OPERATIONS_LOG_SIZE_SETTING.getDefault(Settings.EMPTY));
        assertThat((Boolean) response.rows()[0][1]).isEqualTo(JobsLogService.STATS_ENABLED_SETTING.getDefault(Settings.EMPTY));
    }

    @Test
    public void testSetStatementInvalid() throws Exception {
        Asserts.assertSQLError(() -> execute("set global persistent stats.operations_log_size=-1024"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("Failed to parse value [-1024] for setting [stats.operations_log_size] must be >= 0");

        SQLResponse response = execute("select settings['stats']['operations_log_size'] from sys.cluster");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat((Integer) response.rows()[0][0]).isEqualTo(JobsLogService.STATS_OPERATIONS_LOG_SIZE_SETTING.getDefault(Settings.EMPTY));
    }

    @Test
    public void testSysOperationsLog() throws Exception {
        new Setup(sqlExecutor).groupBySetup();
        execute("set global transient stats.enabled = false");

        execute(
            "select count(*), race from characters group by race order by count(*) desc limit 2");
        SQLResponse resp = execute("select count(*) from sys.operations_log");
        assertThat((Long) resp.rows()[0][0]).isEqualTo(0L);

        execute("set global transient stats.enabled = true, stats.operations_log_size=10");
        waitNoPendingTasksOnAll();

        execute(
            "select count(*), race from characters group by race order by count(*) desc limit 2");

        assertBusy(() -> {
            SQLResponse response = execute("select * from sys.operations_log order by ended limit 3");

            List<String> names = new ArrayList<>();
            for (Object[] objects : response.rows()) {
                names.add((String) objects[4]);
            }
            assertThat(names, Matchers.anyOf(
                Matchers.hasItems("distributing collect", "distributing collect"),
                Matchers.hasItems("collect", "localMerge"),

                // the select * from sys.operations_log has 2 collect operations (1 per node)
                Matchers.hasItems("collect", "collect"),
                Matchers.hasItems("distributed merge", "localMerge")));
        }, 10L, TimeUnit.SECONDS);

        execute("set global transient stats.enabled = false");
        waitNoPendingTasksOnAll();
        resp = execute("select count(*) from sys.operations_log");
        assertThat((Long) resp.rows()[0][0]).isEqualTo(0L);
    }

    @Test
    public void testSysOperationsLogConcurrentAccess() throws Exception {
        new Setup(sqlExecutor).groupBySetup();
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
        execute("select * from sys.jobs");
        execute("select * from sys.jobs");
        execute("select * from sys.jobs");
        execute("select * from sys.jobs_log limit 1");
    }

    @Test
    public void testAddPrimaryKeyColumnToNonEmptyTable() throws Exception {
        new Setup(sqlExecutor).groupBySetup();
        Asserts.assertSQLError(() -> execute("alter table characters add newpkcol string primary key"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4004)
            .hasMessageContaining("Cannot add a primary key column to a table that isn't empty");
    }

    @Test
    public void testIsNullOnObjects() throws Exception {
        new Setup(sqlExecutor).groupBySetup();
        SQLResponse resp = execute("select name from characters where details is null order by name");
        assertThat(resp.rowCount()).isEqualTo(5L);
        List<String> names = new ArrayList<>(5);
        for (Object[] objects : resp.rows()) {
            names.add((String) objects[0]);
        }
        assertThat(names, Matchers.contains("Anjie", "Ford Perfect", "Jeltz", "Kwaltz", "Marving"));

        resp = execute("select count(*) from characters where details is not null");
        assertThat((Long) resp.rows()[0][0]).isEqualTo(2L);
    }

    @Test
    public void testDistanceQueryOnSysTable() throws Exception {
        SQLResponse response = execute(
            "select Distance('POINT (10 20)', 'POINT (11 21)') from sys.cluster");
        assertThat(response.rows()[0][0]).isEqualTo(152354.3209044634);
    }

    @Test
    public void testCreateTableWithInvalidAnalyzer() throws Exception {
        Asserts.assertSQLError(() -> execute("create table t (content string index using fulltext with (analyzer='foobar'))"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4003)
            .hasMessageContaining("Failed to parse mapping: analyzer [foobar] not found for field [content]");
    }

    @Test
    public void testSysNodesVersionFromMultipleNodes() throws Exception {
        SQLResponse response = execute("select version, version['number'], " +
                                       "version['build_hash'], version['build_snapshot'] " +
                                       "from sys.nodes");
        assertThat(response.rowCount()).isEqualTo(2L);
        for (int i = 0; i <= 1; i++) {
            assertThat(response.rows()[i][0]).isInstanceOf(Map.class);
            assertThat((Map<String, Object>) response.rows()[i][0], allOf(hasKey("number"), hasKey("build_hash"), hasKey("build_snapshot")));
            assertThat((String) response.rows()[i][1], Matchers.is(Version.CURRENT.externalNumber()));
            assertThat((String) response.rows()[i][2]).isEqualTo(Build.CURRENT.hash());
            assertThat((Boolean) response.rows()[i][3]).isEqualTo(Version.CURRENT.isSnapshot());
        }
    }

    @Test
    public void selectCurrentTimestamp() throws Exception {
        long before = System.currentTimeMillis();
        SQLResponse response = execute("select current_timestamp(3) from sys.cluster");
        long after = System.currentTimeMillis();
        assertThat(response.cols(), arrayContaining("current_timestamp(3)"));
        assertThat((long) response.rows()[0][0], allOf(greaterThanOrEqualTo(before), lessThanOrEqualTo(after)));
    }

    @Test
    public void selectWhereEqualCurrentTimestamp() throws Exception {
        SQLResponse response = execute("select * from sys.cluster where current_timestamp = current_timestamp");
        assertThat(response.rowCount()).isEqualTo(1L);

        SQLResponse newResponse = execute("select * from sys.cluster where current_timestamp > current_timestamp");
        assertThat(newResponse.rowCount()).isEqualTo(0L);
    }
}
