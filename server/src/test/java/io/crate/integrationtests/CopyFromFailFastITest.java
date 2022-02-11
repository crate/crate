/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

import static com.carrotsearch.randomizedtesting.RandomizedTest.newTempDir;
import static io.crate.integrationtests.CopyIntegrationTest.tmpFileWithLines;
import static io.crate.testing.Asserts.assertThrowsMatches;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.Is.is;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.carrotsearch.randomizedtesting.LifecycleScope;

import io.crate.action.sql.SQLOperations;
import io.crate.exceptions.JobKilledException;
import io.crate.testing.SQLTransportExecutor;
import io.crate.testing.UseJdbc;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0, supportsDedicatedMasters = false)
public class CopyFromFailFastITest extends SQLIntegrationTestCase {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @After
    public void resetSettings() {
        execute("reset global " +
                "overload_protection.dml.initial_concurrency, " +
                "overload_protection.dml.max_concurrency," +
                "overload_protection.dml.queue_size");
    }

    @UseJdbc(0)
    @Test
    public void test_copy_from_with_fail_fast_with_single_node() throws Exception {
        internalCluster().startNode();
        internalCluster().ensureAtLeastNumDataNodes(1);

        // a single uri with 'shared = true' implies that one node will be involved at all times.
        Path tmpDir = newTempDir(LifecycleScope.TEST);
        Path target = Files.createDirectories(tmpDir.resolve("target"));
        tmpFileWithLines(Arrays.asList(("{\"a\":987654321},".repeat(10) + "{\"a\":\"fail here\"}," +
                                        "{\"a\":123456789},".repeat(10)).split(",")),
                         "data1.json",
                         target);

        execute("CREATE TABLE t (a int) CLUSTERED INTO 1 SHARDS");

        assertThrowsMatches(
            () -> execute(
                "COPY t FROM ? WITH (bulk_size = 1, fail_fast = true)", // fail_fast = true
                new Object[]{target.toUri() + "*"}),
            JobKilledException.class,
            "ERRORS: {failed to parse field"
        );
    }

    @TestLogging("io.crate.execution.dml.upsert:DEBUG")
    @Test
    public void test_copy_from_with_fail_fast_with_write_error_on_non_handler_node() throws Exception {
        internalCluster().startNode();
        internalCluster().startNode();
        internalCluster().ensureAtLeastNumDataNodes(2);

        execute("set global overload_protection.dml.initial_concurrency = 2");
        execute("set global overload_protection.dml.max_concurrency = 2");
        execute("set global overload_protection.dml.queue_size = 2");

        execute("CREATE TABLE doc.t (a INT PRIMARY KEY, b INT) CLUSTERED INTO 2 SHARDS WITH (number_of_replicas=0)");

        execute("SELECT node['name'] FROM sys.shards WHERE table_name='t' ORDER BY id");
        var nodeNameOfShard0 = (String) response.rows()[0][0];
        var nodeNameOfShard1 = (String) response.rows()[1][0];

        var clientProvider = new SQLTransportExecutor.ClientProvider() {

            @Override
            public Client client() {
                return internalCluster().client(nodeNameOfShard0);
            }

            @Override
            public String pgUrl() {
                return null;
            }

            @Override
            public SQLOperations sqlOperations() {
                return internalCluster().getInstance(SQLOperations.class, nodeNameOfShard0);
            }
        };
        var handlerNodeExecutor = new SQLTransportExecutor(clientProvider);

        Path tmpDir = newTempDir(LifecycleScope.TEST);
        Path target = Files.createDirectories(tmpDir.resolve("target"));
        int numDocs = 100;

        var indexMetadata = clusterService().state().getMetadata().index("t");
        List<String> rows = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            String line;
            // Ensure the failing doc will fail on shard id 1
            if (i >= 20 && OperationRouting.generateShardId(indexMetadata, Integer.toString(i), null) == 1) {
                line = "{\"a\":" + i + ", \"b\":\"fail here\"}";
            } else {
                line = "{\"a\":" + i + ", \"b\":" + i + "}";
            }
            rows.add(line);
        }

        tmpFileWithLines(rows, "data1.json", target);
        assertExpectedLogMessages(
            () -> assertThrowsMatches(
                () -> handlerNodeExecutor.exec(
                    "COPY doc.t FROM ? WITH (bulk_size = 1, fail_fast = true, shared= true)", // fail_fast = true
                    new Object[]{target.toUri() + "*"}),
                JobKilledException.class,
                "failed to parse field [b] of type [integer]"
            ),
            new MockLogAppender.PatternSeenEventExcpectation(
                "assert failure on node=" + nodeNameOfShard1,
                "io.crate.execution.dml.upsert.TransportShardUpsertAction",
                Level.DEBUG,
                "Failed to execute upsert on nodeName="+nodeNameOfShard1 + ".*")
        );

        execute("SELECT COUNT(*) FROM doc.t");
        assertThat((long) response.rows()[0][0], lessThan(50L));
    }

    @Test
    public void test_copy_from_fail_fast_with_passing_uris_only() throws IOException {

        Path tmpDir = newTempDir(LifecycleScope.TEST);
        Path target = Files.createDirectories(tmpDir.resolve("target"));
        tmpFileWithLines(Collections.singletonList(("{\"a\":123}")),
                         "passing1.json",
                         target);
        tmpFileWithLines(Arrays.asList(("{\"a\":123},".repeat(2).split(","))),
                         "passing2.json",
                         target);
        tmpFileWithLines(Arrays.asList(("{\"a\":123},".repeat(3).split(","))),
                         "passing3.json",
                         target);

        execute("CREATE TABLE t (a int)");

        execute("COPY t FROM ? WITH (fail_fast = true, shared = false)", new Object[]{target.toUri().toString() + "*"});
        refresh();
        execute("select * from t");
        assertThat(response.rowCount(), is(6L * cluster().numDataNodes()));

        execute("COPY t FROM ? WITH (fail_fast = true, shared = false) return summary", new Object[]{target.toUri().toString() + "*"});
        refresh();
        execute("select * from t");
        assertThat(response.rowCount(), is(6L * cluster().numDataNodes() * 2));
    }

    @Test
    public void test_copy_from_fail_fast_with_passing_and_failing_uris() throws IOException {

        Path tmpDir = newTempDir(LifecycleScope.TEST);
        Path target = Files.createDirectories(tmpDir.resolve("target"));
        tmpFileWithLines(Collections.singletonList(("{\"a\":123}")),
                         "passing1.json",
                         target);
        tmpFileWithLines(Arrays.asList(("{\"a\":123},".repeat(2).split(","))),
                         "passing2.json",
                         target);
        tmpFileWithLines(Arrays.asList(("{\"a\":123},".repeat(3).split(","))),
                         "passing3.json",
                         target);
        tmpFileWithLines(Arrays.asList(("{\"a\":987654321},".repeat(2) + "{\"a\":\"fail here\"}," +
                                        "{\"a\":123456789},".repeat(3)).split(",")),
                         "failing1.json",
                         target);
        tmpFileWithLines(Arrays.asList(("{\"a\":987654321},".repeat(4) + "{\"b\":\"fail here\"}," +
                                        "{\"a\":123456789},".repeat(5)).split(",")),
                         "failing2.json",
                         target);

        String expectedMessage = "[URI: failing1.json, ERRORS: {failed to parse field [a] of type [integer]={count=1, line_numbers=[3]}}]" +
                                 "[URI: failing2.json, ERRORS: {mapping set to strict, dynamic introduction of [b] within [default] is not allowed={count=1, line_numbers=[5]}}]" +
                                 "[URI: passing1.json, ERRORS: {}][URI: passing2.json, ERRORS: {}][URI: passing3.json, ERRORS: {}]";

        execute("CREATE TABLE t (a int)");

        try {
            execute("COPY t FROM ? WITH (fail_fast = true, shared = false)",
                    new Object[]{target.toUri().toString() + "*"});
        } catch (Exception e) {
            String exceptionMessage = e.getMessage();
            if (e.getMessage() == null || e.getMessage().isEmpty()) {
                fail("expect a message from this exception");
            }
            exceptionMessage = exceptionMessage.replaceAll("file:///.*/target/", "").replaceAll("],","]");
            exceptionMessage = Arrays.stream(exceptionMessage.split("\n")).filter(s -> s.startsWith("[URI:")).sorted().collect(
                Collectors.joining());
            assertThat(exceptionMessage, is(expectedMessage));
        }

        try {
            execute("COPY t FROM ? WITH (fail_fast = true, shared = false) return summary",
                    new Object[]{target.toUri().toString() + "*"});
        } catch (Exception e) {
            String exceptionMessage = e.getMessage();
            if (e.getMessage() == null || e.getMessage().isEmpty()) {
                fail("expect a message from this exception");
            }
            exceptionMessage = exceptionMessage.replaceAll("file:///.*/target/", "").replaceAll("],","]");
            exceptionMessage = Arrays.stream(exceptionMessage.split("\n")).filter(s -> s.startsWith("[URI:")).sorted().collect(
                Collectors.joining());
            assertThat(exceptionMessage, is(expectedMessage));
        }
    }

    private void assertExpectedLogMessages(Runnable command,
                                           MockLogAppender.LoggingExpectation ... expectations) throws IllegalAccessException {
        Logger testLogger = LogManager.getLogger("io.crate.execution.dml.upsert");
        MockLogAppender appender = new MockLogAppender();
        Loggers.addAppender(testLogger, appender);
        try {
            appender.start();
            Arrays.stream(expectations).forEach(appender::addExpectation);
            command.run();
            appender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(testLogger, appender);
        }
    }

}
