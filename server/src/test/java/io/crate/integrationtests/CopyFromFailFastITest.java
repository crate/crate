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
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.carrotsearch.randomizedtesting.LifecycleScope;

import io.crate.action.sql.Session;
import io.crate.action.sql.Sessions;
import io.crate.exceptions.JobKilledException;
import io.crate.testing.UseJdbc;
import io.crate.testing.UseRandomizedOptimizerRules;
import io.crate.user.User;

@IntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0, supportsDedicatedMasters = false)
public class CopyFromFailFastITest extends IntegTestCase {

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
        cluster().startNode();
        cluster().ensureAtLeastNumDataNodes(1);

        // a single uri with 'shared = true' implies that one node will be involved at all times.
        Path tmpDir = newTempDir(LifecycleScope.TEST);
        Path target = Files.createDirectories(tmpDir.resolve("target"));
        tmpFileWithLines(Arrays.asList(("{\"a\":987654321},".repeat(10) + "{\"a\":\"fail here\"}," +
                                        "{\"a\":123456789},".repeat(10)).split(",")),
                         "data1.json",
                         target);

        execute("CREATE TABLE t (a int) CLUSTERED INTO 1 SHARDS");

        // fail_fast = true
        Assertions.assertThatThrownBy(() -> execute(
                "COPY t FROM ? WITH (bulk_size = 1, fail_fast = true)", // fail_fast = true
                new Object[]{target.toUri() + "*"}))
            .isExactlyInstanceOf(JobKilledException.class)
            .hasMessageContaining("ERRORS: {Cannot cast value `fail here` to type `integer`");
    }

    @UseRandomizedOptimizerRules(0)
    @Test
    public void test_copy_from_with_fail_fast_with_write_error_on_non_handler_node() throws Exception {
        cluster().startNode();
        cluster().startNode();
        cluster().ensureAtLeastNumDataNodes(2);

        execute("set global overload_protection.dml.initial_concurrency = 2");
        execute("set global overload_protection.dml.max_concurrency = 2");
        execute("set global overload_protection.dml.queue_size = 2");

        execute("CREATE TABLE doc.t (a INT PRIMARY KEY, b INT) CLUSTERED INTO 2 SHARDS WITH (number_of_replicas=0)");
        ensureGreen();

        execute("SELECT node['name'] FROM sys.shards WHERE table_name='t' ORDER BY id");
        var nodeNameOfShard0 = (String) response.rows()[0][0];
        Sessions sessions = cluster().getInstance(Sessions.class, nodeNameOfShard0);

        Path tmpDir = newTempDir(LifecycleScope.TEST);
        Path target = Files.createDirectories(tmpDir.resolve("target"));
        int numDocs = 100;

        var indexMetadata = clusterService().state().metadata().index("t");
        List<String> rows = new ArrayList<>();
        int failedNumDocs = 0;
        for (int i = 0; i < numDocs; i++) {
            String line;
            // Ensure the failing doc will fail on shard id 1
            if (i >= 20 && OperationRouting.generateShardId(indexMetadata, Integer.toString(i), null) == 1) {
                line = "{\"a\":" + i + ", \"b\":\"fail here\"}";
                failedNumDocs++;
            } else {
                line = "{\"a\":" + i + ", \"b\":" + i + "}";
            }
            rows.add(line);
        }

        tmpFileWithLines(rows, "data1.json", target);
        try (Session session = sessions.newSession("doc", User.CRATE_USER)) {
            Assertions.assertThatThrownBy(() -> execute(
                        "COPY doc.t FROM ? WITH (bulk_size = 1, fail_fast = true, shared= true)", // fail_fast = true
                        new Object[]{target.toUri() + "*"},
                        session))
                .isExactlyInstanceOf(JobKilledException.class)
                .hasMessageContaining("Cannot cast value `fail here` to type `integer`");
        }

        execute("REFRESH TABLE doc.t");
        execute("SELECT COUNT(*) FROM doc.t");
        assertThat((long) response.rows()[0][0]).isLessThanOrEqualTo(numDocs - failedNumDocs);
    }

    @Test
    public void test_copy_from_with_fail_fast_with_write_error_on_handler_node() throws Exception {
        cluster().startNode();
        cluster().startNode();
        cluster().ensureAtLeastNumDataNodes(2);

        execute("set global overload_protection.dml.initial_concurrency = 2");
        execute("set global overload_protection.dml.max_concurrency = 2");
        execute("set global overload_protection.dml.queue_size = 2");

        execute("CREATE TABLE doc.t (a INT PRIMARY KEY, b INT) CLUSTERED INTO 2 SHARDS WITH (number_of_replicas=0)");
        ensureGreen();

        execute("SELECT node['name'] FROM sys.shards WHERE table_name='t' ORDER BY id");
        var nodeNameOfShard0 = (String) response.rows()[0][0];

        Path tmpDir = newTempDir(LifecycleScope.TEST);
        Path target = Files.createDirectories(tmpDir.resolve("target"));
        int numDocs = 100;

        var indexMetadata = clusterService().state().metadata().index("t");
        List<String> rows = new ArrayList<>();
        int failedNumDocs = 0;
        for (int i = 0; i < numDocs; i++) {
            String line;
            // Ensure the failing doc will fail on shard id 0
            if (i >= 20 && OperationRouting.generateShardId(indexMetadata, Integer.toString(i), null) == 0) {
                failedNumDocs++;
                line = "{\"a\":" + i + ", \"b\":\"fail here\"}";
            } else {
                line = "{\"a\":" + i + ", \"b\":" + i + "}";
            }
            rows.add(line);
        }

        tmpFileWithLines(rows, "data1.json", target);
        Sessions sessions = cluster().getInstance(Sessions.class, nodeNameOfShard0);
        try (Session session = sessions.newSession("doc", User.CRATE_USER)) {
            Assertions.assertThatThrownBy(() -> execute(
                        "COPY doc.t FROM ? WITH (bulk_size = 1, fail_fast = true, shared= true)",
                        new Object[]{target.toUri() + "*"},
                        session))
                .isExactlyInstanceOf(JobKilledException.class)
                .hasMessageContaining("Cannot cast value `fail here` to type `integer`");
        }

        execute("REFRESH TABLE doc.t");
        execute("SELECT COUNT(*) FROM doc.t");
        assertThat((long) response.rows()[0][0]).isLessThanOrEqualTo(numDocs - failedNumDocs);
    }

    @Test
    public void test_copy_from_fail_fast_without_return_summary_without_write_errors() throws IOException {
        // fail_fast = true and fail_fast = false that do not fail take different UpsertResultCollectors.
        // fail_fast = false uses RowCountCollector while fail_fast = true uses newSummaryOnFailOrRowCountOnSuccessCollector
        // and in the case that nothing fail, the collected summary will turn into rowCounts
        cluster().startNode();

        Path tmpDir = newTempDir(LifecycleScope.TEST);
        Path target = Files.createDirectories(tmpDir.resolve("target"));
        tmpFileWithLines(Collections.singletonList(("{\"a\":123}")), "passing1.json", target);

        execute("CREATE TABLE t (a int)");

        execute("COPY t FROM ? WITH (fail_fast = true, shared = false)", new Object[]{target.toUri().toString() + "*"});
        refresh();
        execute("select * from t");
        assertThat(response.rowCount()).isEqualTo(cluster().numDataNodes());
    }
}
