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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.IntStream;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import io.crate.testing.UseHashJoins;
import io.crate.testing.UseJdbc;
import io.crate.testing.UseRandomizedSchema;

@IntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
@UseJdbc(1)
@UseRandomizedSchema(random = false) // Avoid set session stmt to interfere with tests
@UseHashJoins(1) // Avoid set session stmt to interfere with tests
public class PostgresJobsLogsITest extends IntegTestCase {

    Properties properties = new Properties();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder();
        return builder.put(super.nodeSettings(nodeOrdinal))
            .put("psql.port", "4244-4299")
            .put("stats.jobs_log_expiration", "30m")
            .put("network.host", "127.0.0.1").build();
    }

    @Before
    public void initDriverAndStats() throws Exception {
        properties.setProperty("user", "crate");
        try (Connection conn = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties)) {
            ResultSet rs = conn.createStatement().executeQuery("select stmt from sys.jobs");
            assertTrue("sys.jobs must contain statement", rs.next());
            assertEquals(rs.getString(1), "select stmt from sys.jobs");
        }
    }

    @After
    public void resetStats() throws Exception {
        try (Connection conn = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties)) {
            conn.createStatement().execute("reset global stats.enabled");
        }
    }

    @Test
    public void testFailingStatementIsRemovedFromSysJobs() throws Exception {
        try (Connection conn = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties)) {
            try {
                conn.createStatement().execute("set global 'foo.logger' = 'TRACE'");
            } catch (PSQLException e) {
                // this is expected
            }
            ResultSet result = conn.createStatement().executeQuery("select count(*) from sys.jobs");
            assertThat(result.next()).isTrue();
            assertThat(result.getLong(1), is(1L));
        }
    }

    @Test
    public void testStatsTableSuccess() throws Exception {
        try (Connection conn = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties)) {
            conn.setAutoCommit(true);
            ensureGreen();

            String uniqueId = UUID.randomUUID().toString();
            final String stmtStr = "select name, '" + uniqueId + "' from sys.cluster";

            conn.prepareStatement(stmtStr).execute();
            assertJobLogContains(conn, new String[]{stmtStr}, false);
        }
    }

    @Test
    public void testBatchOperationStatsTableSuccess() throws Exception {
        try (Connection conn = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties)) {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("create table t (x string) with (number_of_replicas = 0)");
            ensureGreen();

            String uniqueId1 = UUID.randomUUID().toString();
            String uniqueId2 = UUID.randomUUID().toString();
            Statement statement = conn.createStatement();
            final String stmtStr1 = "insert into t (x) values ('" + uniqueId1 + "')";
            final String stmtStr2 = "insert into t (x) values ('" + uniqueId2 + "')";
            statement.addBatch(stmtStr1);
            statement.addBatch(stmtStr2);
            int[] results = statement.executeBatch();
            assertThat(results, is(new int[]{1, 1}));
            assertJobLogContains(conn, new String[]{stmtStr1, stmtStr2}, false);
        }
    }

    @Test
    public void testStatsTableFailure() throws Exception {
        try (Connection conn = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties)) {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("create table t (a integer not null, b string) " +
                                                 "with (number_of_replicas = 0)");
            ensureGreen();

            String uniqueId = UUID.randomUUID().toString();
            final String stmtStr = "insert into t(a,b) values(null, '" + uniqueId + "')";
            try {
                conn.prepareStatement(stmtStr).execute();
                fail("NOT NULL constraint is not respected");
            } catch (Exception e) {
                assertJobLogContains(conn, new String[]{stmtStr}, true);
            }
        }
    }

    @Test
    public void testBatchOperationStatsTableFailure() throws Exception {
        try (Connection conn = DriverManager.getConnection(sqlExecutor.jdbcUrl(), properties)) {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("create table t (a integer not null, x string) " +
                                                 "with (number_of_replicas = 0)");
            ensureGreen();

            String uniqueId1 = UUID.randomUUID().toString();
            String uniqueId2 = UUID.randomUUID().toString();
            String uniqueId3 = UUID.randomUUID().toString();
            Statement statement = conn.createStatement();
            final String insert1 = "insert into t (a, x) values (1, '" + uniqueId1 + "')";
            final String insertNull = "insert into t (a, x) values (null, '" + uniqueId2 + "')";
            final String insert3 = "insert into t (a, x) values (3, '" + uniqueId3 + "')";

            statement.addBatch(insert1);
            statement.addBatch(insertNull);
            statement.addBatch(insert3);
            try {
                int[] result = statement.executeBatch();
                assertThat("One result must be 0 because it failed due to NOT NULL",
                    IntStream.of(result).filter(i -> i == 0).count(), is(1L));
                assertThat("Two inserts must have succeeded",
                    IntStream.of(result).filter(i -> i == 1).count(), is(2L));
            } catch (Exception e) {
                assertJobLogContains(conn, new String[]{insert1, insert3}, false);
                assertJobLogContains(conn, new String[]{insertNull}, true);
            }
        }
    }

    private void assertJobLogContains(final Connection conn,
                                      final String[] statements,
                                      final boolean checkForError) throws Exception {
        PreparedStatement pStmt = conn.prepareStatement("select stmt, error from sys.jobs_log where stmt = ?");
        assertBusy(() -> {
            try {
                for (String stmtStr : statements) {
                    pStmt.setString(1, stmtStr);
                    ResultSet resultSet = pStmt.executeQuery();
                    assertThat(resultSet.next())
                        .as("sys.jobs_log must have an entry WHERE stmt=" + stmtStr)
                        .isTrue();
                    assertThat(resultSet.getString(1), is(stmtStr));
                    if (checkForError) {
                        assertThat(resultSet.getString(2), is("\"a\" must not be null"));
                    }
                }
            } catch (Exception e) {
                fail("Shouldn't throw an exception: " + e.getMessage());
            }
        });
    }
}
