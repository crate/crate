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

package io.crate.integrationtests;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;

import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class PostgresJobsLogsITest extends SQLTransportIntegrationTest {

    private static final String JDBC_POSTGRESQL_URL = "jdbc:crate://127.0.0.1:4244/";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder();
        return builder.put(super.nodeSettings(nodeOrdinal))
            .put("psql.port", "4244")
            .put("network.host", "127.0.0.1").build();
    }

    @Before
    public void initDriverAndStats() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL)) {
            conn.createStatement().execute("set global stats.enabled = true");
            ResultSet rs = conn.createStatement().executeQuery("select stmt from sys.jobs");
            assertTrue("sys.jobs must contain statement", rs.next());
            assertEquals(rs.getString(1), "select stmt from sys.jobs");
        }
    }

    @After
    public void resetStats() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL)) {
            conn.createStatement().execute("reset global stats.enabled");
            ResultSet rs = conn.createStatement().executeQuery("select stmt from sys.jobs");
            assertFalse("sys.jobs must not contain entries", rs.next());
        }
    }

    @Test
    public void testStatsTableSuccess() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL)) {
            conn.setAutoCommit(true);
            ensureGreen();

            String uniqueId = UUID.randomUUID().toString();
            final String stmtStr = "select name, '" + uniqueId + "' from sys.cluster";
            final String stmtStrWhere = "select name, ''" + uniqueId + "'' from sys.cluster";

            conn.prepareStatement(stmtStr).execute();
            assertJobLogContains(conn, new String[]{stmtStr}, new String[]{stmtStrWhere}, false);
        }
    }

    @Test
    public void testBatchOperationStatsTableSuccess() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL)) {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("create table t (x string) with (number_of_replicas = 0)");
            ensureGreen();

            String uniqueId1 = UUID.randomUUID().toString();
            String uniqueId2 = UUID.randomUUID().toString();
            Statement statement = conn.createStatement();
            final String stmtStr1 = "insert into t (x) values ('" + uniqueId1 + "')";
            final String stmtStr1Where = "insert into t (x) values (''" + uniqueId1 + "'')";
            final String stmtStr2 = "insert into t (x) values ('" + uniqueId2 + "')";
            final String stmtStr2Where = "insert into t (x) values (''" + uniqueId2 + "'')";
            statement.addBatch(stmtStr1);
            statement.addBatch(stmtStr2);
            int[] results = statement.executeBatch();
            assertThat(results, is(new int[]{1, 1}));

            assertJobLogContains(conn,
                new String[]{stmtStr1, stmtStr2},
                new String[]{stmtStr1Where, stmtStr2Where},
                false);
        }
    }

    @Test
    public void testStatsTableFailure() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL)) {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("create table t (a integer not null, b string) " +
                                                 "with (number_of_replicas = 0)");
            ensureGreen();

            String uniqueId = UUID.randomUUID().toString();
            final String stmtStr = "insert into t(a,b) values(null, '" + uniqueId + "')";
            final String stmtStrWhere = "insert into t(a,b) values(null, ''" + uniqueId + "'')";
            try {
                conn.prepareStatement(stmtStr).execute();
                fail("NOT NULL constraint is not respected");
            } catch (Exception e) {
                assertJobLogContains(conn, new String[]{stmtStr}, new String[]{stmtStrWhere}, true);
            }
        }
    }

    @Test
    public void testBatchOperationStatsTableFailure() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL)) {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("create table t (a integer not null, x string) " +
                                                 "with (number_of_replicas = 0)");
            ensureGreen();

            String uniqueId1 = UUID.randomUUID().toString();
            String uniqueId2 = UUID.randomUUID().toString();
            String uniqueId3 = UUID.randomUUID().toString();
            Statement statement = conn.createStatement();
            final String stmtStr1 = "insert into t (a, x) values (1, '" + uniqueId1 + "')";
            final String stmtStr1Where = "insert into t (a, x) values (1, ''" + uniqueId1 + "'')";
            final String stmtStr2 = "insert into t (a, x) values (null, '" + uniqueId2 + "')";
            final String stmtStr2Where = "insert into t (a, x) values (null, ''" + uniqueId2 + "'')";
            final String stmtStr3 = "insert into t (a, x) values (3, '" + uniqueId3 + "')";
            final String stmtStr3Where = "insert into t (a, x) values (3, ''" + uniqueId3 + "'')";
            statement.addBatch(stmtStr1);
            statement.addBatch(stmtStr2);
            statement.addBatch(stmtStr3);
            try {
                statement.executeBatch();
                fail("NOT NULL constraint is not respected");
            } catch (Exception e) {
                assertJobLogContains(conn,
                    new String[]{stmtStr1, stmtStr3},
                    new String[]{stmtStr1Where, stmtStr3Where},
                    false);
                assertJobLogContains(conn,
                    new String[]{stmtStr2},
                    new String[]{stmtStr2Where},
                    true);
            }
        }
    }

    private void assertJobLogContains(final Connection conn,
                                      final String[] stmtStrs,
                                      final String[] stmtStrsWhere,
                                      final boolean checkForError) throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                try {
                    for (int i = 0; i < stmtStrs.length; i++) {
                        String stmtStr = stmtStrs[i];
                        String stmtStrWhere = stmtStrsWhere[i];
                        ResultSet resultSet = conn.prepareStatement("select stmt, error from sys.jobs_log " +
                                                                    "where stmt='" + stmtStrWhere + "'").executeQuery();
                        assertThat(resultSet.next(), is(true));
                        assertThat(resultSet.getString(1), is(stmtStr));
                        if (checkForError) {
                            assertThat(resultSet.getString(2), is("Cannot insert null value for column 'a'"));
                        }
                    }
                } catch (Exception e) {
                    fail("Shouldn't throw an exception: " + e.getMessage());
                }
            }
        });
    }
}
