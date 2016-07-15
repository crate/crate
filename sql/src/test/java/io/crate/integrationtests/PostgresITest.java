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

import io.crate.action.sql.TransportBaseSQLAction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.postgresql.util.PSQLException;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0)
public class PostgresITest extends SQLTransportIntegrationTest {

    private static final String JDBC_POSTGRESQL_URL = "jdbc:postgresql://127.0.0.1:4242/";
    private static final String JDBC_POSTGRESQL_URL_READ_ONLY = "jdbc:postgresql://127.0.0.1:4243/";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder();
        builder.put(super.nodeSettings(nodeOrdinal))
            .put("psql.enabled", true)
            .put("psql.host", "127.0.0.1");

        if ((nodeOrdinal + 1) % 2 == 0) {
            builder.put("psql.port", "4242");
        } else {
            builder.put(TransportBaseSQLAction.NODE_READ_ONLY_SETTING, true);
            builder.put("psql.port", "4243");
        }
        return builder.build();
    }

    @After
    public void resetSettings() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL)) {
            conn.createStatement().execute("reset global stats.enabled");
        }
    }

    @Before
    public void initDriver() throws Exception {
        Class.forName("org.postgresql.Driver");
    }

    @Test
    public void testWriteOperationsWithoutAutocommitAndCommitAndRollback() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL)) {
            conn.setAutoCommit(false);
            try (Statement statement = conn.createStatement()) {
                statement.executeUpdate("create table t (x int) with (number_of_replicas = 0)");
            }
            try (PreparedStatement stmt = conn.prepareStatement("insert into t (x) values (1), (2)")) {
                stmt.executeUpdate();
            }
            try (Statement statement = conn.createStatement()) {
                statement.executeUpdate("refresh table t");
            }

            // Because our ReadyForQuery messages always sends transaction-status IDLE, rollback and commit don't do anything
            conn.rollback();
            conn.commit();

            try (Statement statement = conn.createStatement()) {
                // no error because table was created
                ResultSet resultSet = statement.executeQuery("select * from t");
                assertThat(resultSet.next(), is(true));
                assertThat(resultSet.next(), is(true));
            }
        }
    }

    @Test
    public void testNoAutoCommit() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL)) {
            conn.setAutoCommit(false);
            ResultSet resultSet = conn.prepareStatement("select name from sys.cluster").executeQuery();
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getString(1), Matchers.startsWith("SUITE-CHILD"));
        }
    }

    @Test
    public void testFetchSize() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL)) {
            conn.createStatement().executeUpdate("create table t (x int) with (number_of_replicas = 0)");
            ensureGreen();

            PreparedStatement preparedStatement = conn.prepareStatement("insert into t (x) values (?)");
            for (int i = 0; i < 20; i++) {
                preparedStatement.setInt(1, i);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();

            conn.createStatement().executeUpdate("refresh table t");

            conn.setAutoCommit(false);
            try (Statement st = conn.createStatement()) {
                st.setFetchSize(2);
                try (ResultSet resultSet = st.executeQuery("select x from t")) {
                    List<Integer> result = new ArrayList<>();
                    while (resultSet.next()) {
                        result.add(resultSet.getInt(1));
                    }
                    Collections.sort(result);
                    assertThat(result, Matchers.contains(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19));
                }
            }
        }
    }

    @Test
    public void testSelectPreparedStatement() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL)) {
            conn.setAutoCommit(true);
            PreparedStatement preparedStatement = conn.prepareStatement("select name from sys.cluster");
            ResultSet resultSet = preparedStatement.executeQuery();
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getString(1), Matchers.startsWith("SUITE-CHILD"));
        }
    }

    @Test
    public void testExecuteBatch() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL)) {
            conn.setAutoCommit(true);

            Statement stmt = conn.createStatement();
            stmt.executeUpdate("create table t (x string, b boolean) with (number_of_replicas = 0)");
            ensureYellow();

            PreparedStatement preparedStatement = conn.prepareStatement("insert into t (x, b) values (?, ?)");
            preparedStatement.setString(1, "Arthur");
            preparedStatement.setBoolean(2, true);
            preparedStatement.addBatch();

            preparedStatement.setString(1, "Trillian");
            preparedStatement.setBoolean(2, false);
            preparedStatement.addBatch();

            int[] results = preparedStatement.executeBatch();
            assertThat(results, is(new int[] { 1, 1}));
        }
    }

    @Test
    public void testExecuteBatchWithDifferentStatements() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL)) {
            conn.setAutoCommit(true);

            Statement stmt = conn.createStatement();
            stmt.executeUpdate("create table t (x int) with (number_of_replicas = 0)");
            ensureYellow();

            Statement statement = conn.createStatement();
            statement.addBatch("insert into t (x) values (1)");
            statement.addBatch("insert into t (x) values (2)");

            // mixed statements not supported
            expectedException.expect(BatchUpdateException.class);
            statement.executeBatch();
        }
    }

    @Test
    public void testCreateInsertSelectStringAndTimestamp() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL)) {
            conn.setAutoCommit(true);
            Statement statement = conn.createStatement();
            assertThat(statement.executeUpdate("create table t (x string, ts timestamp) with (number_of_replicas = 0)"), is(0));
            ensureYellow();

            assertThat(statement.executeUpdate("insert into t (x, ts) values ('Marvin', '2016-05-14'), ('Trillian', '2016-06-28')"), is(2));;
            assertThat(statement.executeUpdate("refresh table t"), is(0));

            statement.executeQuery("select x, ts from t order by x");
            ResultSet resultSet = statement.getResultSet();
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getString(1), is("Marvin"));
            assertThat(resultSet.getTimestamp(2), is(new Timestamp(1463184000000L)));

            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getString(1), is("Trillian"));
            assertThat(resultSet.getTimestamp(2), is(new Timestamp(1467072000000L)));
        }
    }

    @Test
    public void testSelectWithParameters() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL)) {
            conn.setAutoCommit(true);
            PreparedStatement preparedStatement = conn.prepareStatement("select name from sys.cluster where name like ?");
            preparedStatement.setString(1, "SUITE%");
            ResultSet resultSet = preparedStatement.executeQuery();
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getString(1), Matchers.startsWith("SUITE-CHILD"));
        }
    }

    @Test
    public void testStatementThatResultsInParseError() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL)) {
            conn.setAutoCommit(true);
            PreparedStatement stmt = conn.prepareStatement("select name fro sys.cluster");
            expectedException.expect(PSQLException.class);
            expectedException.expectMessage("mismatched input 'sys'");
            stmt.executeQuery();
        }
    }

    @Test
    public void testCustomSchemaAndAnalyzerFailure() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL + "foo")) {
            conn.setAutoCommit(true);
            PreparedStatement stmt = conn.prepareStatement("select x from t");
            try {
                stmt.executeQuery();
                assertFalse(true); // should've raised PSQLException
            } catch (PSQLException e) {
                assertThat(e.getMessage(), Matchers.containsString("Schema 'foo' unknown"));
            }
            assertSelectNameFromSysClusterWorks(conn);
        }
    }

    @Test
    public void testStatementReadOnlyFailure() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL_READ_ONLY)) {
            conn.setAutoCommit(true);
            PreparedStatement stmt = conn.prepareStatement("create table test(a integer)");
            expectedException.expect(PSQLException.class);
            expectedException.expectMessage("ERROR: Only read operations are allowed on this node");
            stmt.executeQuery();
        }
    }

    @Test
    public void testErrorRecoveryFromErrorsOutsideSqlOperations() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL)) {
            conn.setAutoCommit(true);
            PreparedStatement stmt = conn.prepareStatement("select * from information_schema.tables");
            try {
                stmt.executeQuery();
                assertFalse(true); // should've raised PSQLException
            } catch (PSQLException e) {
                assertThat(e.getMessage(), Matchers.containsString("No type mapping from 'string_array' to"));
            }

            assertSelectNameFromSysClusterWorks(conn);
        }
    }

    @Test
    public void testStatsTableSuccess() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL)) {
            conn.setAutoCommit(true);
            conn.createStatement().execute("set global stats.enabled=true");
            conn.prepareStatement("select name from sys.cluster").execute();

            ResultSet resultSet = conn.prepareStatement("select stmt from sys.jobs_log order by ended desc;")
                .executeQuery();
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getString(1), is("select name from sys.cluster"));
        }
    }

    @Test
    public void testStatsTableFailure() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL)) {
            conn.setAutoCommit(true);
            conn.createStatement().execute("set global stats.enabled=true");
            conn.prepareStatement("create table t (a integer not null, b string)").execute();

            try {
                conn.prepareStatement("insert into t(a,b) values(null, 'test')").execute();
            } catch (Exception e) {}

            ResultSet resultSet = conn.prepareStatement("select stmt, error from sys.jobs_log order by ended desc;")
                .executeQuery();
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getString(1), is("insert into t(a,b) values(null, 'test')"));
            assertThat(resultSet.getString(2), is("Cannot insert null value for column a"));
        }
    }

    private void assertSelectNameFromSysClusterWorks(Connection conn) throws SQLException {
        PreparedStatement stmt;// verify that queries can be made after an error occurred
        stmt = conn.prepareStatement("select name from sys.cluster");
        ResultSet resultSet = stmt.executeQuery();
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getString(1), Matchers.startsWith("SUITE-CHILD"));
    }
}
