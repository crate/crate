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
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.postgresql.util.PSQLException;

import java.sql.*;

import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0)
public class PostgresITest extends SQLTransportIntegrationTest {

    private static final String JDBC_POSTGRESQL_URL = "jdbc:postgresql://127.0.0.1:4242/";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("network.psql", true)
            .put("psql.host", "127.0.0.1")
            .put("psql.port", "4242")
            .build();
    }

    @Before
    public void initDriver() throws Exception {
        Class.forName("org.postgresql.Driver");
    }

    @Test
    public void testNoAutoCommit() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL)) {
            conn.setAutoCommit(false);
            expectedException.expectMessage("no viable alternative at input 'BEGIN'");
            conn.prepareStatement("select name from sys.cluster").executeQuery();
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
            stmt.executeUpdate("create table t (x string) with (number_of_replicas = 0)");
            ensureYellow();

            PreparedStatement preparedStatement = conn.prepareStatement("insert into t (x) values (?)");
            preparedStatement.setString(1, "Arthur");
            preparedStatement.addBatch();

            preparedStatement.setString(1, "Trillian");
            preparedStatement.addBatch();

            int[] results = preparedStatement.executeBatch();
            assertThat(results, is(new int[] { 1, 1}));
        }
    }

    @Test
    public void testCreateInsertSelect() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL)) {
            conn.setAutoCommit(true);
            Statement statement = conn.createStatement();
            assertThat(statement.executeUpdate("create table t (x string) with (number_of_replicas = 0)"), is(0));
            ensureYellow();

            assertThat(statement.executeUpdate("insert into t (x) values ('Marvin'), ('Trillian')"), is(2));;
            assertThat(statement.executeUpdate("refresh table t"), is(0));

            statement.executeQuery("select x from t order by x");
            ResultSet resultSet = statement.getResultSet();
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getString(1), is("Marvin"));

            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getString(1), is("Trillian"));
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

    private void assertSelectNameFromSysClusterWorks(Connection conn) throws SQLException {
        PreparedStatement stmt;// verify that queries can be made after an error occurred
        stmt = conn.prepareStatement("select name from sys.cluster");
        ResultSet resultSet = stmt.executeQuery();
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getString(1), Matchers.startsWith("SUITE-CHILD"));
    }
}
