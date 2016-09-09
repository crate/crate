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

import io.crate.action.sql.SQLOperations;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.PGProperty;
import org.postgresql.jdbc.PreferQueryMode;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.*;
import java.util.*;

import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0)
public class PostgresITest extends SQLTransportIntegrationTest {

    private static final String JDBC_POSTGRESQL_URL = "jdbc:postgresql://127.0.0.1:4242/";
    private static final String JDBC_POSTGRESQL_URL_READ_ONLY = "jdbc:postgresql://127.0.0.1:4243/";

    private Properties properties = new Properties();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder();
        builder.put(super.nodeSettings(nodeOrdinal))
            .put("psql.enabled", true)
            .put("network.host", "127.0.0.1");

        if ((nodeOrdinal + 1) % 2 == 0) {
            builder.put("psql.port", "4242");
        } else {
            builder.put(SQLOperations.NODE_READ_ONLY_SETTING, true);
            builder.put("psql.port", "4243");
        }
        return builder.build();
    }

    @Before
    public void initProperties() throws Exception {
        if (randomBoolean()) {
            properties.setProperty("prepareThreshold", "-1"); // force binary transfer
        }
    }

    @Test
    public void testGetTransactionIsolation() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL, properties)) {
            int var = conn.getTransactionIsolation();
            assertThat(var, is(Connection.TRANSACTION_READ_UNCOMMITTED));
        }
    }

    @Test
    public void testUseOfUnsupportedType() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL, properties)) {
            PreparedStatement stmt = conn.prepareStatement("select ? from sys.cluster");
            stmt.setObject(1, UUID.randomUUID());

            expectedException.expectMessage("Can't map PGType with oid=2950 to Crate type");
            stmt.executeQuery();
        }
    }

    @Test
    public void testEmptyStatement() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL, properties)) {
            assertThat(conn.createStatement().execute(""), is(false));

            try {
                conn.createStatement().executeQuery("");
                fail("executeQuery with empty query should throw a 'No results were returned by the query' error");
            } catch (PSQLException e) {
                // can't use expectedException.expectMessage because error messages are localized and locale is randomized
                assertThat(e.getSQLState(), is(PSQLState.NO_DATA.getState()));
            }
        }
    }

    @Test
    public void testSimpleQuery() throws Exception {
        properties.setProperty(PGProperty.PREFER_QUERY_MODE.getName(), PreferQueryMode.SIMPLE.value());
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL, properties)) {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("create table t (x int) with (number_of_replicas = 0)");
            conn.createStatement().executeUpdate("insert into t (x) values (1), (2)");
            conn.createStatement().executeUpdate("refresh table t");

            ResultSet resultSet = conn.createStatement().executeQuery("select * from t");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getInt(1), is(1));
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getInt(1), is(2));
        }
    }

    @Test
    public void testPreparedStatementHandling() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("prepareThreshold", "-1");
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL, properties)) {
            PreparedStatement p1 = conn.prepareStatement("select 1 from sys.cluster");
            ResultSet resultSet = p1.executeQuery();
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getInt(1), is(1));

            PreparedStatement p2 = conn.prepareStatement("select 2 from sys.cluster");
            resultSet = p2.executeQuery();
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getInt(1), is(2));

            resultSet = p1.executeQuery();
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getInt(1), is(1));
        }
    }

    @Test
    public void testPreparedSelectStatementWithParametersCanBeDescribed() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("prepareThreshold", "-1");
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL, properties)) {
            PreparedStatement p1 = conn.prepareStatement("select ? from sys.cluster");
            p1.setInt(1, 20);
            ResultSet resultSet = p1.executeQuery();
            /*
             * This execute results in the following messages:
             *
             *      method=parse stmtName=S_1 query=select $1 from sys.cluster paramTypes=[integer]
             *      method=describe type=S portalOrStatement=S_1
             *      method=sync
             *      method=bind portalName= statementName=S_1 params=[20]
             *      method=describe type=P portalOrStatement=
             *      method=execute portalName= maxRows=0
             *      method=sync
             *
             * The tests makes sure that it works even though the describe can't analyze the statement (because of missing params)
             */
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getInt(1), is(20));
        }
    }

    @Test
    public void testWriteOperationsWithoutAutocommitAndCommitAndRollback() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL, properties)) {
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
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL, properties)) {
            conn.setAutoCommit(false);
            ResultSet resultSet = conn.prepareStatement("select name from sys.cluster").executeQuery();
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getString(1), Matchers.startsWith("SUITE-CHILD"));
        }
    }

    @Test
    public void testArrayTypeSupport() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL, properties)) {
            conn.createStatement().executeUpdate(
                "create table t (" +
                "   ints array(int)," +
                "   strings array(string)," +
                "   points array(geo_point) ) with (number_of_replicas = 0)");

            PreparedStatement preparedStatement = conn.prepareStatement("insert into t (ints, strings, points) " +
                                                                        "values (?, ?, ?)");
            preparedStatement.setArray(1, conn.createArrayOf("int4", new Integer[] { 10, 20 }));
            preparedStatement.setArray(2, conn.createArrayOf("varchar", new String[] { "foo", "bar" }));
            preparedStatement.setArray(3, conn.createArrayOf("float8", new Double[][] {new Double[]{1.1, 2.2},
                                                                                       new Double[] {3.3, 4.4}}));
            preparedStatement.executeUpdate();

            conn.createStatement().executeUpdate("refresh table t");

            ResultSet resultSet = conn.createStatement().executeQuery("select ints, strings, points from t");
            assertThat(resultSet.next(), is(true));

            Object[] object = (Object[]) resultSet.getArray(1).getArray();
            assertThat(object, is(new Object[] { 10, 20 }));

            object = (Object[]) resultSet.getArray(2).getArray();
            assertThat(object, is(new Object[] { "foo", "bar" }));

            object = (Object[]) resultSet.getArray(3).getArray();
            assertThat((Double[][]) object, is(new Double[][] {new Double[] { 1.1, 2.2 }, new Double[] { 3.3, 4.4 }}));
        } catch (BatchUpdateException e) {
            throw e.getNextException();
        }
    }

    @Test
    public void testFetchSize() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL, properties)) {
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
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL, properties)) {
            conn.setAutoCommit(true);
            PreparedStatement preparedStatement = conn.prepareStatement("select name from sys.cluster");
            ResultSet resultSet = preparedStatement.executeQuery();
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getString(1), Matchers.startsWith("SUITE-CHILD"));
        }
    }

    @Test
    public void testExecuteBatch() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL, properties)) {
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
            assertThat(results, is(new int[] {1, 1}));
        }
    }

    @Test
    public void testExecuteBatchWithOneFailingAndNothingExecuted() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL, properties)) {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("create table t (x int) with (number_of_replicas = 0)");
            ensureYellow();
            PreparedStatement preparedStatement = conn.prepareStatement("insert into t (x) values (cast(? as integer))");
            preparedStatement.setString(1, Integer.toString(1));
            preparedStatement.addBatch();

            preparedStatement.setString(1, "foobar");
            preparedStatement.addBatch();

            preparedStatement.setString(1, Integer.toString(3));
            preparedStatement.addBatch();

            try {
                preparedStatement.executeBatch();
                fail("executeBatch must throw an exception");
            } catch (BatchUpdateException e) {
                assertThat(e.getUpdateCounts(), is(new int[] { -3, -3, -3 }));
                SQLException nextException = e.getNextException();
                assertThat(nextException.getMessage(), Matchers.containsString("cannot cast 'foobar' to type integer"));
            }

            conn.createStatement().executeUpdate("refresh table t");
            ResultSet rs = conn.createStatement().executeQuery("select count(*) from t");
            assertThat(rs.next(), is(true));
            // all failed because the error happened during analysis and so nothing is executed
            assertThat(rs.getInt(1), is(0));
        }
    }

    @Test
    public void testExecuteBatchWithOneRuntimeFailure() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL, properties)) {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("create table t (id int primary key, x int) with (number_of_replicas = 0)");
            ensureYellow();
            PreparedStatement preparedStatement = conn.prepareStatement("insert into t (id, x) values (?, ?)");
            preparedStatement.setInt(1, 1);
            preparedStatement.setInt(2, 0);
            preparedStatement.addBatch();

            preparedStatement.setInt(1, 2);
            preparedStatement.setInt(2, 10);
            preparedStatement.addBatch();

            assertThat(preparedStatement.executeBatch(), is(new int[] { 1, 1}));
            conn.createStatement().executeUpdate("refresh table t");

            preparedStatement = conn.prepareStatement("update t set x = log(x) where id = ?");
            preparedStatement.setInt(1, 1);
            preparedStatement.addBatch();

            preparedStatement.setInt(1, 2);
            preparedStatement.addBatch();
            assertThat(preparedStatement.executeBatch(), is(new int[] {-3, 1}));
            conn.createStatement().executeUpdate("refresh table t");

            ResultSet rs = conn.createStatement().executeQuery("select x from t order by id");
            assertThat(rs.next(), is(true));
            assertThat(rs.getInt(1), is(0)); // log(0) is an error - the update failed and the value remains unchanged

            assertThat(rs.next(), is(true));
            assertThat(rs.getInt(1), is(1)); // log(10) -> 1
        }
    }

    @Test
    public void testExecuteBatchWithDifferentStatements() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL, properties)) {
            conn.setAutoCommit(true);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("create table t (x int) with (number_of_replicas = 0)");
            ensureYellow();

            Statement statement = conn.createStatement();
            statement.addBatch("insert into t (x) values (1)");
            statement.addBatch("insert into t (x) values (2)");

            int[] results = statement.executeBatch();
            assertThat(results, is(new int[] {1, 1}));

            statement.executeUpdate("refresh table t");
            ResultSet resultSet = statement.executeQuery("select * from t order by x");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getInt(1), is(1));

            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getInt(1), is(2));
        }
    }

    @Test
    public void testCreateInsertSelectStringAndTimestamp() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL, properties)) {
            conn.setAutoCommit(true);
            Statement statement = conn.createStatement();
            assertThat(statement.executeUpdate("create table t (x string, ts timestamp) with (number_of_replicas = 0)"), is(0));
            ensureYellow();

            assertThat(statement.executeUpdate("insert into t (x, ts) values ('Marvin', '2016-05-14'), ('Trillian', '2016-06-28')"), is(2));
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
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL, properties)) {
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
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL, properties)) {
            conn.setAutoCommit(true);
            PreparedStatement stmt = conn.prepareStatement("select name fro sys.cluster");
            expectedException.expect(PSQLException.class);
            expectedException.expectMessage("mismatched input 'sys'");
            stmt.executeQuery();
        }
    }

    @Test
    public void testCustomSchemaAndAnalyzerFailure() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL + "foo", properties)) {
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
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL_READ_ONLY, properties)) {
            conn.setAutoCommit(true);
            PreparedStatement stmt = conn.prepareStatement("create table test(a integer)");
            expectedException.expect(PSQLException.class);
            expectedException.expectMessage("ERROR: Only read operations are allowed on this node");
            stmt.executeQuery();
        }
    }

    @Test
    public void testErrorRecoveryFromErrorsOutsideSqlOperations() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_POSTGRESQL_URL, properties)) {
            conn.setAutoCommit(true);
            PreparedStatement stmt = conn.prepareStatement("select cast([10.3, 20.2] as integer) " +
                                                           "from information_schema.tables");
            try {
                stmt.executeQuery();
                fail("Should've raised PSQLException");
            } catch (PSQLException e) {
                assertThat(e.getMessage(), Matchers.containsString("cannot cast [10.3, 20.2] to type integer"));
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
