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
import io.crate.execution.engine.collect.stats.JobsLogService;
import io.crate.protocols.postgres.PostgresNetty;
import io.crate.testing.UseJdbc;
import io.crate.types.DataTypes;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.postgresql.PGProperty;
import org.postgresql.geometric.PGpoint;
import org.postgresql.jdbc.PreferQueryMode;
import org.postgresql.util.PGobject;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.UUID;

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.protocols.postgres.PostgresNetty.PSQL_PORT_SETTING;
import static io.crate.testing.Asserts.assertThrows;
import static io.crate.testing.SQLErrorMatcher.isPGError;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
public class PostgresITest extends SQLTransportIntegrationTest {

    private static final String NO_IPV6 = "CRATE_TESTS_NO_IPV6";
    private static final String RO = "node_s0";
    private static final String RW = "node_s1";

    private Properties properties = new Properties();
    private static boolean useIPv6;

    @BeforeClass
    public static void setupClass() {
        useIPv6 = randomBoolean() && !"true".equalsIgnoreCase(System.getenv(NO_IPV6));
    }

    private String url(String nodeName) {
        PostgresNetty postgresNetty = internalCluster().getInstance(PostgresNetty.class, nodeName);
        int port = postgresNetty.boundAddress().publishAddress().getPort();
        if (useIPv6) {
            return "jdbc:postgresql://::1:" + port + '/';
        }
        return "jdbc:postgresql://127.0.0.1:" + port + '/';
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder();
        builder.put(super.nodeSettings(nodeOrdinal));

        if (useIPv6) {
            builder.put("network.host", "::1");
        } else {
            builder.put("network.host", "127.0.0.1");
        }

        if ((nodeOrdinal + 1) % 2 != 0) {
            builder.put(PSQL_PORT_SETTING.getKey(), "5432-5532");
            builder.put(SQLOperations.NODE_READ_ONLY_SETTING.getKey(), true);
        } else {
            builder.put(PSQL_PORT_SETTING.getKey(), "5533-5633");
        }
        return builder.build();
    }

    @Before
    public void initProperties() throws Exception {
        if (randomBoolean()) {
            // force binary transfer & use server-side prepared statements
            properties.setProperty("prepareThreshold", "-1");
        }
        if (randomBoolean()) {
            properties.setProperty("user", "crate");
        }
    }

    @Test
    public void testGetTransactionIsolation() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            int var = conn.getTransactionIsolation();
            assertThat(var, is(Connection.TRANSACTION_READ_UNCOMMITTED));
        }
    }

    @Test
    public void testMultidimensionalArrayWithDifferentSizedArrays() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(PGProperty.PREFER_QUERY_MODE.getName(), PreferQueryMode.SIMPLE.value());
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            Statement statement = conn.createStatement();
            statement.executeUpdate("create table t (o1 array(object as (o2 array(object as (x int)))))");
            ensureYellow();

            statement.setEscapeProcessing(false);
            statement.executeUpdate("insert into t (o1) values ( [ {o2=[{x=1}, {x=2}]}, {o2=[{x=3}]} ] )");
            statement.executeUpdate("refresh table t");

            ResultSet resultSet = statement.executeQuery("select o1['o2']['x'] from t");
            assertThat(resultSet.next(), is(true));
            String array = resultSet.getString(1);
            assertThat(array, is("[[1,2],[3]]"));
        }

        properties = new Properties();
        properties.setProperty("prepareThreshold", "-1"); // force binary transfer
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            ResultSet resultSet = conn.createStatement().executeQuery("select o1['o2']['x'] from t");
            assertThat(resultSet.next(), is(true));
            String array = resultSet.getString(1);
            assertThat(array, is("[[1,2],[3]]"));
        }
    }

    @Test
    public void testUseOfUnsupportedType() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            PreparedStatement stmt = conn.prepareStatement("select ? from sys.cluster");
            stmt.setObject(1, UUID.randomUUID());

            expectedException.expectMessage("Can't map PGType with oid=2950 to Crate type");
            stmt.executeQuery();
        }
    }

    @Test
    public void testEmptyStatement() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
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
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("create table t (x int) with (number_of_replicas = 0)");
            conn.createStatement().executeUpdate("insert into t (x) values (1), (2)");
            conn.createStatement().executeUpdate("refresh table t");

            ResultSet resultSet = conn.createStatement().executeQuery("select * from t order by x");
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
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
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
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
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
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
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
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            conn.setAutoCommit(false);
            ResultSet resultSet = conn.prepareStatement("select name from sys.cluster").executeQuery();
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getString(1), Matchers.startsWith("SUITE-CHILD"));
        }
    }

    @Test
    public void test_char_types_arrays() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            conn.createStatement().executeUpdate(
                "CREATE TABLE t (" +
                "   chars array(byte)," +
                "   strings array(text)) " +
                "WITH (number_of_replicas = 0)");

            PreparedStatement preparedStatement = conn.prepareStatement(
                "INSERT INTO t (chars, strings) VALUES (?, ?)");
            preparedStatement.setArray(1, conn.createArrayOf("char", new Byte[]{'c', '3'}));
            preparedStatement.setArray(2, conn.createArrayOf("varchar", new String[]{"fo,o", "bar"}));
            preparedStatement.executeUpdate();
            conn.createStatement().execute("REFRESH TABLE t");

            ResultSet resultSet = conn.createStatement().executeQuery("SELECT chars, strings FROM t");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getArray(1).getArray(), is(new String[]{"99", "51"}));
            assertThat(resultSet.getArray(2).getArray(), is(new String[]{"fo,o", "bar"}));
        } catch (BatchUpdateException e) {
            throw e.getNextException();
        }
    }

    @Test
    public void test_numeric_types_arrays() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            conn.createStatement().executeUpdate(
                "CREATE TABLE t (" +
                "   ints array(int)," +
                "   floats array(float)" +
                ") " +
                "WITH (number_of_replicas = 0)");

            PreparedStatement preparedStatement = conn.prepareStatement(
                "INSERT INTO t (ints, floats) VALUES (?, ?)");
            preparedStatement.setArray(1, conn.createArrayOf("int4", new Integer[]{10, 20}));
            preparedStatement.setArray(2, conn.createArrayOf("float4", new Float[]{1.2f, 3.5f}));
            preparedStatement.executeUpdate();
            conn.createStatement().execute("REFRESH TABLE t");

            ResultSet rs = conn.createStatement().executeQuery("SELECT ints, floats FROM t");
            assertThat(rs.next(), is(true));
            assertThat(rs.getArray(1).getArray(), is(new Integer[]{10, 20}));
            assertThat(rs.getArray(2).getArray(), is(new Float[]{1.2f, 3.5f}));
        } catch (BatchUpdateException e) {
            throw e.getNextException();
        }
    }

    @Test
    public void test_geo_types_arrays() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            conn.createStatement().execute(
                "CREATE TABLE t (" +
                "   geo_points array(geo_point)," +
                "   geo_shapes array(geo_shape)" +
                ") " +
                "WITH (number_of_replicas = 0)");

            PreparedStatement preparedStatement = conn.prepareStatement(
                "INSERT INTO t (geo_points, geo_shapes) VALUES (?, ?)");
            preparedStatement.setArray(1, conn.createArrayOf(
                "point",
                new PGpoint[]{new PGpoint(1.1, 2.2), new PGpoint(3.3, 4.4)}));
            preparedStatement.setArray(2, conn.createArrayOf(
                "json",
                new Object[]{
                    DataTypes.STRING.implicitCast(
                        Map.of(
                            "coordinates", new double[][]{{0, 0}, {1, 1}},
                            "type", "LineString"
                        )
                    )
                }));
            preparedStatement.executeUpdate();
            conn.createStatement().execute("REFRESH TABLE t");

            ResultSet rs = conn.createStatement().executeQuery("SELECT geo_points, geo_shapes FROM t");
            assertThat(rs.next(), is(true));
            assertThat(
                (Object[]) rs.getArray(1).getArray(),
                arrayContaining(new PGpoint(1.1, 2.2), new PGpoint(3.3, 4.4)));

            var shape = new PGobject();
            shape.setType("json");
            shape.setValue("{\"coordinates\":[[0.0,0.0],[1.0,1.0]],\"type\":\"LineString\"}");
            assertThat((Object[]) rs.getArray(2).getArray(), arrayContaining(shape));
        } catch (BatchUpdateException e) {
            throw e.getNextException();
        }
    }

    @Test
    public void testFetchSize() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
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
    public void testCloseConnectionWithUnfinishedResultSetDoesNotLeaveAnyPendingOperations() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            conn.setAutoCommit(false);
            try (Statement statement = conn.createStatement()) {
                statement.setFetchSize(2);
                try (ResultSet resultSet = statement.executeQuery("select mountain from sys.summits")) {
                    resultSet.next();
                    resultSet.next();
                }
            }
        }

        // The client closes connections lazy, so the statement issued below may arrive on the server *before*
        // the previous connection is closed, so it may see the previous operation -> use assertBusy
        assertBusy(() -> {
            try {
                try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
                    String q =
                        "SELECT j.stmt || '-' || o.name FROM sys.operations AS o, sys.jobs AS j WHERE o.job_id = j.id" +
                        " and j.stmt = ?";
                    PreparedStatement statement = conn.prepareStatement(q);
                    statement.setString(1, "select mountain from sys.summits");
                    ResultSet rs = statement.executeQuery();
                    List<String> operations = new ArrayList<>();
                    while (rs.next()) {
                        operations.add(rs.getString(1));
                    }
                    assertThat(operations, Matchers.empty());
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testCreateNewStatementAfterUnfinishedResultSet() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties);
            Statement statement = conn.createStatement()) {
            conn.setAutoCommit(false);
            statement.setFetchSize(2);
            try (ResultSet resultSet = statement.executeQuery("select mountain from sys.summits")) {
                resultSet.next();
                resultSet.next();
            }
            conn.setAutoCommit(true);
            statement.setFetchSize(0);
            try (ResultSet resultSet = statement.executeQuery("select mountain from sys.summits")) {
                while (resultSet.next()) {
                    assertFalse(resultSet.getString(1).isEmpty());
                }
            }
        }
    }

    @Test
    public void testSelectPreparedStatement() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            conn.setAutoCommit(true);
            PreparedStatement preparedStatement = conn.prepareStatement("select name from sys.cluster");
            ResultSet resultSet = preparedStatement.executeQuery();
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getString(1), Matchers.startsWith("SUITE-CHILD"));
        }
    }

    @Test
    public void testExecuteBatch() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
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
            assertThat(results, is(new int[]{1, 1}));
        }
    }

    @Test
    public void testExecuteBatchWithOneFailingAndNothingExecuted() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("create table t (x int) with (number_of_replicas = 0)");
            PreparedStatement preparedStatement = conn.prepareStatement("insert into t (x) values (cast(? as integer))");
            preparedStatement.setString(1, Integer.toString(1));
            preparedStatement.addBatch();

            preparedStatement.setString(1, "foobar");
            preparedStatement.addBatch();

            preparedStatement.setString(1, Integer.toString(3));
            preparedStatement.addBatch();

            assertThat(preparedStatement.executeBatch(), is(new int[]{0, 0, 0}));

            conn.createStatement().executeUpdate("refresh table t");
            ResultSet rs = conn.createStatement().executeQuery("select count(*) from t");
            assertThat(rs.next(), is(true));
            assertThat(rs.getInt(1), is(0));
        }
    }

    @Test
    public void testExecuteBatchWithOneRuntimeFailure() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
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

            assertThat(preparedStatement.executeBatch(), is(new int[]{1, 1}));
            conn.createStatement().executeUpdate("refresh table t");

            preparedStatement = conn.prepareStatement("update t set x = log(x) where id = ?");
            preparedStatement.setInt(1, 1);
            preparedStatement.addBatch();

            preparedStatement.setInt(1, 2);
            preparedStatement.addBatch();
            assertThat(preparedStatement.executeBatch(), is(new int[]{0, 1}));
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
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            conn.setAutoCommit(true);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("create table t (x int) with (number_of_replicas = 0)");
            ensureYellow();

            Statement statement = conn.createStatement();
            statement.addBatch("insert into t (x) values (1)");
            statement.addBatch("insert into t (x) values (2)");

            int[] results = statement.executeBatch();
            assertThat(results, is(new int[]{1, 1}));

            statement.executeUpdate("refresh table t");
            ResultSet resultSet = statement.executeQuery("select * from t order by x");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getInt(1), is(1));

            // add another batch
            statement.addBatch("insert into t (x) values (3)");

            // only the batches after last execution will be executed
            results = statement.executeBatch();
            assertThat(results, is(new int[]{1}));

            statement.executeUpdate("refresh table t");
            resultSet = statement.executeQuery("select * from t order by x desc");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getInt(1), is(3));
        }
    }

    @Test
    public void test_execute_batch_fails_with_read_operations() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            conn.setAutoCommit(true);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("create table t (x int) with (number_of_replicas = 0)");
            Statement statement = conn.createStatement();
            statement.addBatch("insert into t(x) values(1), (2)");
            statement.addBatch("refresh table t");
            statement.addBatch("select count(*) from t");

            expectedException.expect(BatchUpdateException.class);
            expectedException.expectMessage("Only write operations are allowed in Batch statements");
            statement.executeBatch();
        }
    }

    @Test
    public void testCreateInsertSelectStringAndTimestamp() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            conn.setAutoCommit(true);
            Statement statement = conn.createStatement();
            assertThat(statement.executeUpdate(
                "create table t (" +
                "   x string," +
                "   ts timestamp with time zone" +
                ") with (number_of_replicas = 0)"), is(1));
            ensureYellow();

            assertThat(statement.executeUpdate("insert into t (x, ts) values ('Marvin', '2016-05-14'), ('Trillian', '2016-06-28')"), is(2));
            assertThat(statement.executeUpdate("refresh table t"), is(1));

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
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
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
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            conn.setAutoCommit(true);
            PreparedStatement stmt = conn.prepareStatement("select name fro sys.cluster");
            expectedException.expect(PSQLException.class);
            expectedException.expectMessage("mismatched input 'sys'");
            stmt.executeQuery();
        }
    }

    @Test
    public void testStatementReadOnlyFailure() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RO), properties)) {
            conn.setAutoCommit(true);
            PreparedStatement stmt = conn.prepareStatement("create table test(a integer)");
            expectedException.expect(PSQLException.class);
            expectedException.expectMessage("Only read operations allowed on this node");
            stmt.executeQuery();
        }
    }

    @Test
    public void testErrorRecoveryFromErrorsOutsideSqlOperations() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            conn.setAutoCommit(true);
            PreparedStatement stmt = conn.prepareStatement("select cast([10.3, 20.2] as integer) " +
                                                           "from information_schema.tables");
            try {
                stmt.executeQuery();
                fail("Should've raised PSQLException");
            } catch (PSQLException e) {
                assertThat(e.getMessage(), Matchers.containsString("Cannot cast expressions from type `double precision_array` to type `integer`"));
            }

            assertSelectNameFromSysClusterWorks(conn);
        }
    }

    @Test
    public void testErrorDetailsFromStackTraceInErrorResponse() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("select sqrt('abcd') from sys.cluster");
        } catch (PSQLException e) {
            assertThat(e.getServerErrorMessage().getFile(), not(is(emptyOrNullString())));
            assertThat(e.getServerErrorMessage().getRoutine(), not(is(emptyOrNullString())));
            assertThat(e.getServerErrorMessage().getLine(), greaterThan(0));
        }
    }

    @Test
    public void testGetPostgresPort() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            ResultSet resultSet = conn.createStatement().executeQuery("select port['psql'] from sys.nodes limit 1");
            assertThat(resultSet.next(), is(true));
            Integer port = resultSet.getInt(1);

            ArrayList<Integer> actualPorts = new ArrayList<>();
            for (PostgresNetty postgresNetty : internalCluster().getInstances(PostgresNetty.class)) {
                actualPorts.add(postgresNetty.boundAddress().publishAddress().getPort());
            }
            assertThat(port, Matchers.isOneOf(actualPorts.toArray(new Integer[0])));
        }
    }

    @Test
    public void testSetSchemaOnSession() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            conn.setAutoCommit(true);

            conn.createStatement().execute("set session search_path to bar");
            conn.createStatement().executeUpdate("create table foo (id int) with (number_of_replicas=0)");

            conn.createStatement().execute("set session search_path to DEFAULT");
            assertThrows(() -> conn.createStatement().execute("select * from foo"),
                         isPGError(is("Relation 'foo' unknown"), INTERNAL_ERROR));
        }
    }

    @Test
    public void testSetMultipleSchemasOnSession() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            conn.setAutoCommit(true);

            conn.createStatement().execute("set session search_path to bar ,custom");
            conn.createStatement().executeUpdate("create table foo (id int) with (number_of_replicas=0)");
            conn.createStatement().executeQuery("select * from bar.foo");

            assertThrows(() -> conn.createStatement().execute("select * from custom.foo"),
                         isPGError(is("Schema 'custom' unknown"), INTERNAL_ERROR));
        }
    }

    @Test
    public void testCountDistinctFromJoin() throws Exception {
        // Regression test for a bug where the Postgres-ResultReceiver received types which were a view on symbols which
        // were mutated during analysis/planning of a join - due to that the types changed which led to a ClassCastException
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            ResultSet resultSet = conn.createStatement().executeQuery(
                "select count (distinct 74) from unnest([1, 2]) t1 cross join unnest([2, 3]) t2");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getLong(1), is(1L));
        }
    }

    @Test
    public void testMultiStatementQuery() throws Exception {
        Properties properties = new Properties(this.properties);
        // multi query support is only available in simple query mode
        properties.setProperty("preferQueryMode", "simple");
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            Statement statement = conn.createStatement();
            statement.execute("set search_path to 'hoschi';" +
                              "create table t (id int);" +
                              "insert into t values (42);" +
                              "refresh table t");
            statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from hoschi.t");
            resultSet.next();
            assertThat(resultSet.getInt(1), is(42));
            assertThat(resultSet.isLast(), is(true));
        }
    }

    @Test
    public void testRepeatedFetchDoesNotLeakSysJobsLog() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW))) {
            conn.prepareStatement(
                "create table t (" +
                "   x int, ts timestamp with time zone" +
                ") clustered into 1 shards " +
                "with (number_of_replicas = 0)").execute();
            PreparedStatement preparedStatement = conn.prepareStatement("insert into t (x, ts) values (?, ?)");
            for (int i = 0; i < 20; i++) {
                preparedStatement.setInt(1, i);
                preparedStatement.setLong(2, 1541548800000L);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            conn.prepareStatement("refresh table t").execute();

            conn.setAutoCommit(false);
            PreparedStatement stmt = conn.prepareStatement("SELECT x FROM t WHERE ts > ? ORDER BY ts ASC LIMIT 5");
            for (int i = 0; i < 5; i++) {
                stmt.setInt(1, i);
                stmt.setFetchSize(5);
                ResultSet resultSet = stmt.executeQuery();
                while (resultSet.next()) {
                    resultSet.getInt(1);
                }
            }
        }
        for (JobsLogService jobsLogService : internalCluster().getDataNodeInstances(JobsLogService.class)) {
            assertBusy(() -> assertThat(jobsLogService.get().activeJobs(), emptyIterable()));
        }
    }

    @Test
    public void test_insert_with_on_conflict_do_nothing_batch_error_resp_is_0_for_conflicting_items() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW))) {
            conn.prepareStatement("create table t (id int primary key) clustered into 1 shards").execute();
            conn.prepareStatement("insert into t (id) (select col1 from generate_series(1, 3))").execute();

            PreparedStatement stmt = conn.prepareStatement("insert into t (id) values (?) on conflict (id) do nothing");
            stmt.setInt(1, 4);
            stmt.addBatch();
            stmt.setInt(1, 1);
            stmt.addBatch();

            int[] result = stmt.executeBatch();
            assertThat(result.length, is(2));
            assertThat(result[0], is(1));
            assertThat(result[1], is(0));
        }
    }

    @Test
    public void test_all_system_columns_can_be_queried_via_postgres() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW))) {
            // Create a table to also have some shards and sys.allocations, sys.heatlh, sys.shards, etc are not empty.
            conn.prepareStatement("create table tbl (x int)").execute();

            ResultSet result = conn.prepareStatement(
                "SELECT " +
                "   table_schema, " +
                "   table_name," +
                "   collect_set(column_name) as columns " +
                "FROM " +
                "   information_schema.columns " +
                "WHERE " +
                "   table_schema IN ('sys', 'pg_catalog', 'information_schema') " +
                "GROUP BY " +
                "   1, 2").executeQuery();
            while (result.next()) {
                String schema = result.getString(1);
                String table = result.getString(2);

                Array columns = result.getArray(3);
                StringJoiner columnList = new StringJoiner(", ");
                for (String column : ((String[]) columns.getArray())) {
                    if (column.contains("[")) {
                        columnList.add(column);
                    } else {
                        columnList.add('"' + column + '"');
                    }
                }
                String statement = String.format(
                    Locale.ENGLISH,
                    "SELECT %s FROM \"%s\".\"%s\"",
                    columnList.toString(),
                    schema,
                    table
                );
                // This must not throw a ClassCastException
                conn.prepareStatement(statement).executeQuery();
            }
        }
    }

    @Test
    @UseJdbc(0) // Simulate explicit call by a user through HTTP iface
    public void test_proper_termination_of_deallocate_through_http_call() throws Exception {
       execute("DEALLOCATE ALL");
       assertThat(response.rowCount(), is (0L));
    }

    @Test
    public void test_getProcedureColumns() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW))) {
            var results = conn.getMetaData().getProcedureColumns("", "", "", "");
            assertThat(results.next(), is(true));
            assertThat(results.getString(3), is("_pg_expandarray"));
        }
    }

    @Test
    public void test_each_non_array_pg_type_entry_can_be_joined_with_pg_proc() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW))) {
            PreparedStatement stmt = conn.prepareStatement(
                "SELECT pg_type.oid, pg_type.typname, pg_proc.proname " +
                "FROM pg_catalog.pg_type LEFT OUTER JOIN pg_catalog.pg_proc ON pg_proc.oid = pg_type.typreceive " +
                "WHERE pg_type.typarray <> 0");
            ResultSet result = stmt.executeQuery();
            while (result.next()) {
                assertThat(
                    "There must be an entry for `" + result.getInt(1) + "/" + result.getString(2) + "` in pg_proc",
                    result.getString(3),
                    Matchers.notNullValue(String.class)
                );
            }
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
