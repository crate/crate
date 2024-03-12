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

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.protocols.postgres.PGErrorStatus.UNDEFINED_TABLE;
import static io.crate.protocols.postgres.PostgresNetty.PSQL_PORT_SETTING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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
import java.util.function.Function;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.postgresql.PGProperty;
import org.postgresql.geometric.PGpoint;
import org.postgresql.jdbc.PreferQueryMode;
import org.postgresql.util.PGobject;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import io.crate.action.sql.Sessions;
import io.crate.execution.engine.collect.stats.JobsLogService;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.protocols.postgres.PostgresNetty;
import io.crate.testing.Asserts;
import io.crate.testing.DataTypeTesting;
import io.crate.testing.UseJdbc;
import io.crate.types.DataTypes;

@IntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
public class PostgresITest extends IntegTestCase {

    private static final String NO_IPV6 = "CRATE_TESTS_NO_IPV6";
    private static final String RO = "node_s0";
    private static final String RW = "node_s1";

    private final Properties properties = new Properties();
    private static boolean useIPv6;

    @BeforeClass
    public static void setupClass() {
        useIPv6 = randomBoolean() && !"true".equalsIgnoreCase(System.getenv(NO_IPV6));
    }

    private String url(String nodeName) {
        PostgresNetty postgresNetty = cluster().getInstance(PostgresNetty.class, nodeName);
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
            builder.put(Sessions.NODE_READ_ONLY_SETTING.getKey(), true);
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
        properties.setProperty("user", "crate");
    }

    @Test
    public void testGetTransactionIsolation() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            int var = conn.getTransactionIsolation();
            assertThat(var).isEqualTo(Connection.TRANSACTION_READ_UNCOMMITTED);
        }
    }

    @Test
    public void test_simple_mode_can_handle_npgsql_startup_query() throws Exception {
        String query = """
            SELECT version();
            SELECT ns.nspname, t.oid, t.typname, t.typtype, t.typnotnull, t.elemtypoid
            FROM (
                -- Arrays have typtype=b - this subquery identifies them by their typreceive and converts their typtype to a
                -- We first do this for the type (innerest-most subquery), and then for its element type
                -- This also returns the array element, range subtype and domain base type as elemtypoid
                SELECT
                    typ.oid, typ.typnamespace, typ.typname, typ.typtype, typ.typrelid, typ.typnotnull, typ.relkind,
                    elemtyp.oid AS elemtypoid, elemtyp.typname AS elemtypname, elemcls.relkind AS elemrelkind,
                    CASE WHEN elemproc.proname='array_recv' THEN 'a' ELSE elemtyp.typtype END AS elemtyptype
                FROM (
                    SELECT typ.oid, typnamespace, typname, typrelid, typnotnull, relkind, typelem AS elemoid,
                        CASE WHEN proc.proname='array_recv' THEN 'a' ELSE typ.typtype END AS typtype,
                        CASE
                            WHEN proc.proname='array_recv' THEN typ.typelem
                            WHEN typ.typtype='r' THEN rngsubtype

                            WHEN typ.typtype='d' THEN typ.typbasetype
                        END AS elemtypoid
                    FROM pg_type AS typ
                    LEFT JOIN pg_class AS cls ON (cls.oid = typ.typrelid)
                    LEFT JOIN pg_proc AS proc ON proc.oid = typ.typreceive
                    LEFT JOIN pg_range ON (pg_range.rngtypid = typ.oid)
                ) AS typ
                LEFT JOIN pg_type AS elemtyp ON elemtyp.oid = elemtypoid
                LEFT JOIN pg_class AS elemcls ON (elemcls.oid = elemtyp.typrelid)
                LEFT JOIN pg_proc AS elemproc ON elemproc.oid = elemtyp.typreceive
            ) AS t
            JOIN pg_namespace AS ns ON (ns.oid = typnamespace)
            WHERE
                typtype IN ('b', 'r', 'm', 'e', 'd') OR -- Base, range, multirange, enum, domain
                (typtype = 'c' AND relkind='c') OR -- User-defined free-standing composites (not table composites) by default
                (typtype = 'p' AND typname IN ('record', 'void')) OR -- Some special supported pseudo-types
                (typtype = 'a' AND (  -- Array of...
                    elemtyptype IN ('b', 'r', 'm', 'e', 'd') OR -- Array of base, range, multirange, enum, domain
                    (elemtyptype = 'p' AND elemtypname IN ('record', 'void')) OR -- Arrays of special supported pseudo-types
                    (elemtyptype = 'c' AND elemrelkind='c') -- Array of user-defined free-standing composites (not table composites) by default
                ))
            ORDER BY CASE
                   WHEN typtype IN ('b', 'e', 'p') THEN 0           -- First base types, enums, pseudo-types
                   WHEN typtype = 'r' THEN 1                        -- Ranges after
                   WHEN typtype = 'm' THEN 2                        -- Multiranges after
                   WHEN typtype = 'c' THEN 3                        -- Composites after
                   WHEN typtype = 'd' AND elemtyptype <> 'a' THEN 4 -- Domains over non-arrays after
                   WHEN typtype = 'a' THEN 5                        -- Arrays after
                   WHEN typtype = 'd' AND elemtyptype = 'a' THEN 6  -- Domains over arrays last
            END;

            -- Load field definitions for (free-standing) composite types
            SELECT typ.oid, att.attname, att.atttypid
            FROM pg_type AS typ
            JOIN pg_namespace AS ns ON (ns.oid = typ.typnamespace)
            JOIN pg_class AS cls ON (cls.oid = typ.typrelid)
            JOIN pg_attribute AS att ON (att.attrelid = typ.typrelid)
            WHERE
              (typ.typtype = 'c' AND cls.relkind='c') AND
              attnum > 0 AND     -- Don't load system attributes
              NOT attisdropped
            ORDER BY typ.oid, att.attnum;

            -- Load enum fields
            SELECT pg_type.oid, enumlabel
            FROM pg_enum
            JOIN pg_type ON pg_type.oid=enumtypid
            ORDER BY oid, enumsortorder;
            """;

        Properties properties = new Properties();
        properties.setProperty("user", "crate");
        properties.setProperty(PGProperty.PREFER_QUERY_MODE.getName(), PreferQueryMode.SIMPLE.value());
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            Statement statement = conn.createStatement();
            statement.execute(query);
            ResultSet resultSet = statement.getResultSet();
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getString(1)).contains("CrateDB " + Version.CURRENT.toString());
            assertThat(statement.getMoreResults()).isTrue();
        }
    }

    @Test
    public void testMultidimensionalArrayWithDifferentSizedArrays() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", "crate");
        properties.setProperty(PGProperty.PREFER_QUERY_MODE.getName(), PreferQueryMode.SIMPLE.value());
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            Statement statement = conn.createStatement();
            statement.executeUpdate("create table t (o1 array(object as (o2 array(object as (x int)))))");
            ensureYellow();

            statement.setEscapeProcessing(false);
            statement.executeUpdate("insert into t (o1) values ( [ {o2=[{x=1}, {x=2}]}, {o2=[{x=3}]} ] )");
            statement.executeUpdate("refresh table t");

            ResultSet resultSet = statement.executeQuery("select o1['o2']['x'] from t");
            assertThat(resultSet.next()).isTrue();
            String array = resultSet.getString(1);
            assertThat(array).isEqualTo("[[1,2],[3]]");
        }

        properties = new Properties();
        properties.setProperty("user", "crate");
        properties.setProperty("prepareThreshold", "-1"); // force binary transfer
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            ResultSet resultSet = conn.createStatement().executeQuery("select o1['o2']['x'] from t");
            assertThat(resultSet.next()).isTrue();
            String array = resultSet.getString(1);
            assertThat(array).isEqualTo("[[1,2],[3]]");
        }
    }

    @Test
    public void testUseOfUnsupportedType() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            PreparedStatement stmt = conn.prepareStatement("select ? from sys.cluster");
            stmt.setObject(1, UUID.randomUUID());
            Asserts.assertSQLError(() -> stmt.executeQuery())
                .isExactlyInstanceOf(PSQLException.class)
                .hasPGError(INTERNAL_ERROR)
                .hasMessageContaining("Can't map PGType with oid=2950 to Crate type");
        }
    }

    @Test
    public void testEmptyStatement() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            assertThat(conn.createStatement().execute("")).isFalse();

            assertThatThrownBy(() -> conn.createStatement().executeQuery(""))
                .as("executeQuery with empty query should throw a 'No results were returned by the query' error")
                 // can't use expectedException.expectMessage because error messages are localized and locale is randomized
                .isExactlyInstanceOf(PSQLException.class)
                .extracting((Function<Throwable, String>) t -> ((PSQLException) t).getSQLState())
                .isEqualTo(PSQLState.NO_DATA.getState());
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
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getInt(1)).isEqualTo(1);
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getInt(1)).isEqualTo(2);
        }
    }

    @Test
    public void testPreparedStatementHandling() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", "crate");
        properties.setProperty("prepareThreshold", "-1");
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            PreparedStatement p1 = conn.prepareStatement("select 1 from sys.cluster");
            ResultSet resultSet = p1.executeQuery();
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getInt(1)).isEqualTo(1);

            PreparedStatement p2 = conn.prepareStatement("select 2 from sys.cluster");
            resultSet = p2.executeQuery();
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getInt(1)).isEqualTo(2);

            resultSet = p1.executeQuery();
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getInt(1)).isEqualTo(1);
        }
    }

    @Test
    public void testPreparedSelectStatementWithParametersCanBeDescribed() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", "crate");
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
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getInt(1)).isEqualTo(20);
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
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.next()).isTrue();
            }
        }
    }

    @Test
    public void testNoAutoCommit() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            conn.setAutoCommit(false);
            ResultSet resultSet = conn.prepareStatement("select name from sys.cluster").executeQuery();
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getString(1)).startsWith("SUITE-TEST_WORKER");
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
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getArray(1).getArray()).isEqualTo(new String[]{"99", "51"});
            assertThat(resultSet.getArray(2).getArray()).isEqualTo(new String[]{"fo,o", "bar"});
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
            assertThat(rs.next()).isTrue();
            assertThat(rs.getArray(1).getArray()).isEqualTo(new Integer[]{10, 20});
            assertThat(rs.getArray(2).getArray()).isEqualTo(new Float[]{1.2f, 3.5f});
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
            assertThat(rs.next()).isTrue();
            assertThat((Object[]) rs.getArray(1).getArray())
                .containsExactly(new PGpoint(1.1, 2.2), new PGpoint(3.3, 4.4));

            var shapeObject = new PGobject();
            shapeObject.setType("json");
            String shape = "{\"coordinates\":[[0.0,0.0],[1.0,1.0]],\"type\":\"LineString\"}";
            shapeObject.setValue(shape);
            assertThat((Object[]) rs.getArray(2).getArray()).containsExactly(shape);
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
                    assertThat(result).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19);
                }
            }
        }
    }

    @Test
    public void test_query_inbetween_suspended_fetch_operation() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            conn.createStatement().executeUpdate("create table t (x int) with (number_of_replicas = 0)");
            PreparedStatement preparedStatement = conn.prepareStatement("insert into t (x) values (?)");
            for (int i = 0; i < 6; i++) {
                preparedStatement.setInt(1, i);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();

            conn.createStatement().executeUpdate("refresh table t");
            conn.setAutoCommit(false);
            try (Statement st = conn.createStatement()) {
                st.setFetchSize(2);
                try (ResultSet xResults = st.executeQuery("select x from t")) {
                    List<Integer> result = new ArrayList<>();
                    while (xResults.next()) {
                        if (result.size() == 3) {
                            var select30Result = conn.createStatement().executeQuery("select 30");
                            assertThat(select30Result.next()).isTrue();
                            assertThat(select30Result.getInt(1)).isEqualTo(30);
                        }
                        result.add(xResults.getInt(1));
                    }
                    assertThat(result).containsExactlyInAnyOrder(0, 1, 2, 3, 4, 5);
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
                    assertThat(operations).isEmpty();
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
                    assertThat(resultSet.getString(1)).isNotEmpty();
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
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getString(1)).startsWith("SUITE-TEST_WORKER");
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
            assertThat(results).isEqualTo(new int[]{1, 1});
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

            assertThat(preparedStatement.executeBatch()).isEqualTo(new int[]{0, 0, 0});

            conn.createStatement().executeUpdate("refresh table t");
            ResultSet rs = conn.createStatement().executeQuery("select count(*) from t");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(0);
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

            assertThat(preparedStatement.executeBatch()).isEqualTo(new int[]{1, 1});
            conn.createStatement().executeUpdate("refresh table t");

            preparedStatement = conn.prepareStatement("update t set x = log(x) where id = ?");
            preparedStatement.setInt(1, 1);
            preparedStatement.addBatch();

            preparedStatement.setInt(1, 2);
            preparedStatement.addBatch();
            assertThat(preparedStatement.executeBatch()).isEqualTo(new int[]{0, 1});
            conn.createStatement().executeUpdate("refresh table t");

            ResultSet rs = conn.createStatement().executeQuery("select x from t order by id");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(0); // log(0) is an error - the update failed and the value remains unchanged

            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(1); // log(10) -> 1
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
            statement.addBatch("insert into t (x) values (2), (3)");

            int[] results = statement.executeBatch();
            assertThat(results).isEqualTo(new int[]{1, 2});

            statement.executeUpdate("refresh table t");
            ResultSet resultSet = statement.executeQuery("select * from t order by x");
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getInt(1)).isEqualTo(1);

            // add another batch
            statement.addBatch("insert into t (x) values (3)");

            // only the batches after last execution will be executed
            results = statement.executeBatch();
            assertThat(results).isEqualTo(new int[]{1});

            statement.executeUpdate("refresh table t");
            resultSet = statement.executeQuery("select * from t order by x desc");
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getInt(1)).isEqualTo(3);
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

            Assertions.assertThrows(BatchUpdateException.class,
                                    () -> statement.executeBatch(),
                                    "Only write operations are allowed in Batch statements"
            );
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
                ") with (number_of_replicas = 0)")).isEqualTo(1);
            ensureYellow();

            assertThat(statement.executeUpdate("insert into t (x, ts) values ('Marvin', '2016-05-14'), ('Trillian', '2016-06-28')")).isEqualTo(2);
            assertThat(statement.executeUpdate("refresh table t")).isEqualTo(1);

            statement.executeQuery("select x, ts from t order by x");
            ResultSet resultSet = statement.getResultSet();
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getString(1)).isEqualTo("Marvin");
            assertThat(resultSet.getTimestamp(2)).isEqualTo(new Timestamp(1463184000000L));

            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getString(1)).isEqualTo("Trillian");
            assertThat(resultSet.getTimestamp(2)).isEqualTo(new Timestamp(1467072000000L));
        }
    }

    @Test
    public void testSelectWithParameters() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            conn.setAutoCommit(true);
            PreparedStatement preparedStatement = conn.prepareStatement("select name from sys.cluster where name like ?");
            preparedStatement.setString(1, "SUITE%");
            ResultSet resultSet = preparedStatement.executeQuery();
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getString(1)).startsWith("SUITE-TEST_WORKER");
        }
    }

    @Test
    public void testStatementThatResultsInParseError() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            conn.setAutoCommit(true);
            PreparedStatement stmt = conn.prepareStatement("select name fro sys.cluster");
            Asserts.assertSQLError(() -> stmt.executeQuery())
                .isExactlyInstanceOf(PSQLException.class)
                .hasPGError(INTERNAL_ERROR)
                .hasMessageContaining("mismatched input 'sys'");
        }
    }

    @Test
    public void testStatementReadOnlyFailure() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RO), properties)) {
            conn.setAutoCommit(true);
            PreparedStatement stmt = conn.prepareStatement("create table test(a integer)");
            Asserts.assertSQLError(() -> stmt.executeQuery())
                .isExactlyInstanceOf(PSQLException.class)
                .hasPGError(INTERNAL_ERROR)
                .hasMessageContaining("Only read operations allowed on this node");
        }
    }

    @Test
    public void testErrorRecoveryFromErrorsOutsideSqlOperations() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            conn.setAutoCommit(true);
            PreparedStatement stmt = conn.prepareStatement("select cast([10.3, 20.2] as integer) " +
                                                           "from information_schema.tables");
            assertThatThrownBy(() -> stmt.executeQuery())
                .isExactlyInstanceOf(PSQLException.class)
                .hasMessageContaining("Cannot cast expressions from type `double precision_array` to type `integer`");

            assertSelectNameFromSysClusterWorks(conn);
        }
    }

    @Test
    public void testErrorDetailsFromStackTraceInErrorResponse() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("select sqrt('abcd') from sys.cluster");
        } catch (PSQLException e) {
            assertThat(e.getServerErrorMessage().getFile()).isNotEmpty();
            assertThat(e.getServerErrorMessage().getRoutine()).isNotEmpty();
            assertThat(e.getServerErrorMessage().getLine()).isGreaterThan(0);
        }
    }

    @Test
    public void testGetPostgresPort() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            ResultSet resultSet = conn.createStatement().executeQuery("select port['psql'] from sys.nodes limit 1");
            assertThat(resultSet.next()).isTrue();
            Integer port = resultSet.getInt(1);

            ArrayList<Integer> actualPorts = new ArrayList<>();
            for (PostgresNetty postgresNetty : cluster().getInstances(PostgresNetty.class)) {
                actualPorts.add(postgresNetty.boundAddress().publishAddress().getPort());
            }
            assertThat(port).isIn(actualPorts);
        }
    }

    @Test
    public void testSetSchemaOnSession() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            conn.setAutoCommit(true);

            conn.createStatement().execute("set session search_path to bar");
            conn.createStatement().executeUpdate("create table foo (id int) with (number_of_replicas=0)");

            conn.createStatement().execute("set session search_path to DEFAULT");
            Asserts.assertSQLError(() -> conn.createStatement().execute("select * from foo"))
                .isExactlyInstanceOf(PSQLException.class)
                .hasPGError(UNDEFINED_TABLE)
                .hasMessageContaining("Relation 'foo' unknown");
        }
    }

    @Test
    public void testSetMultipleSchemasOnSession() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            conn.setAutoCommit(true);

            conn.createStatement().execute("set session search_path to bar ,custom");
            conn.createStatement().executeUpdate("create table foo (id int) with (number_of_replicas=0)");
            conn.createStatement().executeQuery("select * from bar.foo");

            Asserts.assertSQLError(() -> conn.createStatement().execute("select * from custom.foo"))
                .isExactlyInstanceOf(PSQLException.class)
                .hasPGError(INTERNAL_ERROR)
                .hasMessageContaining("Schema 'custom' unknown");
        }
    }

    @Test
    public void testCountDistinctFromJoin() throws Exception {
        // Regression test for a bug where the Postgres-ResultReceiver received types which were a view on symbols which
        // were mutated during analysis/planning of a join - due to that the types changed which led to a ClassCastException
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            ResultSet resultSet = conn.createStatement().executeQuery(
                "select count (distinct 74) from unnest([1, 2]) t1 cross join unnest([2, 3]) t2");
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getLong(1)).isEqualTo(1L);
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
            assertThat(resultSet.getInt(1)).isEqualTo(42);
            assertThat(resultSet.isLast()).isTrue();
        }
    }

    @Test
    public void testRepeatedFetchDoesNotLeakSysJobsLog() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
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
        for (JobsLogService jobsLogService : cluster().getDataNodeInstances(JobsLogService.class)) {
            assertBusy(() -> assertThat(jobsLogService.get().activeJobs()).isEmpty());
        }
    }

    @Test
    public void test_insert_with_on_conflict_do_nothing_batch_error_resp_is_0_for_conflicting_items() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            conn.prepareStatement("create table t (id int primary key) clustered into 1 shards").execute();
            conn.prepareStatement("insert into t (id) (select generate_series from generate_series(1, 3))").execute();

            PreparedStatement stmt = conn.prepareStatement("insert into t (id) values (?) on conflict (id) do nothing");
            stmt.setInt(1, 4);
            stmt.addBatch();
            stmt.setInt(1, 1);
            stmt.addBatch();

            int[] result = stmt.executeBatch();
            assertThat(result.length).isEqualTo(2);
            assertThat(result[0]).isEqualTo(1);
            assertThat(result[1]).isEqualTo(0);
        }
    }

    @Test
    public void test_all_system_columns_can_be_queried_via_postgres() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
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
        assertThat(response.rowCount()).isEqualTo(0L);
    }

    @Test
    public void test_getProcedureColumns() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            var results = conn.getMetaData().getProcedureColumns("", "", "", "");
            assertThat(results.next()).isTrue();
            assertThat(results.getString(3)).isEqualTo("_pg_expandarray");
        }
    }

    @Test
    public void test_each_non_array_pg_type_entry_can_be_joined_with_pg_proc() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            PreparedStatement stmt = conn.prepareStatement(
                "SELECT pg_type.oid, pg_type.typname, pg_proc.proname " +
                "FROM pg_catalog.pg_type LEFT OUTER JOIN pg_catalog.pg_proc ON pg_proc.oid = pg_type.typreceive " +
                "WHERE pg_type.typarray <> 0");
            ResultSet result = stmt.executeQuery();
            while (result.next()) {
                assertThat(result.getString(3))
                    .as("There must be an entry for `" + result.getInt(1) + "/" + result.getString(2) + "` in pg_proc")
                    .isNotNull();
            }
        }
    }

    @Test
    public void test_parse_failures_in_simple_protocol_mode_are_propagated_to_client() throws Exception {
        // regression test; used to get stuck
        Properties properties = new Properties();
        properties.setProperty("user", "crate");
        properties.setProperty(PGProperty.PREFER_QUERY_MODE.getName(), PreferQueryMode.SIMPLE.value());
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            Statement statement = conn.createStatement();
            assertThatThrownBy(() -> statement.execute("create index invalid_statement"))
                .isExactlyInstanceOf(PSQLException.class);
            ResultSet result = statement.executeQuery("select 1");
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(1);
        }
    }

    @Test
    public void test_insert_from_unnest_returning_which_inserts_duplicate_keys() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", "crate");
        try (var conn = DriverManager.getConnection(url(RW), properties)) {
            var statement = conn.createStatement();
            statement.execute("create table tbl (id bigint primary key, val text)");
            var textGenerator = DataTypeTesting.getDataGenerator(DataTypes.STRING);
            var insertUnnest = conn.prepareStatement(
                "insert into tbl (id, val) (select id, val from unnest(?, ?) as t (id, val)) returning _id");

            for (int iteration = 0; iteration < 2; iteration++) {
                var ids = new Object[10];
                var values = new Object[10];
                for (int i = 0; i < 10; i++) {
                    ids[i] = i;
                    values[i] = textGenerator.get();
                }
                insertUnnest.setObject(1, conn.createArrayOf("bigint", ids));
                insertUnnest.setObject(2, conn.createArrayOf("text", values));
                var result = insertUnnest.executeQuery();
                int numResults = 0;
                while (result.next()) {
                    numResults++;
                }
                if (iteration == 0) {
                    assertThat(numResults).isEqualTo(10);
                } else {
                    assertThat(numResults).isEqualTo(0);
                }
            }
        }
    }

    @Test
    public void test_metadata_calls_do_not_query_pg_catalog_tables_repeatedly() throws Exception {
        var properties = new Properties();
        properties.put("user", "crate");
        properties.put("password", "");
        try (var conn = DriverManager.getConnection(url(RW), properties)) {
            var stmt = conn.createStatement();
            stmt.execute("create table uservisits (\"adRevenue\" float, \"cCode\" text, \"duration\" float)");

            conn.setAutoCommit(false);
            stmt = conn.createStatement();
            stmt.setFetchSize(101);
            ResultSet result = stmt.executeQuery(
                    "select avg(\"adRevenue\") from uservisits group by \"cCode\", \"duration\" limit 100");

            long numQueries = getNumQueriesFromJobsLogs();
            ResultSetMetaData metaData = result.getMetaData();
            for (int i = 0; i < 50; i++) {
                // These calls must not invoke extra queries
                metaData.isAutoIncrement(1);
            }
            assertThat(getNumQueriesFromJobsLogs()).isEqualTo(numQueries);
            result.close();
        }
    }

    @Test
    public void test_table_function_without_from_can_bind_parameters() throws Exception {
        // https://github.com/crate/crate/issues/12442
        Properties properties = new Properties();
        properties.setProperty("user", "crate");
        try (var conn = DriverManager.getConnection(url(RW), properties)) {
            var selectUnnest = conn.prepareStatement("select unnest(?)");
            var toUnnest = new Object[10];
            for (int i = 0; i < 10; i++) {
                toUnnest[i] = i;
            }
            selectUnnest.setArray(1, conn.createArrayOf("bigint", toUnnest));

            var result = selectUnnest.executeQuery();
            int numResults = 0;
            while (result.next()) {
                numResults++;
            }
            assertThat(numResults).isEqualTo(10);
        }
    }

    @Test
    public void test_c_options_in_properties() throws Exception {
        var properties = new Properties();
        properties.setProperty("user", "crate");
        properties.setProperty("options", "-c error_on_unknown_object_key=false -c statement_timeout=60000");
        try (var conn = DriverManager.getConnection(url(RW), properties)) {
            Statement stmt = conn.createStatement();
            ResultSet result = stmt.executeQuery("SHOW error_on_unknown_object_key");
            assertThat(result.next()).isTrue();
            assertThat(result.getBoolean("setting")).isFalse();

            result = stmt.executeQuery("SHOW statement_timeout");
            assertThat(result.next()).isTrue();
            assertThat(result.getString("setting")).isEqualTo("1m");
        }
    }

    @Test
    public void test_insert_array_in_simple_query_mode() throws Exception {
        var properties = new Properties();
        properties.setProperty("user", "crate");
        properties.setProperty("preferQueryMode", "simple");
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            conn.createStatement().executeUpdate(
                    "CREATE TABLE t (" +
                    "   ints array(int)) " +
                    "WITH (number_of_replicas = 0)");

            PreparedStatement preparedStatement = conn.prepareStatement(
                    "INSERT INTO t (ints) VALUES (?)");
            preparedStatement.setArray(1, conn.createArrayOf("int", new Integer[]{1, 2}));
            preparedStatement.executeUpdate();
            conn.createStatement().execute("REFRESH TABLE t");

            ResultSet resultSet = conn.createStatement().executeQuery("SELECT ints FROM t");
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getArray(1).getArray()).isEqualTo(new Integer[]{1, 2});
        } catch (BatchUpdateException e) {
            throw e.getNextException();
        }
    }

    @Test
    public void test_original_query_appears_in_jobs_log() throws Exception {
        var properties = new Properties();
        properties.setProperty("user", "crate");
        properties.setProperty("preferQueryMode", "simple");
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            var stmt = "SET timezone = 'Europe/Berlin'";
            conn.createStatement().execute(stmt);

            // Check that the statement is logged with a WHERE filter to avoid flakiness because of
            // other statements logged like:  "SET extra_float_digits = 3"
            var prepStmt = conn.prepareStatement("SELECT stmt FROM sys.jobs_log WHERE stmt = ?");
            prepStmt.setString(1, stmt);
            var resultSet = prepStmt.executeQuery();
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getString(1)).isEqualTo(stmt);
            assertThat(resultSet.next()).isFalse();
        } catch (BatchUpdateException e) {
            throw e.getNextException();
        }
    }

    @Test
    public void test_float_vector_jdbc() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            conn.createStatement().execute("create table tbl (id int, xs float_vector(2))");
            PreparedStatement stmt = conn.prepareStatement("insert into tbl (id, xs) values (?, ?)");
            stmt.setObject(1, 1);
            stmt.setObject(2, new float[] { 1.2f, 1.3f });
            stmt.executeUpdate();
            stmt.setObject(1, 2);
            stmt.setObject(2, new float[] { 2.2f, 2.3f });
            stmt.executeUpdate();
            stmt.setObject(1, 3);
            stmt.setObject(2, null);
            stmt.executeUpdate();

            conn.createStatement().execute("refresh table tbl");
            var resultSet = conn.createStatement().executeQuery("select xs from tbl order by id");
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getArray(1).getArray()).isEqualTo(new Float[] { 1.2f, 1.3f });
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getArray(1).getArray()).isEqualTo(new Float[] { 2.2f, 2.3f });
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getArray(1)).isNull();
        }
    }

    @Test
    public void test_fetch_with_intermediate_prepared_statement() throws Exception {
        try (Connection conn = DriverManager.getConnection(url(RW), properties)) {
            try (var statement = conn.createStatement()) {
                statement.execute("create table tbl as select * from generate_series(1, 100, 1) g(a)");
                statement.execute("refresh table tbl");
            }
            Statement statement = conn.createStatement();
            statement.setFetchSize(0);
            statement.setMaxRows(20);
            boolean hasResultSet = statement.execute("select * from tbl");
            assertThat(hasResultSet).isTrue();
            ResultSet resultSet = statement.getResultSet();
            resultSet.next();
            try (var pStatement = conn.prepareStatement("SELECT current_schema(), session_user")) {
                try (ResultSet rs = pStatement.executeQuery()) {
                    if (rs.next()) {
                        rs.getString(1);
                        rs.getString(2);
                    }
                }
            }
        }
    }

    @Test
    public void test_bulk_insert_from_select_fail_fast() throws Exception {
        var properties = new Properties();
        properties.setProperty("user", "crate");
        properties.setProperty("options", "-c insert_fail_fast=true");
        try (var conn = DriverManager.getConnection(url(RW), properties)) {
            conn.createStatement().executeUpdate("create table t (a int primary key)");
            ensureGreen();

            PreparedStatement preparedStatement = conn.prepareStatement("insert into t (a) values (?)");
            preparedStatement.setInt(1, 1);
            preparedStatement.addBatch();
            preparedStatement.setInt(1, 1);
            preparedStatement.addBatch();

            assertThatThrownBy(() -> preparedStatement.executeBatch())
                .isExactlyInstanceOf(BatchUpdateException.class)
                .hasMessageContaining("A document with the same primary key exists already");


            // First error encountered is reflected in jobs_log.
            var resultSet = conn.createStatement().executeQuery("""
                SELECT error FROM sys.jobs_log WHERE stmt LIKE 'insert into t (a) values%'
                """);
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getString(1)).contains("version conflict, document already exists");
        }
    }

    @Test
    public void test_bulk_update_by_id_insert_fail_fast_throws() throws Exception {
        var properties = new Properties();
        properties.setProperty("user", "crate");
        properties.setProperty("options", "-c insert_fail_fast=true");

        try (var conn = DriverManager.getConnection(url(RW), properties)) {
            conn.createStatement().executeUpdate("create table t (id int primary key, a int CHECK (a < 100))");
            conn.createStatement().executeUpdate("insert into t (id, a) values (1, 1), (2, 2), (3, 3)");
            conn.createStatement().executeUpdate("refresh table t");

            PreparedStatement preparedStatement = conn.prepareStatement("update t set a = a + 98 where id = ?");
            preparedStatement.setInt(1, 2);
            preparedStatement.addBatch();
            preparedStatement.setInt(1, 3);
            preparedStatement.addBatch();

            assertThatThrownBy(() -> preparedStatement.executeBatch())
                .isExactlyInstanceOf(BatchUpdateException.class)
                .hasMessageContaining("Failed CONSTRAINT");


            // First error encountered is reflected in jobs_log.
            var resultSet = conn.createStatement().executeQuery("""
                SELECT error FROM sys.jobs_log WHERE stmt LIKE 'update t set a = a +%'
                """);
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getString(1)).contains("Failed CONSTRAINT");
        }
    }

    private long getNumQueriesFromJobsLogs() {
        long result = 0;
        Iterable<JobsLogs> jobLogs = cluster().getInstances(JobsLogs.class);
        for (JobsLogs jobsLogs : jobLogs) {
            for (var metrics : jobsLogs.metrics()) {
                result += metrics.totalCount();
            }
        }
        return result;
    }

    private void assertSelectNameFromSysClusterWorks(Connection conn) throws SQLException {
        PreparedStatement stmt;// verify that queries can be made after an error occurred
        stmt = conn.prepareStatement("select name from sys.cluster");
        ResultSet resultSet = stmt.executeQuery();
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getString(1)).startsWith("SUITE-TEST_WORKER");
    }
}
