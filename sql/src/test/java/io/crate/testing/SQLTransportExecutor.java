/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.testing;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.google.common.base.MoreObjects;
import io.crate.action.sql.BaseResultReceiver;
import io.crate.action.sql.Option;
import io.crate.action.sql.ResultReceiver;
import io.crate.action.sql.SQLActionException;
import io.crate.action.sql.SQLOperations;
import io.crate.action.sql.Session;
import io.crate.auth.user.User;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.exceptions.SQLExceptions;
import io.crate.expression.symbol.Field;
import io.crate.metadata.SearchPath;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.protocols.postgres.types.PGType;
import io.crate.protocols.postgres.types.PGTypes;
import io.crate.shade.org.postgresql.geometric.PGpoint;
import io.crate.shade.org.postgresql.util.PGobject;
import io.crate.shade.org.postgresql.util.PSQLException;
import io.crate.shade.org.postgresql.util.ServerErrorMessage;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.support.AdapterActionFuture;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.hamcrest.Matchers;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.shape.impl.PointImpl;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static io.crate.action.sql.Session.UNNAMED;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class SQLTransportExecutor {

    private static final String SQL_REQUEST_TIMEOUT = "CRATE_TESTS_SQL_REQUEST_TIMEOUT";

    public static final TimeValue REQUEST_TIMEOUT = new TimeValue(Long.parseLong(
        MoreObjects.firstNonNull(System.getenv(SQL_REQUEST_TIMEOUT), "5")), TimeUnit.SECONDS);

    private static final Logger LOGGER = LogManager.getLogger(SQLTransportExecutor.class);

    private static final TestExecutionConfig EXECUTION_FEATURES_DISABLED = new TestExecutionConfig(false, false);

    private final ClientProvider clientProvider;

    private SearchPath searchPath = SearchPath.pathWithPGCatalogAndDoc();

    public SQLTransportExecutor(ClientProvider clientProvider) {
        this.clientProvider = clientProvider;
    }

    public String getCurrentSchema() {
        return searchPath.currentSchema();
    }

    public void setSearchPath(String... searchPath) {
        this.searchPath = SearchPath.createSearchPathFrom(searchPath);
    }

    public SQLResponse exec(String statement) {
        return executeTransportOrJdbc(EXECUTION_FEATURES_DISABLED, statement, null, REQUEST_TIMEOUT);
    }

    public SQLResponse exec(TestExecutionConfig config, String statement, Object[] params) {
        return executeTransportOrJdbc(config, statement, params, REQUEST_TIMEOUT);
    }

    public SQLResponse exec(TestExecutionConfig config, String statement, Object[] params, TimeValue timeout) {
        return executeTransportOrJdbc(config, statement, params, timeout);
    }

    public SQLResponse exec(String statement, Object[] params) {
        return executeTransportOrJdbc(EXECUTION_FEATURES_DISABLED, statement, params, REQUEST_TIMEOUT);
    }

    public long[] execBulk(String statement, @Nullable Object[][] bulkArgs) {
        return executeBulk(statement, bulkArgs, REQUEST_TIMEOUT);
    }

    public long[] execBulk(String statement, @Nullable Object[][] bulkArgs, TimeValue timeout) {
        return executeBulk(statement, bulkArgs, timeout);
    }

    private SQLResponse executeTransportOrJdbc(TestExecutionConfig config,
                                               String stmt,
                                               @Nullable Object[] args,
                                               TimeValue timeout) {
        String pgUrl = clientProvider.pgUrl();
        Random random = RandomizedContext.current().getRandom();

        List<String> sessionList = new ArrayList<>();
        sessionList.add("set search_path to "
                        + StreamSupport.stream(searchPath.spliterator(), false)
                            // explicitly setting the pg catalog schema will make it the current schema so attempts to
                            // create un-fully-qualified relations will fail. we filter it out and will implicitly
                            // remain the first in the search path.
                            .filter(s -> !s.equals(PgCatalogSchemaInfo.NAME))
                            .collect(Collectors.joining(", "))
        );

        if (!config.isHashJoinEnabled()) {
            sessionList.add("set enable_hashjoin=false");
            LOGGER.trace("Executing with enable_hashjoin=false: {}", stmt);
        }

        if (pgUrl != null && config.isJdbcEnabled()) {
            LOGGER.trace("Executing with pgJDBC: {}", stmt);
            return executeWithPg(
                stmt,
                args,
                pgUrl,
                random,
                sessionList);
        }
        try {
            if (!sessionList.isEmpty()) {
                try (Session session = newSession()) {
                    sessionList.forEach((setting) -> exec(setting, session));
                    return execute(stmt, args, session).actionGet(timeout);
                }
            }
            return execute(stmt, args).actionGet(timeout);
        } catch (ElasticsearchTimeoutException e) {
            LOGGER.error("Timeout on SQL statement: {} {}", stmt, e);
            throw e;
        }
    }

    public String jdbcUrl() {
        return clientProvider.pgUrl();
    }

    public ActionFuture<SQLResponse> execute(String stmt, @Nullable Object[] args) {
        Session session = newSession();
        ListenableActionFuture<SQLResponse> execute = execute(stmt, args, session);
        execute.addListener(new ActionListener<>() {
            @Override
            public void onResponse(SQLResponse sqlResponse) {
                session.close();
            }

            @Override
            public void onFailure(Exception e) {
                session.close();
            }
        });
        return execute;
    }

    public Session newSession() {
        return clientProvider.sqlOperations().createSession(
            searchPath.currentSchema(),
            User.CRATE_USER,
            Option.NONE
        );
    }

    public SQLResponse exec(String statement, Session session) {
        return execute(statement, null, session).actionGet(REQUEST_TIMEOUT);
    }

    public static ListenableActionFuture<SQLResponse> execute(String stmt,
                                                              @Nullable Object[] args,
                                                              Session session) {
        PlainListenableActionFuture<SQLResponse> actionFuture = PlainListenableActionFuture.newListenableFuture();
        execute(stmt, args, actionFuture, session);
        return actionFuture;
    }

    private static void execute(String stmt,
                                @Nullable Object[] args,
                                ActionListener<SQLResponse> listener,
                                Session session) {
        try {
            session.parse(UNNAMED, stmt, Collections.emptyList());
            List<Object> argsList = args == null ? Collections.emptyList() : Arrays.asList(args);
            session.bind(UNNAMED, UNNAMED, argsList, null);
            List<Field> outputFields = session.describe('P', UNNAMED).getFields();
            if (outputFields == null) {
                ResultReceiver resultReceiver = new RowCountReceiver(listener);
                session.execute(UNNAMED, 0, resultReceiver);
            } else {
                ResultReceiver resultReceiver = new ResultSetReceiver(listener, outputFields);
                session.execute(UNNAMED, 0, resultReceiver);
            }
            session.sync();
        } catch (Throwable t) {
            listener.onFailure(SQLExceptions.createSQLActionException(t, e -> {}));
        }
    }

    private void execute(String stmt, @Nullable Object[][] bulkArgs, final ActionListener<long[]> listener) {
        Session session = newSession();
        try {
            session.parse(UNNAMED, stmt, Collections.emptyList());
            if (bulkArgs == null) {
                bulkArgs = new Object[0][];
            }
            final long[] rowCounts = new long[bulkArgs.length];
            if (rowCounts.length == 0) {
                session.bind(UNNAMED, UNNAMED, Collections.emptyList(), null);
                session.execute(UNNAMED, 0, new BaseResultReceiver());
            } else {
                for (int i = 0; i < bulkArgs.length; i++) {
                    session.bind(UNNAMED, UNNAMED, Arrays.asList(bulkArgs[i]), null);
                    ResultReceiver resultReceiver = new BulkRowCountReceiver(rowCounts, i);
                    session.execute(UNNAMED, 0, resultReceiver);
                }
            }
            List<Field> outputColumns = session.describe('P', UNNAMED).getFields();
            if (outputColumns != null) {
                throw new UnsupportedOperationException(
                    "Bulk operations for statements that return result sets is not supported");
            }
            session.sync().whenComplete((Object result, Throwable t) -> {
                if (t == null) {
                    listener.onResponse(rowCounts);
                } else {
                    listener.onFailure(SQLExceptions.createSQLActionException(t, e -> {}));
                }
                session.close();
            });
        } catch (Throwable t) {
            session.close();
            listener.onFailure(SQLExceptions.createSQLActionException(t, e -> {}));
        }
    }

    private SQLResponse executeWithPg(String stmt,
                                      @Nullable Object[] args,
                                      String pgUrl,
                                      Random random,
                                      List<String> setSessionStatementsList) {
        try {
            Properties properties = new Properties();
            if (random.nextBoolean()) {
                properties.setProperty("prepareThreshold", "-1"); // disable prepared statements
            }
            try (Connection conn = DriverManager.getConnection(pgUrl, properties)) {
                conn.setAutoCommit(true);
                for (String setSessionStmt : setSessionStatementsList) {
                    conn.createStatement().execute(setSessionStmt);
                }
                try (PreparedStatement preparedStatement = conn.prepareStatement(stmt)) {
                    if (args != null) {
                        for (int i = 0; i < args.length; i++) {
                            preparedStatement.setObject(i + 1, toJdbcCompatObject(conn, args[i]));
                        }
                    }
                    return executeAndConvertResult(preparedStatement);
                }
            }
        } catch (PSQLException e) {
            LOGGER.error("Error executing stmt={} args={}", stmt, Arrays.toString(args));
            ServerErrorMessage serverErrorMessage = e.getServerErrorMessage();
            final StackTraceElement[] stacktrace;
            //noinspection ThrowableNotThrown add the test-call-chain to the stack to be able
            // to quickly figure out which statement in a test case led to the error
            StackTraceElement[] traceToExecWithPg = new Exception().getStackTrace();
            if (serverErrorMessage == null) {
                stacktrace = traceToExecWithPg;
            } else {
                stacktrace = new StackTraceElement[traceToExecWithPg.length + 1];
                stacktrace[0] = new StackTraceElement(
                    serverErrorMessage.getFile(),
                    serverErrorMessage.getRoutine(),
                    serverErrorMessage.getFile(),
                    serverErrorMessage.getLine());
                System.arraycopy(traceToExecWithPg, 0, stacktrace, 1, traceToExecWithPg.length);
            }
            throw new SQLActionException(
                e.getMessage(),
                0,
                HttpResponseStatus.BAD_REQUEST,
                stacktrace);
        } catch (SQLException e) {
            throw new SQLActionException(e.getMessage(), 0, HttpResponseStatus.BAD_REQUEST);
        }
    }

    private static Object toJdbcCompatObject(Connection connection, Object arg) {
        if (arg == null) {
            return null;
        }
        if (arg instanceof Map) {
            // setObject with a Map would use hstore. But that only supports text values
            try {
                return toPGObjectJson(toJsonString(((Map) arg)));
            } catch (SQLException | IOException e) {
                throw new RuntimeException(e);
            }
        }
        if (arg.getClass().isArray()) {
            arg = Arrays.asList((Object[]) arg);
        }
        if (arg instanceof Collection) {
            Collection values = (Collection) arg;
            if (values.isEmpty()) {
                return null; // TODO: can't insert empty list without knowing the type
            }

            if (values.iterator().next() instanceof Map) {
                try {
                    return toPGObjectJson(toJsonString(values));
                } catch (SQLException | IOException e) {
                    throw new RuntimeException(e);
                }
            }
            List<Object> convertedValues = new ArrayList<>(values.size());
            PGType pgType = null;
            for (Object value : values) {
                convertedValues.add(toJdbcCompatObject(connection, value));
                if (pgType == null && value != null) {
                    pgType = PGTypes.get(DataTypes.guessType(value));
                }
            }
            try {
                return connection.createArrayOf(pgType.typName(), convertedValues.toArray(new Object[0]));
            } catch (SQLException e) {
                /*
                 * pg error message doesn't include a stacktrace.
                 * Set a breakpoint in {@link io.crate.protocols.postgres.Messages#sendErrorResponse(Channel, Throwable)}
                 * to inspect the error
                 */
                throw new RuntimeException(e);
            }
        }
        return arg;
    }

    private static String toJsonString(Map value) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.map(value);
        builder.close();
        return Strings.toString(builder);
    }

    private static String toJsonString(Collection values) throws IOException {
        XContentBuilder builder;
        builder = JsonXContent.contentBuilder();
        builder.startArray();
        for (Object value : values) {
            builder.value(value);
        }
        builder.endArray();
        builder.close();
        return Strings.toString(builder);
    }

    private static PGobject toPGObjectJson(String json) throws SQLException {
        PGobject pGobject = new PGobject();
        pGobject.setType("json");
        pGobject.setValue(json);
        return pGobject;
    }

    private SQLResponse executeAndConvertResult(PreparedStatement preparedStatement) throws SQLException {
        if (preparedStatement.execute()) {
            ResultSetMetaData metaData = preparedStatement.getMetaData();
            ResultSet resultSet = preparedStatement.getResultSet();
            List<Object[]> rows = new ArrayList<>();
            List<String> columnNames = new ArrayList<>(metaData.getColumnCount());
            DataType[] dataTypes = new DataType[metaData.getColumnCount()];
            for (int i = 0; i < metaData.getColumnCount(); i++) {
                columnNames.add(metaData.getColumnName(i + 1));
            }
            while (resultSet.next()) {
                Object[] row = new Object[metaData.getColumnCount()];
                for (int i = 0; i < row.length; i++) {
                    Object value;
                    String typeName = metaData.getColumnTypeName(i + 1);
                    value = getObject(resultSet, i, typeName);
                    row[i] = value;
                }
                rows.add(row);
            }
            return new SQLResponse(
                columnNames.toArray(new String[0]),
                rows.toArray(new Object[0][]),
                dataTypes,
                rows.size()
            );
        } else {
            int updateCount = preparedStatement.getUpdateCount();
            if (updateCount < 0) {
                /*
                 * In Crate -1 means row-count unknown, and -2 means error. In JDBC -2 means row-count unknown and -3 means error.
                 * See {@link java.sql.Statement#EXECUTE_FAILED}
                 */
                updateCount += 1;
            }
            return new SQLResponse(
                new String[0],
                new Object[0][],
                new DataType[0],
                updateCount
            );
        }
    }

    /**
     * retrieve the same type of object from the resultSet as the CrateClient would return
     */
    private Object getObject(ResultSet resultSet, int i, String typeName) throws SQLException {
        Object value;
        switch (typeName) {
            // need to use explicit `get<Type>` for some because getObject would return a wrong type.
            // E.g. int2 would return Integer instead of short.
            case "int2":
                Integer intValue = (Integer) resultSet.getObject(i + 1);
                if (intValue == null) {
                    return null;
                }
                value = intValue.shortValue();
                break;
            case "byte":
                value = resultSet.getByte(i + 1);
                break;
            case "_json":
                List<Object> jsonObjects = new ArrayList<>();
                for (Object json : (Object[]) resultSet.getArray(i + 1).getArray()) {
                    jsonObjects.add(jsonToObject(((PGobject) json).getValue()));
                }
                value = jsonObjects;
                break;
            case "json":
                String json = resultSet.getString(i + 1);
                value = jsonToObject(json);
                break;
            case "point":
                PGpoint pGpoint = resultSet.getObject(i + 1, PGpoint.class);
                value = new PointImpl(pGpoint.x, pGpoint.y, JtsSpatialContext.GEO);
                break;
            default:
                value = resultSet.getObject(i + 1);
                break;
        }
        if (value instanceof Timestamp) {
            value = ((Timestamp) value).getTime();
        } else if (value instanceof Array) {
            value = Arrays.asList(((Object[]) ((Array) value).getArray()));
        }
        return value;
    }

    private Object jsonToObject(String json) {
        try {
            if (json != null) {
                byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
                XContentParser parser = JsonXContent.jsonXContent.createParser(
                    NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, bytes);
                if (bytes.length >= 1 && bytes[0] == '[') {
                    parser.nextToken();
                    return parser.list();
                } else {
                    return parser.mapOrdered();
                }
            } else {
                return null;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @return an array with the rowCounts
     */
    private long[] executeBulk(String stmt, Object[][] bulkArgs, TimeValue timeout) {
        try {
            AdapterActionFuture<long[], long[]> actionFuture = new PlainActionFuture<>();
            execute(stmt, bulkArgs, actionFuture);
            return actionFuture.actionGet(timeout);
        } catch (ElasticsearchTimeoutException e) {
            LOGGER.error("Timeout on SQL statement: {}", e, stmt);
            throw e;
        }
    }

    public ClusterHealthStatus ensureGreen() {
        return ensureState(ClusterHealthStatus.GREEN);
    }

    public ClusterHealthStatus ensureYellowOrGreen() {
        return ensureState(ClusterHealthStatus.YELLOW);
    }

    private ClusterHealthStatus ensureState(ClusterHealthStatus state) {
        Client client = clientProvider.client();
        ClusterHealthResponse actionGet = client.admin().cluster().health(
            Requests.clusterHealthRequest()
                .waitForStatus(state)
                .waitForEvents(Priority.LANGUID).waitForNoRelocatingShards(false)
        ).actionGet();

        if (actionGet.isTimedOut()) {
            LOGGER.info("ensure state timed out, cluster state:\n{}\n{}",
                client.admin().cluster().prepareState().get().getState(),
                client.admin().cluster().preparePendingClusterTasks().get());
            assertThat("timed out waiting for state", actionGet.isTimedOut(), equalTo(false));
        }
        if (state == ClusterHealthStatus.YELLOW) {
            assertThat(actionGet.getStatus(), Matchers.anyOf(equalTo(state), equalTo(ClusterHealthStatus.GREEN)));
        } else {
            assertThat(actionGet.getStatus(), equalTo(state));
        }
        return actionGet.getStatus();
    }

    public interface ClientProvider {
        Client client();

        @Nullable
        String pgUrl();

        SQLOperations sqlOperations();
    }


    private static final DataType[] EMPTY_TYPES = new DataType[0];
    private static final String[] EMPTY_NAMES = new String[0];
    private static final Object[][] EMPTY_ROWS = new Object[0][];

    /**
     * Wrapper for testing issues. Creates a {@link SQLResponse} from
     * query results.
     */
    private static class ResultSetReceiver extends BaseResultReceiver {

        private final List<Object[]> rows = new ArrayList<>();
        private final ActionListener<SQLResponse> listener;
        private final List<Field> outputFields;

        ResultSetReceiver(ActionListener<SQLResponse> listener, List<Field> outputFields) {
            this.listener = listener;
            this.outputFields = outputFields;
        }

        @Override
        public void setNextRow(Row row) {
            rows.add(row.materialize());
        }

        @Override
        public void allFinished(boolean interrupted) {
            try {
                SQLResponse response = createSqlResponse();
                listener.onResponse(response);
            } catch (Exception e) {
                listener.onFailure(e);
            }
            super.allFinished(interrupted);
        }

        @Override
        public void fail(@Nonnull Throwable t) {
            listener.onFailure(SQLExceptions.createSQLActionException(t, e -> {}));
            super.fail(t);
        }

        private SQLResponse createSqlResponse() {
            String[] outputNames = new String[outputFields.size()];
            DataType[] outputTypes = new DataType[outputFields.size()];

            for (int i = 0, outputFieldsSize = outputFields.size(); i < outputFieldsSize; i++) {
                Field field = outputFields.get(i);
                outputNames[i] = field.path().sqlFqn();
                outputTypes[i] = field.valueType();
            }

            Object[][] rowsArr = rows.toArray(new Object[0][]);
            return new SQLResponse(
                outputNames,
                rowsArr,
                outputTypes,
                rowsArr.length
            );
        }
    }

    /**
     * Wrapper for testing issues. Creates a {@link SQLResponse} with
     * rowCount and duration of query execution.
     */
    private static class RowCountReceiver extends BaseResultReceiver {

        private final ActionListener<SQLResponse> listener;

        private long rowCount;

        RowCountReceiver(ActionListener<SQLResponse> listener) {
            this.listener = listener;
        }

        @Override
        public void setNextRow(Row row) {
            rowCount = (long) row.get(0);
        }

        @Override
        public void allFinished(boolean interrupted) {
            SQLResponse sqlResponse = new SQLResponse(
                EMPTY_NAMES,
                EMPTY_ROWS,
                EMPTY_TYPES,
                rowCount
            );
            listener.onResponse(sqlResponse);
            super.allFinished(interrupted);

        }

        @Override
        public void fail(@Nonnull Throwable t) {
            listener.onFailure(SQLExceptions.createSQLActionException(t, e -> {}));
            super.fail(t);
        }
    }


    /**
     * Wraps results of bulk requests for testing.
     */
    private static class BulkRowCountReceiver extends BaseResultReceiver {

        private final int resultIdx;
        private final long[] rowCounts;
        private long rowCount;

        BulkRowCountReceiver(long[] rowCounts, int resultIdx) {
            this.rowCounts = rowCounts;
            this.resultIdx = resultIdx;
        }

        @Override
        public void setNextRow(Row row) {
            rowCount = (long) row.get(0);
        }

        @Override
        public void allFinished(boolean interrupted) {
            rowCounts[resultIdx] = rowCount;
            super.allFinished(interrupted);
        }

        @Override
        public void fail(@Nonnull Throwable t) {
            rowCounts[resultIdx] = Row1.ERROR;
            super.fail(t);
        }
    }

}
