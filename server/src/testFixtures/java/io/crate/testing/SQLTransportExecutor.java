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

package io.crate.testing;

import static io.crate.action.sql.Session.UNNAMED;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

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
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.carrotsearch.randomizedtesting.RandomizedContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.support.AdapterActionFuture;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.hamcrest.Matchers;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.shape.impl.PointImpl;
import org.postgresql.geometric.PGpoint;
import org.postgresql.util.PGobject;

import io.crate.action.FutureActionListener;
import io.crate.action.sql.BaseResultReceiver;
import io.crate.action.sql.ResultReceiver;
import io.crate.action.sql.SQLOperations;
import io.crate.action.sql.Session;
import io.crate.auth.AccessControl;
import io.crate.common.unit.TimeValue;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.exceptions.Exceptions;
import io.crate.exceptions.SQLExceptions;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.SearchPath;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.protocols.postgres.types.PGArray;
import io.crate.protocols.postgres.types.PGType;
import io.crate.protocols.postgres.types.PGTypes;
import io.crate.protocols.postgres.types.PgOidVectorType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.user.User;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class SQLTransportExecutor {

    private static final String SQL_REQUEST_TIMEOUT = "CRATE_TESTS_SQL_REQUEST_TIMEOUT";

    public static final TimeValue REQUEST_TIMEOUT = new TimeValue(Long.parseLong(
        Objects.requireNonNullElse(System.getenv(SQL_REQUEST_TIMEOUT), "10")), TimeUnit.SECONDS);

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
            try (Session session = newSession()) {
                sessionList.forEach((setting) -> exec(setting, session));
                return FutureUtils.get(execute(stmt, args, session), timeout.millis(), TimeUnit.MILLISECONDS);
            }
        } catch (ElasticsearchTimeoutException ex) {
            Throwable cause = ex.getCause();
            if (cause == null) {
                cause = ex;
            }
            throw new ElasticsearchTimeoutException("Timeout while running `" + stmt + "`", cause);
        } catch (RuntimeException e) {
            var cause = e.getCause();
            // ActionListener.onFailure takes `Exception` as argument instead of `Throwable`.
            // That requires us to wrap Throwable; That Throwable may be an AssertionError.
            //
            // Wrapping the exception can hide parts of the stacktrace that are interesting
            // to figure out the root cause of an error, so we prefer the cause here
            if (e.getClass() == RuntimeException.class && cause != null) {
                Exceptions.rethrowUnchecked(cause);
            }
            throw e;
        }
    }

    public String jdbcUrl() {
        return clientProvider.pgUrl();
    }

    public CompletableFuture<SQLResponse> execute(String stmt, @Nullable Object[] args) {
        Session session = newSession();
        CompletableFuture<SQLResponse> result = execute(stmt, args, session);
        return result.whenComplete((res, err) -> session.close());
    }

    public Session newSession() {
        return clientProvider.sqlOperations().createSession(
            searchPath.currentSchema(),
            User.CRATE_USER
        );
    }

    public SQLResponse executeAs(String stmt, User user) {
        Session session = clientProvider.sqlOperations().createSession(null, user);
        return FutureUtils.get(execute(stmt, null, session), SQLTransportExecutor.REQUEST_TIMEOUT.millis(), TimeUnit.MILLISECONDS);
    }

    public SQLResponse exec(String statement, @Nullable Object[] args, Session session, TimeValue timeout) {
        return FutureUtils.get(execute(statement, args, session), timeout.millis(), TimeUnit.MILLISECONDS);
    }

    public SQLResponse exec(String statement, @Nullable Object[] args, Session session) {
        return exec(statement, args, session, REQUEST_TIMEOUT);
    }

    public SQLResponse exec(String statement, Session session) {
        return exec(statement, null, session);
    }

    public static CompletableFuture<SQLResponse> execute(String stmt,
                                                         @Nullable Object[] args,
                                                         Session session) {
        FutureActionListener<SQLResponse, SQLResponse> future = FutureActionListener.newInstance();
        execute(stmt, args, future, session);
        return future.exceptionally(err -> {
            throw SQLExceptions.prepareForClientTransmission(AccessControl.DISABLED, err);
        });
    }

    private static void execute(String stmt,
                                @Nullable Object[] args,
                                ActionListener<SQLResponse> listener,
                                Session session) {
        try {
            session.parse(UNNAMED, stmt, Collections.emptyList());
            List<Object> argsList = args == null ? Collections.emptyList() : Arrays.asList(args);
            session.bind(UNNAMED, UNNAMED, argsList, null);
            List<Symbol> outputFields = session.describe('P', UNNAMED).getFields();
            if (outputFields == null) {
                ResultReceiver<?> resultReceiver = new RowCountReceiver(listener);
                session.execute(UNNAMED, 0, resultReceiver);
            } else {
                ResultReceiver<?> resultReceiver = new ResultSetReceiver(listener, outputFields);
                session.execute(UNNAMED, 0, resultReceiver);
            }
            session.sync();
        } catch (Throwable t) {
            listener.onFailure(Exceptions.toException(t));
        }
    }

    private void execute(String stmt, @Nullable Object[][] bulkArgs, final ActionListener<long[]> listener) {
        if (bulkArgs != null && bulkArgs.length == 0) {
            listener.onResponse(new long[0]);
            return;
        }
        Session session = newSession();
        try {
            session.parse(UNNAMED, stmt, Collections.emptyList());
            final long[] rowCounts = bulkArgs == null ? new long[0] : new long[bulkArgs.length];
            if (bulkArgs == null) {
                session.bind(UNNAMED, UNNAMED, Collections.emptyList(), null);
                session.execute(UNNAMED, 0, new BaseResultReceiver());
            } else {
                for (int i = 0; i < bulkArgs.length; i++) {
                    session.bind(UNNAMED, UNNAMED, Arrays.asList(bulkArgs[i]), null);
                    ResultReceiver<?> resultReceiver = new BulkRowCountReceiver(rowCounts, i);
                    session.execute(UNNAMED, 0, resultReceiver);
                }
            }
            List<Symbol> outputColumns = session.describe('P', UNNAMED).getFields();
            if (outputColumns != null) {
                throw new UnsupportedOperationException(
                    "Bulk operations for statements that return result sets is not supported");
            }
            session.sync().whenComplete((Object result, Throwable t) -> {
                if (t == null) {
                    listener.onResponse(rowCounts);
                } else {
                    listener.onFailure(Exceptions.toException(t));
                }
                session.close();
            });
        } catch (Throwable t) {
            session.close();
            listener.onFailure(Exceptions.toException(t));
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
                properties.setProperty("prepareThreshold", "-1"); // always use prepared statements
            }
            properties.put("user", User.CRATE_USER.name());
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
        } catch (SQLException e) {
            LOGGER.error("Error executing stmt={} args={} error={}", stmt, Arrays.toString(args), e);
            Exceptions.rethrowUnchecked(e);
            // this should never happen
            return null;
        }
    }

    public static Object toJdbcCompatObject(Connection connection, Object arg) {
        if (arg == null) {
            return arg;
        }
        if (arg instanceof Map) {
            return DataTypes.STRING.implicitCast(arg);
        }
        if (arg.getClass().isArray()) {
            arg = Arrays.asList((Object[]) arg);
        }
        if (arg instanceof Collection) {
            Collection<?> values = (Collection<?>) arg;
            if (values.isEmpty()) {
                return null; // Can't insert empty list without knowing the type
            }
            List<Object> convertedValues = new ArrayList<>(values.size());
            PGType<?> pgType = null;
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

    private static SQLResponse executeAndConvertResult(PreparedStatement preparedStatement) throws SQLException {
        if (preparedStatement.execute()) {
            ResultSetMetaData metadata = preparedStatement.getMetaData();
            ResultSet resultSet = preparedStatement.getResultSet();
            List<Object[]> rows = new ArrayList<>();
            List<String> columnNames = new ArrayList<>(metadata.getColumnCount());
            DataType[] dataTypes = new DataType[metadata.getColumnCount()];
            for (int i = 0; i < metadata.getColumnCount(); i++) {
                columnNames.add(metadata.getColumnName(i + 1));
            }
            while (resultSet.next()) {
                Object[] row = new Object[metadata.getColumnCount()];
                for (int i = 0; i < row.length; i++) {
                    Object value;
                    String typeName = metadata.getColumnTypeName(i + 1);
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
    private static Object getObject(ResultSet resultSet, int i, String typeName) throws SQLException {
        Object value;
        int columnIndex = i + 1;
        switch (typeName) {
            // need to use explicit `get<Type>` for some because getObject would return a wrong type.
            // E.g. int2 would return Integer instead of short.
            case "int2":
                Integer intValue = (Integer) resultSet.getObject(columnIndex);
                if (intValue == null) {
                    return null;
                }
                value = intValue.shortValue();
                break;
            case "_char": {
                Array array = resultSet.getArray(columnIndex);
                if (array == null) {
                    return null;
                }
                ArrayList<Byte> elements = new ArrayList<>();
                for (Object o : ((Object[]) array.getArray())) {
                    elements.add(Byte.parseByte((String) o));
                }
                return elements;
            }
            case "oidvector": {
                String textval = resultSet.getString(columnIndex);
                if (textval == null) {
                    return null;
                }
                return PgOidVectorType.listFromOidVectorString(textval);
            }
            case "char":
                String strValue = resultSet.getString(columnIndex);
                if (strValue == null) {
                    return null;
                }
                return Byte.valueOf(strValue);
            case "byte":
                value = resultSet.getByte(columnIndex);
                break;
            case "_json": {
                Array array = resultSet.getArray(columnIndex);
                if (array == null) {
                    return null;
                }
                ArrayList<Object> jsonObjects = new ArrayList<>();
                for (Object item : (Object[]) array.getArray()) {
                    jsonObjects.add(jsonToObject((String) item));
                }
                value = jsonObjects;
                break;
            }
            case "json":
                String json = resultSet.getString(columnIndex);
                value = jsonToObject(json);
                break;
            case "point":
                PGpoint pGpoint = resultSet.getObject(columnIndex, PGpoint.class);
                value = new PointImpl(pGpoint.x, pGpoint.y, JtsSpatialContext.GEO);
                break;
            case "record":
                value = resultSet.getObject(columnIndex, PGobject.class).getValue();
                break;
            case "_bit":
                String pgBitStringArray = resultSet.getString(columnIndex);
                if (pgBitStringArray == null) {
                    return null;
                }
                byte[] bytes = pgBitStringArray.getBytes(StandardCharsets.UTF_8);
                ByteBuf buf = Unpooled.wrappedBuffer(bytes);
                value = PGArray.BIT_ARRAY.readTextValue(buf, bytes.length);
                buf.release();
                break;

            default:
                value = resultSet.getObject(columnIndex);
                break;
        }
        if (value instanceof Timestamp) {
            value = ((Timestamp) value).getTime();
        } else if (value instanceof Array) {
            value = Arrays.asList(((Object[]) ((Array) value).getArray()));
        }
        return value;
    }

    private static Object jsonToObject(String json) {
        try {
            if (json != null) {
                byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
                XContentParser parser = JsonXContent.JSON_XCONTENT.createParser(
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
            var listener = ActionListener.delegateResponse(actionFuture, (delegate, failure) -> {
                delegate.onFailure(SQLExceptions.prepareForClientTransmission(AccessControl.DISABLED, failure));
            });
            execute(stmt, bulkArgs, listener);
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
            new ClusterHealthRequest()
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
        private final List<Symbol> outputFields;

        ResultSetReceiver(ActionListener<SQLResponse> listener, List<Symbol> outputFields) {
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
            listener.onFailure(Exceptions.toException(t));
            super.fail(t);
        }

        private SQLResponse createSqlResponse() {
            String[] outputNames = new String[outputFields.size()];
            DataType[] outputTypes = new DataType[outputFields.size()];

            for (int i = 0, outputFieldsSize = outputFields.size(); i < outputFieldsSize; i++) {
                Symbol field = outputFields.get(i);
                outputNames[i] = Symbols.pathFromSymbol(field).sqlFqn();
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
            listener.onFailure(Exceptions.toException(t));
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
