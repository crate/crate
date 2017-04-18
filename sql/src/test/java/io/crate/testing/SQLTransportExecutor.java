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
import com.google.common.base.Throwables;
import io.crate.action.sql.*;
import io.crate.analyze.symbol.Field;
import io.crate.data.Row;
import io.crate.exceptions.SQLExceptions;
import io.crate.protocols.postgres.types.PGType;
import io.crate.protocols.postgres.types.PGTypes;
import io.crate.shade.org.postgresql.util.PGobject;
import io.crate.shade.org.postgresql.util.PSQLException;
import io.crate.shade.org.postgresql.util.ServerErrorMessage;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.support.AdapterActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.RestStatus;
import org.hamcrest.Matchers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static io.crate.action.sql.SQLOperations.Session.UNNAMED;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class SQLTransportExecutor {

    private static final String SQL_REQUEST_TIMEOUT = "CRATE_TESTS_SQL_REQUEST_TIMEOUT";

    public final static int DEFAULT_SOFT_LIMIT = 10_000;

    public static final TimeValue REQUEST_TIMEOUT = new TimeValue(Long.parseLong(
        MoreObjects.firstNonNull(System.getenv(SQL_REQUEST_TIMEOUT), "5")), TimeUnit.SECONDS);

    private static final Logger LOGGER = Loggers.getLogger(SQLTransportExecutor.class);
    private final ClientProvider clientProvider;

    public SQLTransportExecutor(ClientProvider clientProvider) {
        this.clientProvider = clientProvider;
    }

    public SQLResponse exec(String statement) {
        return exec(statement, null, REQUEST_TIMEOUT);
    }

    public SQLResponse exec(String statement, Object... params) {
        return exec(statement, params, REQUEST_TIMEOUT);
    }

    public SQLBulkResponse execBulk(String statement, @Nullable Object[][] bulkArgs) {
        return executeBulk(statement, bulkArgs, REQUEST_TIMEOUT);
    }

    public SQLBulkResponse execBulk(String statement, @Nullable Object[][] bulkArgs, TimeValue timeout) {
        return executeBulk(statement, bulkArgs, timeout);
    }

    private SQLResponse exec(String stmt, @Nullable Object[] args, TimeValue timeout) {
        String pgUrl = clientProvider.pgUrl();
        Random random = RandomizedContext.current().getRandom();
        if (pgUrl != null && isJdbcEnabled()) {
            LOGGER.trace("Executing with pgJDBC: {}", stmt);
            return executeWithPg(stmt, args, pgUrl, random);
        }
        try {
            return execute(stmt, args).actionGet(timeout);
        } catch (ElasticsearchTimeoutException e) {
            LOGGER.error("Timeout on SQL statement: {}", e, stmt);
            throw e;
        }
    }

    /**
     * @return true if a class or method in the stacktrace contains a @UseJdbc annotation
     * and based on the ration provided
     * <p>
     * Method annotations have higher priority than class annotations.
     */
    private boolean isJdbcEnabled() {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        for (StackTraceElement element : stackTrace) {
            try {
                Class<?> ar = Class.forName(element.getClassName());
                Method method = ar.getMethod(element.getMethodName());
                UseJdbc annotation = method.getAnnotation(UseJdbc.class);
                if (annotation == null) {
                    annotation = ar.getAnnotation(UseJdbc.class);
                    if (annotation == null) {
                        continue;
                    }
                }
                double ratio = annotation.value();
                assert ratio >= 0.0 && ratio <= 1.0;
                if (ratio == 0) {
                    return false;
                }
                if (ratio == 1) {
                    return true;
                }
                return RandomizedContext.current().getRandom().nextDouble() < ratio;
            } catch (NoSuchMethodException | ClassNotFoundException ignored) {
            }
        }
        return false;
    }


    public ActionFuture<SQLResponse> execute(String stmt, @Nullable Object[] args) {
        return execute(stmt, args, clientProvider.sqlOperations().createSession(
            null,
            Option.NONE,
            DEFAULT_SOFT_LIMIT
        ));
    }

    public static ActionFuture<SQLResponse> execute(String stmt, @Nullable Object[] args, SQLOperations.Session session) {
        final AdapterActionFuture<SQLResponse, SQLResponse> actionFuture = new TestTransportActionFuture<>();
        execute(stmt, args, actionFuture, session);
        return actionFuture;
    }

    private static void execute(String stmt, @Nullable Object[] args, ActionListener<SQLResponse> listener,
                                SQLOperations.Session session) {
        try {
            session.parse(UNNAMED, stmt, Collections.<DataType>emptyList());
            List<Object> argsList = args == null ? Collections.emptyList() : Arrays.asList(args);
            session.bind(UNNAMED, UNNAMED, argsList, null);
            List<Field> outputFields = session.describe('P', UNNAMED);
            if (outputFields == null) {
                ResultReceiver resultReceiver = new RowCountReceiver(listener);
                session.execute(UNNAMED, 0, resultReceiver);
            } else {
                ResultReceiver resultReceiver = new ResultSetReceiver(listener, outputFields);
                session.execute(UNNAMED, 0, resultReceiver);
            }
            session.sync();
        } catch (Throwable t) {
            listener.onFailure(SQLExceptions.createSQLActionException(t));
        }
    }

    private void execute(String stmt, @Nullable Object[][] bulkArgs, final ActionListener<SQLBulkResponse> listener) {
        SQLOperations.Session session = clientProvider.sqlOperations().createSession(
            null,
            Option.NONE,
            DEFAULT_SOFT_LIMIT
        );
        try {
            session.parse(UNNAMED, stmt, Collections.<DataType>emptyList());
            if (bulkArgs == null) {
                bulkArgs = new Object[0][];
            }
            final SQLBulkResponse.Result[] results = new SQLBulkResponse.Result[bulkArgs.length];
            if (results.length == 0) {
                session.bind(UNNAMED, UNNAMED, Collections.emptyList(), null);
                session.execute(UNNAMED, 0, new BaseResultReceiver());
            } else {
                for (int i = 0; i < bulkArgs.length; i++) {
                    session.bind(UNNAMED, UNNAMED, Arrays.asList(bulkArgs[i]), null);
                    ResultReceiver resultReceiver = new BulkRowCountReceiver(results, i);
                    session.execute(UNNAMED, 0, resultReceiver);
                }
            }
            List<Field> outputColumns = session.describe('P', UNNAMED);
            if (outputColumns != null) {
                throw new UnsupportedOperationException(
                    "Bulk operations for statements that return result sets is not supported");
            }
            session.sync().whenComplete((Object result, Throwable t) -> {
                if (t == null) {
                    listener.onResponse(new SQLBulkResponse(results));
                } else {
                    listener.onFailure(SQLExceptions.createSQLActionException(t));
                }
            });
        } catch (Throwable t) {
            listener.onFailure(SQLExceptions.createSQLActionException(t));
        }
    }

    private SQLResponse executeWithPg(String stmt, @Nullable Object[] args, String pgUrl, Random random) {
        try {
            Properties properties = new Properties();
            if (random.nextBoolean()) {
                properties.setProperty("prepareThreshold", "-1"); // disable prepared statements
            }
            try (Connection conn = DriverManager.getConnection(pgUrl, properties)) {
                conn.setAutoCommit(true);
                PreparedStatement preparedStatement = conn.prepareStatement(stmt);
                if (args != null) {
                    for (int i = 0; i < args.length; i++) {
                        preparedStatement.setObject(i + 1, toJdbcCompatObject(conn, args[i]));
                    }
                }
                return executeAndConvertResult(preparedStatement);
            }
        } catch (PSQLException e) {
            ServerErrorMessage serverErrorMessage = e.getServerErrorMessage();
            StackTraceElement[] stacktrace;
            if (serverErrorMessage != null) {
                StackTraceElement stackTraceElement = new StackTraceElement(
                    serverErrorMessage.getFile(),
                    serverErrorMessage.getRoutine(),
                    serverErrorMessage.getFile(),
                    serverErrorMessage.getLine());
                stacktrace = new StackTraceElement[]{stackTraceElement};
            } else {
                stacktrace = new StackTraceElement[]{};
            }
            throw new SQLActionException(
                e.getMessage(),
                0,
                RestStatus.BAD_REQUEST,
                stacktrace);
        } catch (SQLException e) {
            throw new SQLActionException(e.getMessage(), 0, RestStatus.BAD_REQUEST);
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
                throw Throwables.propagate(e);
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
                    throw Throwables.propagate(e);
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
                throw Throwables.propagate(e);
            }
        }
        return arg;
    }

    private static String toJsonString(Map value) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.map(value);
        builder.close();
        return builder.bytes().utf8ToString();
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
        return builder.bytes().utf8ToString();
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
                value = jsonObjects.toArray();
                break;
            case "json":
                String json = resultSet.getString(i + 1);
                value = jsonToObject(json);
                break;
            default:
                value = resultSet.getObject(i + 1);
                break;
        }
        if (value instanceof Timestamp) {
            value = ((Timestamp) value).getTime();
        } else if (value instanceof Array) {
            value = ((Array) value).getArray();
        }
        return value;
    }

    private Object jsonToObject(String json) {
        try {
            if (json != null) {
                byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
                // It is safe to use NamedXContentRegistry.EMPTY here because this never uses namedObject
                XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, bytes);
                if (bytes.length >= 1 && bytes[0] == '[') {
                    parser.nextToken();
                    return recursiveListToArray(parser.list());
                } else {
                    return parser.mapOrdered();
                }
            } else {
                return null;
            }
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private Object recursiveListToArray(Object value) {
        if (value instanceof List) {
            List list = (List) value;
            Object[] arr = list.toArray(new Object[0]);
            for (int i = 0; i < list.size(); i++) {
                arr[i] = recursiveListToArray(list.get(i));
            }
            return arr;
        }
        return value;
    }

    private SQLBulkResponse executeBulk(String stmt, Object[][] bulkArgs, TimeValue timeout) {
        try {
            AdapterActionFuture<SQLBulkResponse, SQLBulkResponse> actionFuture = new TestTransportActionFuture<>();
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
            LOGGER.info("ensure state timed out, cluster state:\n{}\n{}", client.admin().cluster().prepareState().get().getState().toString(), client.admin().cluster().preparePendingClusterTasks().get().toString());
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

    private static class TestTransportActionFuture<R> extends AdapterActionFuture<R, R> {

        @Override
        protected R convert(R response) {
            return response;
        }

        @Override
        public void onFailure(Exception e) {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause instanceof NotSerializableExceptionWrapper) {
                NotSerializableExceptionWrapper wrapper = ((NotSerializableExceptionWrapper) cause);
                SQLActionException sae = SQLActionException.fromSerializationWrapper(wrapper);
                if (sae != null) {
                    e = sae;
                }
            }
            super.onFailure(e);
        }
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

        ResultSetReceiver(ActionListener<SQLResponse> listener,
                          List<Field> outputFields) {
            this.listener = listener;
            this.outputFields = outputFields;
        }

        @Override
        public void setNextRow(Row row) {
            rows.add(row.materialize());
        }

        @Override
        public void allFinished(boolean interrupted) {
            listener.onResponse(createSqlResponse());
            super.allFinished(interrupted);
        }

        @Override
        public void fail(@Nonnull Throwable t) {
            listener.onFailure(SQLExceptions.createSQLActionException(t));
            super.fail(t);
        }

        private SQLResponse createSqlResponse() {
            String[] outputNames = new String[outputFields.size()];
            DataType[] outputTypes = new DataType[outputFields.size()];

            for (int i = 0, outputFieldsSize = outputFields.size(); i < outputFieldsSize; i++) {
                Field field = outputFields.get(i);
                outputNames[i] = field.path().outputName();
                outputTypes[i] = field.valueType();
            }

            Object[][] rowsArr = rows.toArray(new Object[0][]);
            BytesRefUtils.ensureStringTypesAreStrings(outputTypes, rowsArr);
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
            listener.onFailure(SQLExceptions.createSQLActionException(t));
            super.fail(t);
        }
    }


    /**
     * Wraps results of bulk requests for testing.
     */
    private static class BulkRowCountReceiver extends BaseResultReceiver {

        private final SQLBulkResponse.Result[] results;
        private final int resultIdx;
        private long rowCount;

        BulkRowCountReceiver(SQLBulkResponse.Result[] results, int resultIdx) {
            this.results = results;
            this.resultIdx = resultIdx;
        }

        @Override
        public void setNextRow(Row row) {
            rowCount = ((long) row.get(0));
        }

        @Override
        public void allFinished(boolean interrupted) {
            results[resultIdx] = new SQLBulkResponse.Result(null, rowCount);
            super.allFinished(interrupted);
        }

        @Override
        public void fail(@Nonnull Throwable t) {
            results[resultIdx] = new SQLBulkResponse.Result(SQLExceptions.messageOf(t), rowCount);
            super.fail(t);
        }
    }

}
