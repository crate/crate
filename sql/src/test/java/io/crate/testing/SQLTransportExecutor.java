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
import io.crate.protocols.postgres.types.PGType;
import io.crate.protocols.postgres.types.PGTypes;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.support.AdapterActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.RestStatus;
import org.hamcrest.Matchers;
import org.postgresql.util.PGobject;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class SQLTransportExecutor {

    private static final String SQL_REQUEST_TIMEOUT = "CRATE_TESTS_SQL_REQUEST_TIMEOUT";

    public static final TimeValue REQUEST_TIMEOUT = new TimeValue(Long.parseLong(
        MoreObjects.firstNonNull(System.getenv(SQL_REQUEST_TIMEOUT), "5")), TimeUnit.SECONDS);

    private static final ESLogger LOGGER = Loggers.getLogger(SQLTransportExecutor.class);
    private final ClientProvider clientProvider;

    public SQLTransportExecutor(ClientProvider clientProvider) {
        this.clientProvider = clientProvider;
    }

    public SQLResponse exec(String statement) {
        return exec(new SQLRequest(statement));
    }

    public SQLResponse exec(String statement, Object... params) {
        return exec(new SQLRequest(statement, params));
    }

    public SQLResponse exec(String statement, Object[] params, TimeValue timeout) {
        return exec(new SQLRequest(statement, params), timeout);
    }

    public SQLBulkResponse execBulk(String statement, Object[][] bulkArgs) {
        return exec(new SQLBulkRequest(statement, bulkArgs), REQUEST_TIMEOUT);
    }

    public SQLBulkResponse execBulk(String statement, Object[][] bulkArgs, TimeValue timeout) {
        return exec(new SQLBulkRequest(statement, bulkArgs), timeout);
    }

    public SQLResponse exec(SQLRequest request) {
        return exec(request, REQUEST_TIMEOUT);
    }

    public SQLResponse exec(SQLRequest request, TimeValue timeout) {
        String pgUrl = clientProvider.pgUrl();
        Random random = RandomizedContext.current().getRandom();
        if (pgUrl != null && random.nextBoolean() && isJdbcCompatible()) {
            return executeWithPg(request, pgUrl, random);
        }
        try {
            return execute(request).actionGet(timeout);
        } catch (ElasticsearchTimeoutException e) {
            LOGGER.error("Timeout on SQL statement: {}", e, request.stmt());
            throw e;
        }
    }

    /**
     * @return true if a class or method in the stacktrace contains a @UseJdbc(true) annotation.
     * <p>
     * Method annotations have higher priority than class annotations.
     */
    private boolean isJdbcCompatible() {
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
                return annotation.value();
            } catch (NoSuchMethodException | ClassNotFoundException ignored) {
            }
        }
        return false;
    }

    private SQLResponse executeWithPg(SQLRequest request, String pgUrl, Random random) {
        try {
            Properties properties = new Properties();
            if (random.nextBoolean()) {
                properties.setProperty("prepareThreshold", "-1"); // disable prepared statements
            }
            try (Connection conn = DriverManager.getConnection(pgUrl, properties)) {
                conn.setAutoCommit(true);
                PreparedStatement preparedStatement = conn.prepareStatement(request.stmt());
                Object[] args = request.args();
                for (int i = 0; i < args.length; i++) {
                    preparedStatement.setObject(i + 1, toJdbcCompatObject(conn, args[i]));
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
        return builder.bytes().toUtf8();
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
        return builder.bytes().toUtf8();
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
                rows.size(),
                1,
                true
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
                updateCount,
                1,
                true
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
                value = resultSet.getShort(i + 1);
                break;
            case "_char":
                value = getCharArray(resultSet, i);
                break;
            case "char":
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
                XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
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

    private static Object getCharArray(ResultSet resultSet, int i) throws SQLException {
        Object value;
        Array array = resultSet.getArray(i + 1);
        if (array == null) {
            value = null;
        } else {
            ResultSet arrRS = array.getResultSet();
            List<Object> values = new ArrayList<>();
            while (arrRS.next()) {
                values.add(arrRS.getByte(2));
            }
            value = values.toArray(new Object[0]);
        }
        return value;
    }

    public SQLBulkResponse exec(SQLBulkRequest request) {
        return exec(request, REQUEST_TIMEOUT);
    }

    public SQLBulkResponse exec(SQLBulkRequest request, TimeValue timeout) {
        try {
            return execute(request).actionGet(timeout);
        } catch (ElasticsearchTimeoutException e) {
            LOGGER.error("Timeout on SQL statement: {}", e, request.stmt());
            throw e;
        }
    }

    public ActionFuture<SQLResponse> execute(SQLRequest request) {
        AdapterActionFuture<SQLResponse, SQLResponse> actionFuture = new TestTransportActionFuture<>();
        clientProvider.client().execute(SQLAction.INSTANCE, request, actionFuture);
        return actionFuture;
    }

    private ActionFuture<SQLBulkResponse> execute(SQLBulkRequest request) {
        AdapterActionFuture<SQLBulkResponse, SQLBulkResponse> actionFuture = new TestTransportActionFuture<>();
        clientProvider.client().execute(SQLBulkAction.INSTANCE, request, actionFuture);
        return actionFuture;
    }

    public ClusterHealthStatus ensureGreen() {
        return ensureState(ClusterHealthStatus.GREEN);
    }

    public ClusterHealthStatus ensureYellowOrGreen() {
        return ensureState(ClusterHealthStatus.YELLOW);
    }

    private ClusterHealthStatus ensureState(ClusterHealthStatus state) {
        ClusterHealthResponse actionGet = client().admin().cluster().health(
            Requests.clusterHealthRequest()
                .waitForStatus(state)
                .waitForEvents(Priority.LANGUID).waitForRelocatingShards(0)
        ).actionGet();

        if (actionGet.isTimedOut()) {
            LOGGER.info("ensure state timed out, cluster state:\n{}\n{}", client().admin().cluster().prepareState().get().getState().prettyPrint(), client().admin().cluster().preparePendingClusterTasks().get().prettyPrint());
            assertThat("timed out waiting for state", actionGet.isTimedOut(), equalTo(false));
        }
        if (state == ClusterHealthStatus.YELLOW) {
            assertThat(actionGet.getStatus(), Matchers.anyOf(equalTo(state), equalTo(ClusterHealthStatus.GREEN)));
        } else {
            assertThat(actionGet.getStatus(), equalTo(state));
        }
        return actionGet.getStatus();
    }

    public Client client() {
        return clientProvider.client();
    }

    public interface ClientProvider {
        Client client();

        @Nullable
        String pgUrl();
    }

    private class TestTransportActionFuture<Response extends SQLBaseResponse> extends AdapterActionFuture<Response, Response> {

        @Override
        protected Response convert(Response response) {
            return response;
        }

        @Override
        public void onFailure(Throwable e) {
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
}
